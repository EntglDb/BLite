using System.Security.Cryptography;
using BLite.Bson;
using BLite.Core;
using BLite.Core.Encryption;
using BLite.Core.Storage;

namespace BLite.Tests;

/// <summary>
/// Tests for backup encryption (<see cref="BackupOptions.BackupCryptoProvider"/>)
/// and encryption migration (<see cref="BLiteEngine.MigrateToEncryptedAsync"/>,
/// <see cref="BLiteEngine.MigrateToPlaintextAsync"/>).
/// </summary>
public sealed class BackupEncryptionTests : IDisposable
{
    private readonly List<string> _tempDirs = new();

    private string TempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"blite_bkenc_{Guid.NewGuid()}");
        Directory.CreateDirectory(dir);
        _tempDirs.Add(dir);
        return dir;
    }

    public void Dispose()
    {
        foreach (var d in _tempDirs)
        {
            try { if (Directory.Exists(d)) Directory.Delete(d, recursive: true); }
            catch { /* best-effort */ }
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static byte[] MakeMasterKey(byte seed = 0x55)
    {
        var key = new byte[32];
        for (var i = 0; i < key.Length; i++) key[i] = (byte)(seed + i);
        return key;
    }

    private static async Task InsertDocumentsAsync(BLiteEngine engine, string collection, int count)
    {
        var col = engine.GetOrCreateCollection(collection);
        var batch = new List<BsonDocument>(count);
        for (var i = 0; i < count; i++)
        {
            var doc = col.CreateDocument(["_id", "name", "index"],
                b => b.AddString("name", $"Item_{i}").AddInt32("index", i));
            batch.Add(doc);
        }
        await col.InsertBulkAsync(batch);
        await engine.CommitAsync();
    }

    private static async Task<int> CountDocumentsAsync(BLiteEngine engine, string collection)
    {
        var col = engine.GetOrCreateCollection(collection);
        return (int)await col.CountAsync();
    }

    // ── IKeyProvider test implementation ──────────────────────────────────────

    private sealed class StaticKeyProvider : IKeyProvider
    {
        private readonly byte[] _key;
        public StaticKeyProvider(byte[] key) => _key = (byte[])key.Clone();
        public ValueTask<byte[]> GetKeyAsync(string databaseName, CancellationToken ct = default)
            => new ValueTask<byte[]>((byte[])_key.Clone());
    }

    // ── BackupOptions.BackupCryptoProvider ────────────────────────────────────

    [Fact]
    public void BackupOptions_BackupCryptoProvider_DefaultIsNull()
    {
        var options = new BackupOptions { DestinationPath = "/tmp/test.db" };
        Assert.Null(options.BackupCryptoProvider);
    }

    [Fact]
    public void BackupOptions_BackupCryptoProvider_CanBeSet()
    {
        var provider = new AesGcmCryptoProvider(new CryptoOptions("backup-key", iterations: 1));
        var options = new BackupOptions
        {
            DestinationPath = "/tmp/test.db",
            BackupCryptoProvider = provider
        };
        Assert.Same(provider, options.BackupCryptoProvider);
        provider.Dispose();
    }

    // ── PageFile.TranscryptBackupAsync ────────────────────────────────────────

    [Fact]
    public async Task TranscryptBackup_PlaintextToEncrypted_BackupIsNotPlaintext()
    {
        var dir = TempDir();
        var srcPath = Path.Combine(dir, "source.db");
        var bakPath = Path.Combine(dir, "backup.db");

        // Create plaintext source
        using (var engine = new BLiteEngine(srcPath))
        {
            await InsertDocumentsAsync(engine, "items", 5);
        }

        // Backup with encryption
        var backupKey = new CryptoOptions("backup-pass", iterations: 1);
        var backupProvider = new AesGcmCryptoProvider(backupKey);
        using (var engine = new BLiteEngine(srcPath))
        {
            await engine.BackupAsync(new BackupOptions
            {
                DestinationPath = bakPath,
                BackupCryptoProvider = backupProvider
            });
        }

        // The backup file should have a BLCE header (first 4 bytes = magic 0x424C4345 in LE)
        var rawBytes = File.ReadAllBytes(bakPath);
        Assert.True(rawBytes.Length >= 64, "Backup file should have at least the 64-byte BLCE header.");
        var magic = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(rawBytes.AsSpan(0, 4));
        Assert.Equal(0x424C4345u, magic); // "BLCE" magic stored as little-endian uint

        backupProvider.Dispose();
    }

    [Fact]
    public async Task TranscryptBackup_PlaintextToEncrypted_CanBeReadBack()
    {
        var dir = TempDir();
        var srcPath = Path.Combine(dir, "source.db");
        var bakPath = Path.Combine(dir, "backup.db");
        const string secret = "MySecretDocument";

        // Create plaintext source with a known value
        using (var engine = new BLiteEngine(srcPath))
        {
            var col = engine.GetOrCreateCollection("items");
            var doc = col.CreateDocument(["_id", "name"], b => b.AddString("name", secret));
            await col.InsertAsync(doc);
            await engine.CommitAsync();
        }

        // Backup with encryption
        var backupOpts = new CryptoOptions("backup-pass", iterations: 1);
        var backupProvider = new AesGcmCryptoProvider(backupOpts);
        using (var engine = new BLiteEngine(srcPath))
        {
            await engine.BackupAsync(new BackupOptions
            {
                DestinationPath = bakPath,
                BackupCryptoProvider = backupProvider
            });
        }
        backupProvider.Dispose();

        // Verify: the raw file should NOT contain the plaintext secret
        var rawBytes = File.ReadAllBytes(bakPath);
        var secretBytes = System.Text.Encoding.UTF8.GetBytes(secret);
        Assert.True(rawBytes.AsSpan().IndexOf(secretBytes) == -1,
            "Plaintext value must not appear in the encrypted backup.");

        // Verify: the backup can be read back with the correct key
        using var readProvider = new AesGcmCryptoProvider(backupOpts);
        var bakConfig = PageFileConfig.Default with { CryptoProvider = readProvider };
        using var bakEngine = new BLiteEngine(bakPath, bakConfig);
        var count = await CountDocumentsAsync(bakEngine, "items");
        Assert.Equal(1, count);
        var col2 = bakEngine.GetOrCreateCollection("items");
        var found = await col2.FindAllAsync().ToListAsync();
        Assert.Single(found);
        Assert.True(found[0].TryGetString("name", out var name));
        Assert.Equal(secret, name);
    }

    [Fact]
    public async Task TranscryptBackup_EncryptedToReencrypted_CanBeReadBack()
    {
        var dir = TempDir();
        var srcPath = Path.Combine(dir, "source.db");
        var bakPath = Path.Combine(dir, "backup.db");
        const string secret = "ReencryptedValue";

        // Create encrypted source
        var srcCrypto = new CryptoOptions("source-pass", iterations: 1);
        using (var engine = new BLiteEngine(srcPath, srcCrypto))
        {
            var col = engine.GetOrCreateCollection("data");
            var doc = col.CreateDocument(["_id", "value"], b => b.AddString("value", secret));
            await col.InsertAsync(doc);
            await engine.CommitAsync();
        }

        // Re-encrypt backup with a DIFFERENT key
        var bakCrypto = new CryptoOptions("backup-pass", iterations: 1);
        using var bakProvider = new AesGcmCryptoProvider(bakCrypto);
        using (var engine = new BLiteEngine(srcPath, srcCrypto))
        {
            await engine.BackupAsync(new BackupOptions
            {
                DestinationPath = bakPath,
                BackupCryptoProvider = bakProvider
            });
        }

        // Backup file must be readable with the backup key
        var correctConfig = PageFileConfig.Default with
        {
            CryptoProvider = new AesGcmCryptoProvider(bakCrypto)
        };
        using var readEngine = new BLiteEngine(bakPath, correctConfig);
        var col2 = readEngine.GetOrCreateCollection("data");
        var found = await col2.FindAllAsync().ToListAsync();
        Assert.Single(found);
        Assert.True(found[0].TryGetString("value", out var val));
        Assert.Equal(secret, val);
    }

    [Fact]
    public async Task TranscryptBackup_WithNullCryptoProvider_ProducesPlaintextBackup()
    {
        var dir = TempDir();
        var srcPath = Path.Combine(dir, "source.db");
        var bakPath = Path.Combine(dir, "backup.db");
        const string secret = "PlaintextValue";

        // Create encrypted source
        var srcCrypto = new CryptoOptions("source-pass", iterations: 1);
        using (var engine = new BLiteEngine(srcPath, srcCrypto))
        {
            var col = engine.GetOrCreateCollection("data");
            var doc = col.CreateDocument(["_id", "value"], b => b.AddString("value", secret));
            await col.InsertAsync(doc);
            await engine.CommitAsync();
        }

        // Backup with NullCryptoProvider = plaintext backup
        using (var engine = new BLiteEngine(srcPath, srcCrypto))
        {
            await engine.BackupAsync(new BackupOptions
            {
                DestinationPath = bakPath,
                BackupCryptoProvider = NullCryptoProvider.Instance
            });
        }

        // Backup file should be readable without encryption
        using var readEngine = new BLiteEngine(bakPath);
        var col2 = readEngine.GetOrCreateCollection("data");
        var found = await col2.FindAllAsync().ToListAsync();
        Assert.Single(found);
        Assert.True(found[0].TryGetString("value", out var val));
        Assert.Equal(secret, val);
    }

    [Fact]
    public async Task TranscryptBackup_ManifestIsGenerated()
    {
        var dir = TempDir();
        var srcPath = Path.Combine(dir, "source.db");
        var bakDir = Path.Combine(dir, "backup");
        Directory.CreateDirectory(bakDir);
        var bakPath = Path.Combine(bakDir, "backup.db");

        using (var engine = new BLiteEngine(srcPath))
        {
            await InsertDocumentsAsync(engine, "items", 3);
        }

        using var bakProvider = new AesGcmCryptoProvider(new CryptoOptions("key", iterations: 1));
        using (var engine = new BLiteEngine(srcPath))
        {
            var result = await engine.BackupAsync(new BackupOptions
            {
                DestinationPath = bakPath,
                BackupCryptoProvider = bakProvider
            });
            Assert.NotNull(result.ManifestPath);
            Assert.True(File.Exists(result.ManifestPath));
        }

        var manifest = File.ReadAllText(Path.Combine(bakDir, "backup.manifest.json"));
        Assert.Contains("backup.db", manifest);
    }

    // ── IKeyProvider ──────────────────────────────────────────────────────────

    [Fact]
    public async Task IKeyProvider_GetKeyAsync_ReturnsKey()
    {
        var expectedKey = MakeMasterKey(0x10);
        var provider = new StaticKeyProvider(expectedKey);
        var key = await provider.GetKeyAsync("mydb");
        Assert.Equal(expectedKey, key);
    }

    // ── BLiteEngine.MigrateToEncryptedAsync ───────────────────────────────────

    [Fact]
    public async Task MigrateToEncrypted_ArgumentValidation()
    {
        var dir = TempDir();
        var existingPath = Path.Combine(dir, "test.db");
        var newPath = Path.Combine(dir, "encrypted.db");
        File.WriteAllBytes(existingPath, new byte[64]); // dummy file

        var keyProvider = new StaticKeyProvider(MakeMasterKey());

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => BLiteEngine.MigrateToEncryptedAsync(null!, newPath, keyProvider));
        await Assert.ThrowsAsync<ArgumentNullException>(
            () => BLiteEngine.MigrateToEncryptedAsync(existingPath, null!, keyProvider));
        await Assert.ThrowsAsync<ArgumentNullException>(
            () => BLiteEngine.MigrateToEncryptedAsync(existingPath, newPath, null!));
        await Assert.ThrowsAsync<FileNotFoundException>(
            () => BLiteEngine.MigrateToEncryptedAsync("/nonexistent/path.db", newPath, keyProvider));
    }

    [Fact]
    public async Task MigrateToEncrypted_PlaintextToEncrypted_DataPreserved()
    {
        var dir = TempDir();
        var srcPath = Path.Combine(dir, "source.db");
        var destPath = Path.Combine(dir, "encrypted.db");
        const int docCount = 20;

        // Create plaintext source
        using (var engine = new BLiteEngine(srcPath))
        {
            await InsertDocumentsAsync(engine, "items", docCount);
        }

        var masterKey = MakeMasterKey();
        var keyProvider = new StaticKeyProvider(masterKey);
        await BLiteEngine.MigrateToEncryptedAsync(srcPath, destPath, keyProvider);

        // Source is untouched
        Assert.True(File.Exists(srcPath));

        // Encrypted target is readable with the master key
        using var coordinator = new EncryptionCoordinator(masterKey);
        using var engine2 = new BLiteEngine(destPath, coordinator);
        var count = await CountDocumentsAsync(engine2, "items");
        Assert.Equal(docCount, count);
    }

    [Fact]
    public async Task MigrateToEncrypted_EncryptedDatabase_IsNotReadableWithoutKey()
    {
        var dir = TempDir();
        var srcPath = Path.Combine(dir, "source.db");
        var destPath = Path.Combine(dir, "encrypted.db");

        using (var engine = new BLiteEngine(srcPath))
        {
            await InsertDocumentsAsync(engine, "items", 5);
        }

        var keyProvider = new StaticKeyProvider(MakeMasterKey(0x42));
        await BLiteEngine.MigrateToEncryptedAsync(srcPath, destPath, keyProvider);

        // Opening without encryption must fail (BLCE header misread as page header)
        Assert.ThrowsAny<Exception>(() => new BLiteEngine(destPath).Dispose());
    }

    [Fact]
    public async Task MigrateToEncrypted_InPlace_DataPreserved()
    {
        var dir = TempDir();
        var dbPath = Path.Combine(dir, "mydb.db");
        const int docCount = 15;

        // Create plaintext source
        using (var engine = new BLiteEngine(dbPath))
        {
            await InsertDocumentsAsync(engine, "docs", docCount);
        }

        Assert.True(File.Exists(dbPath), "Source .db must exist before migration.");

        var masterKey = MakeMasterKey(0x30);
        var keyProvider = new StaticKeyProvider(masterKey);

        // In-place migration
        await BLiteEngine.MigrateToEncryptedAsync(dbPath, dbPath, keyProvider);

        // File must still exist at the same path
        Assert.True(File.Exists(dbPath), "Encrypted .db must exist after in-place migration.");

        // Must be readable with the key
        using var coordinator = new EncryptionCoordinator(masterKey);
        using var engine2 = new BLiteEngine(dbPath, coordinator);
        var count = await CountDocumentsAsync(engine2, "docs");
        Assert.Equal(docCount, count);
    }

    // ── BLiteEngine.MigrateToPlaintextAsync ───────────────────────────────────

    [Fact]
    public async Task MigrateToPlaintext_ArgumentValidation()
    {
        var dir = TempDir();
        var existingPath = Path.Combine(dir, "test.db");
        var newPath = Path.Combine(dir, "plain.db");
        File.WriteAllBytes(existingPath, new byte[128]);

        var keyProvider = new StaticKeyProvider(MakeMasterKey());

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => BLiteEngine.MigrateToPlaintextAsync(null!, newPath, keyProvider));
        await Assert.ThrowsAsync<ArgumentNullException>(
            () => BLiteEngine.MigrateToPlaintextAsync(existingPath, null!, keyProvider));
        await Assert.ThrowsAsync<ArgumentNullException>(
            () => BLiteEngine.MigrateToPlaintextAsync(existingPath, newPath, null!));
        await Assert.ThrowsAsync<FileNotFoundException>(
            () => BLiteEngine.MigrateToPlaintextAsync("/nonexistent/path.db", newPath, keyProvider));
    }

    [Fact]
    public async Task MigrateToPlaintext_EncryptedToPlaintext_DataPreserved()
    {
        var dir = TempDir();
        var encPath = Path.Combine(dir, "encrypted.db");
        var plainPath = Path.Combine(dir, "plaintext.db");
        const int docCount = 25;
        const string secretValue = "SensitiveContent";

        // Create encrypted source
        var masterKey = MakeMasterKey(0x11);
        using (var coordinator = new EncryptionCoordinator(masterKey))
        using (var engine = new BLiteEngine(encPath, coordinator))
        {
            await InsertDocumentsAsync(engine, "records", docCount);
            var col = engine.GetOrCreateCollection("special");
            var doc = col.CreateDocument(["_id", "secret"], b => b.AddString("secret", secretValue));
            await col.InsertAsync(doc);
            await engine.CommitAsync();
        }

        var keyProvider = new StaticKeyProvider(masterKey);
        await BLiteEngine.MigrateToPlaintextAsync(encPath, plainPath, keyProvider);

        // Encrypted source is untouched
        Assert.True(File.Exists(encPath));

        // Plaintext target is readable without encryption
        int count;
        List<BsonDocument> found;
        using (var plainEngine = new BLiteEngine(plainPath))
        {
            count = await CountDocumentsAsync(plainEngine, "records");
            var col2 = plainEngine.GetOrCreateCollection("special");
            found = await col2.FindAllAsync().ToListAsync();
        }

        Assert.Equal(docCount, count);
        Assert.Single(found);
        Assert.True(found[0].TryGetString("secret", out var s));
        Assert.Equal(secretValue, s);

        // And the value should appear in plaintext on disk (after engine is closed)
        var rawBytes = File.ReadAllBytes(plainPath);
        var secretBytes = System.Text.Encoding.UTF8.GetBytes(secretValue);
        Assert.True(rawBytes.AsSpan().IndexOf(secretBytes) >= 0,
            "Plaintext value must be readable in the plaintext-migrated file.");
    }

    [Fact]
    public async Task MigrateToPlaintext_InPlace_DataPreserved()
    {
        var dir = TempDir();
        var dbPath = Path.Combine(dir, "mydb.db");
        const int docCount = 18;

        // Create encrypted source
        var masterKey = MakeMasterKey(0x22);
        using (var coordinator = new EncryptionCoordinator(masterKey))
        using (var engine = new BLiteEngine(dbPath, coordinator))
        {
            await InsertDocumentsAsync(engine, "things", docCount);
        }

        Assert.True(File.Exists(dbPath), "Encrypted .db must exist before migration.");

        var keyProvider = new StaticKeyProvider(masterKey);

        // In-place migration to plaintext
        await BLiteEngine.MigrateToPlaintextAsync(dbPath, dbPath, keyProvider);

        // File must still exist at the same path
        Assert.True(File.Exists(dbPath), "Plaintext .db must exist after in-place migration.");

        // Must be readable without encryption
        using var engine2 = new BLiteEngine(dbPath);
        var count = await CountDocumentsAsync(engine2, "things");
        Assert.Equal(docCount, count);
    }

    // ── Round-trip: Encrypt → Plaintext → re-verify ───────────────────────────

    [Fact]
    public async Task RoundTrip_EncryptThenDecrypt_DataPreserved()
    {
        var dir = TempDir();
        var origPath  = Path.Combine(dir, "original.db");
        var encPath   = Path.Combine(dir, "encrypted.db");
        var plainPath = Path.Combine(dir, "final.db");
        const int docCount = 30;

        // 1. Create plaintext original
        using (var engine = new BLiteEngine(origPath))
        {
            await InsertDocumentsAsync(engine, "col", docCount);
        }

        var masterKey = MakeMasterKey(0x88);
        var kp = new StaticKeyProvider(masterKey);

        // 2. Migrate to encrypted
        await BLiteEngine.MigrateToEncryptedAsync(origPath, encPath, kp);

        // 3. Migrate back to plaintext
        await BLiteEngine.MigrateToPlaintextAsync(encPath, plainPath, kp);

        // 4. Verify data
        using var final = new BLiteEngine(plainPath);
        Assert.Equal(docCount, await CountDocumentsAsync(final, "col"));
    }
}
