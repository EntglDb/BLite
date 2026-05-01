using BLite.Bson;
using BLite.Core;
using BLite.Core.Encryption;
using BLite.Core.Storage;

namespace BLite.Tests;

/// <summary>
/// Tests that an encrypted database backup is itself encrypted with the same key.
/// The backup is a raw byte-for-byte copy of the source file, so the encryption
/// header and encrypted pages are preserved automatically.
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
            try { if (Directory.Exists(d)) Directory.Delete(d, recursive: true); } catch { }
    }

    [Fact]
    public async Task Backup_EncryptedDatabase_BackupIsAlsoEncrypted()
    {
        var dir = TempDir();
        var srcPath = Path.Combine(dir, "source.db");
        var bakPath = Path.Combine(dir, "backup.db");
        const string secret = "TopSecretValue";

        var crypto = new CryptoOptions("my-passphrase", iterations: 1);

        // Create encrypted source database with a known value.
        using (var engine = new BLiteEngine(srcPath, crypto))
        {
            var col = engine.GetOrCreateCollection("items");
            var doc = col.CreateDocument(["_id", "value"], b => b.AddString("value", secret));
            await col.InsertAsync(doc);
            await engine.CommitAsync();
        }

        // Back up the encrypted database.
        using (var engine = new BLiteEngine(srcPath, crypto))
        {
            await engine.BackupAsync(new BackupOptions { DestinationPath = bakPath });
        }

        // The backup file must start with the BLCE crypto header (magic 0x424C4345).
        var rawBytes = File.ReadAllBytes(bakPath);
        Assert.True(rawBytes.Length >= 64, "Backup file must contain the 64-byte BLCE encryption header.");
        var magic = System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(rawBytes.AsSpan(0, 4));
        Assert.Equal(0x424C4345u, magic); // "BLCE" magic stored as little-endian uint

        // The raw bytes must NOT contain the plaintext secret.
        var secretBytes = System.Text.Encoding.UTF8.GetBytes(secret);
        Assert.True(rawBytes.AsSpan().IndexOf(secretBytes) == -1,
            "Plaintext value must not appear unencrypted in the backup file.");
    }

    [Fact]
    public async Task Backup_EncryptedDatabase_IsReadableWithSameKey()
    {
        var dir = TempDir();
        var srcPath = Path.Combine(dir, "source.db");
        var bakPath = Path.Combine(dir, "backup.db");
        const string secret = "RoundTripValue";

        var crypto = new CryptoOptions("my-passphrase", iterations: 1);

        // Create encrypted source.
        using (var engine = new BLiteEngine(srcPath, crypto))
        {
            var col = engine.GetOrCreateCollection("items");
            var doc = col.CreateDocument(["_id", "value"], b => b.AddString("value", secret));
            await col.InsertAsync(doc);
            await engine.CommitAsync();
        }

        // Back up.
        using (var engine = new BLiteEngine(srcPath, crypto))
        {
            await engine.BackupAsync(new BackupOptions { DestinationPath = bakPath });
        }

        // The backup must be readable using the same passphrase.
        using var bakEngine = new BLiteEngine(bakPath, crypto);
        var col2 = bakEngine.GetOrCreateCollection("items");
        var found = await col2.FindAllAsync().ToListAsync();
        Assert.Single(found);
        Assert.True(found[0].TryGetString("value", out var val));
        Assert.Equal(secret, val);
    }

    [Fact]
    public async Task Backup_EncryptedDatabase_NotReadableWithoutKey()
    {
        var dir = TempDir();
        var srcPath = Path.Combine(dir, "source.db");
        var bakPath = Path.Combine(dir, "backup.db");

        var crypto = new CryptoOptions("my-passphrase", iterations: 1);

        using (var engine = new BLiteEngine(srcPath, crypto))
        {
            var col = engine.GetOrCreateCollection("items");
            var doc = col.CreateDocument(["_id", "x"], b => b.AddInt32("x", 1));
            await col.InsertAsync(doc);
            await engine.CommitAsync();
        }

        using (var engine = new BLiteEngine(srcPath, crypto))
        {
            await engine.BackupAsync(new BackupOptions { DestinationPath = bakPath });
        }

        // Opening the backup without any encryption must fail.
        Assert.ThrowsAny<Exception>(() => new BLiteEngine(bakPath).Dispose());
    }
}
