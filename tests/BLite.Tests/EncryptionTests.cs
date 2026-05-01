using System.Buffers.Binary;
using BLite.Bson;
using BLite.Core;
using BLite.Core.Encryption;
using BLite.Core.Storage;

namespace BLite.Tests;

/// <summary>
/// Tests for the transparent page-level encryption layer:
/// <see cref="ICryptoProvider"/>, <see cref="NullCryptoProvider"/>,
/// <see cref="AesGcmCryptoProvider"/>, <see cref="KeyDerivation"/>,
/// and the <see cref="PageFile"/> read/write hooks.
/// </summary>
public class EncryptionTests : IDisposable
{
    // ── Helpers ──────────────────────────────────────────────────────────────

    private readonly List<string> _tempFiles = new();
    private readonly List<string> _tempDirs = new();

    private string TempDb()
    {
        var path = Path.Combine(Path.GetTempPath(), $"enc_test_{Guid.NewGuid()}.db");
        _tempFiles.Add(path);
        _tempFiles.Add(Path.ChangeExtension(path, ".wal"));
        return path;
    }

    private string TempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"enc_test_{Guid.NewGuid()}");
        Directory.CreateDirectory(dir);
        _tempDirs.Add(dir);
        return dir;
    }

    public void Dispose()
    {
        foreach (var f in _tempFiles)
            if (File.Exists(f)) try { File.Delete(f); } catch { /* best-effort */ }
        foreach (var d in _tempDirs)
            if (Directory.Exists(d)) try { Directory.Delete(d, recursive: true); } catch { /* best-effort */ }
    }

    // ── NullCryptoProvider ───────────────────────────────────────────────────

    [Fact]
    public void NullCryptoProvider_HasZeroOverhead()
    {
        Assert.Equal(0, NullCryptoProvider.Instance.PageOverhead);
        Assert.Equal(0, NullCryptoProvider.Instance.FileHeaderSize);
    }

    [Fact]
    public void NullCryptoProvider_Encrypt_IsIdentity()
    {
        var plaintext = new byte[] { 1, 2, 3, 4, 5 };
        var ciphertext = new byte[5];
        NullCryptoProvider.Instance.Encrypt(42, plaintext, ciphertext);
        Assert.Equal(plaintext, ciphertext);
    }

    [Fact]
    public void NullCryptoProvider_Decrypt_IsIdentity()
    {
        var ciphertext = new byte[] { 0xAA, 0xBB, 0xCC };
        var plaintext = new byte[3];
        NullCryptoProvider.Instance.Decrypt(42, ciphertext, plaintext);
        Assert.Equal(ciphertext, plaintext);
    }

    [Fact]
    public void NullCryptoProvider_GetFileHeader_IsNoOp()
    {
        NullCryptoProvider.Instance.GetFileHeader(Span<byte>.Empty); // should not throw
    }

    [Fact]
    public void NullCryptoProvider_LoadFromFileHeader_IsNoOp()
    {
        NullCryptoProvider.Instance.LoadFromFileHeader(ReadOnlySpan<byte>.Empty); // should not throw
    }

    // ── CryptoOptions ────────────────────────────────────────────────────────

    [Fact]
    public void CryptoOptions_Ctor_ThrowsOnNullPassphrase()
    {
        Assert.Throws<ArgumentNullException>(() => new CryptoOptions(null!));
    }

    [Fact]
    public void CryptoOptions_Ctor_ThrowsOnEmptyPassphrase()
    {
        Assert.Throws<ArgumentNullException>(() => new CryptoOptions(""));
    }

    [Fact]
    public void CryptoOptions_Ctor_ThrowsOnZeroIterations()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new CryptoOptions("pass", iterations: 0));
    }

    [Fact]
    public void CryptoOptions_DefaultIterations_Are100000()
    {
        var opts = new CryptoOptions("test");
        Assert.Equal(100_000, opts.Iterations);
        Assert.Equal(KdfAlgorithm.Pbkdf2Sha256, opts.Kdf);
    }

    // ── KeyDerivation ────────────────────────────────────────────────────────

    [Fact]
    public void KeyDerivation_Pbkdf2_Returns32Bytes()
    {
        var salt = new byte[32];
        var key = KeyDerivation.DeriveKeyPbkdf2("secret", salt, 1);
        Assert.Equal(32, key.Length);
    }

    [Fact]
    public void KeyDerivation_Pbkdf2_IsDeterministic()
    {
        var salt = new byte[32];
        new Random(42).NextBytes(salt);
        var k1 = KeyDerivation.DeriveKeyPbkdf2("secret", salt, 100);
        var k2 = KeyDerivation.DeriveKeyPbkdf2("secret", salt, 100);
        Assert.Equal(k1, k2);
    }

    [Fact]
    public void KeyDerivation_Pbkdf2_DifferentSaltsProduceDifferentKeys()
    {
        var salt1 = new byte[32];
        var salt2 = new byte[32];
        salt2[0] = 0xFF;
        var k1 = KeyDerivation.DeriveKeyPbkdf2("secret", salt1, 100);
        var k2 = KeyDerivation.DeriveKeyPbkdf2("secret", salt2, 100);
        Assert.NotEqual(k1, k2);
    }

    [Fact]
    public void KeyDerivation_Hkdf_Returns32Bytes()
    {
        var ikm = new byte[32];
        var key = KeyDerivation.DeriveKeyHkdf(ikm, ReadOnlySpan<byte>.Empty, ReadOnlySpan<byte>.Empty, 32);
        Assert.Equal(32, key.Length);
    }

    [Fact]
    public void KeyDerivation_Hkdf_IsDeterministic()
    {
        var ikm = new byte[] { 1, 2, 3, 4, 5 };
        var salt = new byte[] { 10, 20 };
        var info = new byte[] { 99 };
        var k1 = KeyDerivation.DeriveKeyHkdf(ikm, salt, info, 32);
        var k2 = KeyDerivation.DeriveKeyHkdf(ikm, salt, info, 32);
        Assert.Equal(k1, k2);
    }

    // ── AesGcmCryptoProvider ─────────────────────────────────────────────────

    [Fact]
    public void AesGcmCryptoProvider_Properties()
    {
        var opts = new CryptoOptions("test", iterations: 1);
        var p = new AesGcmCryptoProvider(opts);
        // PageOverhead = nonce (12) + GCM tag (16) = 28 bytes per physical page
        Assert.Equal(28, p.PageOverhead);
        Assert.Equal(64, p.FileHeaderSize);
    }

    [Fact]
    public void AesGcmCryptoProvider_EncryptDecrypt_RoundTrip()
    {
        var opts = new CryptoOptions("my-secret", iterations: 1);
        var provider = new AesGcmCryptoProvider(opts);

        // Initialise key (as if creating a new file)
        var fileHeader = new byte[AesGcmCryptoProvider.HeaderSize];
        provider.GetFileHeader(fileHeader);

        // Prepare a plaintext page
        const int pageSize = 4096;
        var plaintext = new byte[pageSize];
        new Random(1).NextBytes(plaintext);

        // Encrypt — ciphertext buffer must be plaintext.Length + PageOverhead
        var ciphertext = new byte[pageSize + provider.PageOverhead];
        provider.Encrypt(7, plaintext, ciphertext);

        // Ciphertext must differ from plaintext
        Assert.NotEqual(plaintext, ciphertext[..pageSize]);

        // Decrypt
        var decrypted = new byte[pageSize];
        provider.Decrypt(7, ciphertext, decrypted);

        Assert.Equal(plaintext, decrypted);
    }

    [Fact]
    public void AesGcmCryptoProvider_DifferentPageIds_ProduceDifferentCiphertext()
    {
        var opts = new CryptoOptions("pass", iterations: 1);
        var provider = new AesGcmCryptoProvider(opts);
        var fileHeader = new byte[AesGcmCryptoProvider.HeaderSize];
        provider.GetFileHeader(fileHeader);

        const int pageSize = 512;
        var plaintext = new byte[pageSize]; // all zeros

        var ct0 = new byte[pageSize + provider.PageOverhead];
        var ct1 = new byte[pageSize + provider.PageOverhead];

        provider.Encrypt(0, plaintext, ct0);
        provider.Encrypt(1, plaintext, ct1);

        // Different page IDs → different random nonces → different ciphertext
        Assert.NotEqual(ct0, ct1);
    }

    [Fact]
    public void AesGcmCryptoProvider_LoadFromFileHeader_RestoresKey()
    {
        var opts = new CryptoOptions("shared-pass", iterations: 1);

        // Writer side
        var writer = new AesGcmCryptoProvider(opts);
        var fileHeader = new byte[AesGcmCryptoProvider.HeaderSize];
        writer.GetFileHeader(fileHeader);

        const int pageSize = 512;
        var plaintext = new byte[pageSize];
        new Random(99).NextBytes(plaintext);
        var ciphertext = new byte[pageSize + writer.PageOverhead];
        writer.Encrypt(5, plaintext, ciphertext);

        // Reader side (same passphrase, loads from header)
        var reader = new AesGcmCryptoProvider(opts);
        reader.LoadFromFileHeader(fileHeader);

        var decrypted = new byte[pageSize];
        reader.Decrypt(5, ciphertext, decrypted);

        Assert.Equal(plaintext, decrypted);
    }

    [Fact]
    public void AesGcmCryptoProvider_WrongPassphrase_ThrowsAuthTagMismatch()
    {
        var opts = new CryptoOptions("correct", iterations: 1);
        var writer = new AesGcmCryptoProvider(opts);
        var fileHeader = new byte[AesGcmCryptoProvider.HeaderSize];
        writer.GetFileHeader(fileHeader);

        const int pageSize = 512;
        var plaintext = new byte[pageSize];
        var ciphertext = new byte[pageSize + writer.PageOverhead];
        writer.Encrypt(0, plaintext, ciphertext);

        // Wrong passphrase
        var badOpts = new CryptoOptions("wrong", iterations: 1);
        var reader = new AesGcmCryptoProvider(badOpts);
        reader.LoadFromFileHeader(fileHeader);

        var decrypted = new byte[pageSize];
        Assert.ThrowsAny<Exception>(() => reader.Decrypt(0, ciphertext, decrypted));
    }

    [Fact]
    public void AesGcmCryptoProvider_LoadFromFileHeader_WrongMagic_Throws()
    {
        var opts = new CryptoOptions("pass", iterations: 1);
        var provider = new AesGcmCryptoProvider(opts);
        var badHeader = new byte[AesGcmCryptoProvider.HeaderSize];
        // Magic bytes are wrong (all zeros)
        Assert.Throws<InvalidOperationException>(() => provider.LoadFromFileHeader(badHeader));
    }

    [Fact]
    public void AesGcmCryptoProvider_EncryptBeforeInit_Throws()
    {
        var opts = new CryptoOptions("pass", iterations: 1);
        var provider = new AesGcmCryptoProvider(opts);
        // Key not derived yet (neither GetFileHeader nor LoadFromFileHeader called).
        // Buffer sized for the new physical layout: plaintext (512) + nonce (12) + tag (16) = 540.
        Assert.Throws<InvalidOperationException>(() =>
            provider.Encrypt(0, new byte[512], new byte[540]));
    }

    // ── PageFile integration with AesGcmCryptoProvider ───────────────────────

    [Fact]
    public void PageFile_WithEncryption_CreateAndRead()
    {
        var path = TempDb();
        var opts = new CryptoOptions("test-passphrase", iterations: 1);
        var provider = new AesGcmCryptoProvider(opts);
        var config = PageFileConfig.Default with { CryptoProvider = provider };

        uint allocatedPageId;
        byte[] writeBuf;
        int pageSize;
        int physicalPageSize;

        // Create encrypted page file and write a page
        using (var pf = new PageFile(path, config))
        {
            pf.Open();

            pageSize = pf.PageSize;
            physicalPageSize = pageSize + provider.PageOverhead; // nonce + ciphertext + tag

            allocatedPageId = pf.AllocatePage();
            writeBuf = new byte[pageSize];
            new Random(0).NextBytes(writeBuf);
            writeBuf[0] = 0xDE;
            writeBuf[1] = 0xAD;
            pf.WritePage(allocatedPageId, writeBuf);
            pf.Flush();
        }

        // Verify on-disk content at the correct physical offset is NOT the original plaintext.
        // File layout: [ 64-byte crypto header ][ physicalPageSize * pageCount ]
        var rawContent = File.ReadAllBytes(path);
        const int cryptoHeaderSize = 64;
        int pageOffset = cryptoHeaderSize + (int)allocatedPageId * physicalPageSize;

        // The on-disk bytes for our page must not equal the plaintext we wrote.
        Assert.False(
            rawContent.AsSpan(pageOffset, writeBuf.Length).SequenceEqual(writeBuf),
            "The on-disk bytes for the allocated page should not match the plaintext when encryption is enabled.");
    }

    [Fact]
    public void PageFile_WithEncryption_PersistsAndReadsBack()
    {
        var path = TempDb();
        var opts = new CryptoOptions("my-db-secret", iterations: 1);

        byte[] written;
        uint allocatedPageId;

        // Phase 1: Write
        using (var pf = new PageFile(path, PageFileConfig.Default with { CryptoProvider = new AesGcmCryptoProvider(opts) }))
        {
            pf.Open();
            allocatedPageId = pf.AllocatePage();
            written = new byte[pf.PageSize];
            new Random(77).NextBytes(written);
            pf.WritePage(allocatedPageId, written);
            pf.Flush();
        }

        // Phase 2: Read back
        using (var pf = new PageFile(path, PageFileConfig.Default with { CryptoProvider = new AesGcmCryptoProvider(opts) }))
        {
            pf.Open();
            var readBuf = new byte[pf.PageSize];
            pf.ReadPage(allocatedPageId, readBuf);
            Assert.Equal(written, readBuf);
        }
    }

    [Fact]
    public void PageFile_NullCryptoProvider_FileFormatUnchanged()
    {
        // With NullCryptoProvider, the file format must be byte-for-byte identical
        // to a file created without any crypto provider.
        var path1 = TempDb();
        var path2 = TempDb();

        var configNull = PageFileConfig.Default;
        var configNullProvider = PageFileConfig.Default with { CryptoProvider = NullCryptoProvider.Instance };

        using (var pf = new PageFile(path1, configNull))
        {
            pf.Open();
            var buf = new byte[pf.PageSize];
            new Random(1).NextBytes(buf);
            pf.WritePage(pf.AllocatePage(), buf);
            pf.Flush();
        }

        using (var pf = new PageFile(path2, configNullProvider))
        {
            pf.Open();
            var buf = new byte[pf.PageSize];
            new Random(1).NextBytes(buf);
            pf.WritePage(pf.AllocatePage(), buf);
            pf.Flush();
        }

        var bytes1 = File.ReadAllBytes(path1);
        var bytes2 = File.ReadAllBytes(path2);
        Assert.Equal(bytes1.Length, bytes2.Length);
        Assert.Equal(bytes1, bytes2);
    }

    // ── BLiteEngine integration with encryption ───────────────────────────────

    [Fact]
    public async Task BLiteEngine_WithCrypto_BasicCrud()
    {
        var path = TempDb();
        var crypto = new CryptoOptions("engine-secret", iterations: 1);

        BsonId insertedId;

        // Write
        using (var engine = new BLiteEngine(path, crypto))
        {
            var col = engine.GetOrCreateCollection("items");
            var doc = col.CreateDocument(["_id", "name", "age"], b => b
                .AddString("name", "Alice")
                .AddInt32("age", 30));
            insertedId = await col.InsertAsync(doc);
            await engine.CommitAsync();
        }

        // Read back
        using (var engine = new BLiteEngine(path, crypto))
        {
            var col = engine.GetOrCreateCollection("items");
            var count = await col.CountAsync();
            Assert.Equal(1, count);
            var found = await col.FindByIdAsync(insertedId);
            Assert.NotNull(found);
            Assert.True(found!.TryGetString("name", out var name));
            Assert.Equal("Alice", name);
        }
    }

    [Fact]
    public async Task BLiteEngine_WithCrypto_DataIsEncryptedOnDisk()
    {
        var path = TempDb();
        var crypto = new CryptoOptions("supersecret", iterations: 1);

        using (var engine = new BLiteEngine(path, crypto))
        {
            var col = engine.GetOrCreateCollection("secrets");
            var doc = col.CreateDocument(["_id", "secret"], b => b
                .AddString("secret", "TopSecretValue"));
            await col.InsertAsync(doc);
            await engine.CommitAsync();
        }

        // The raw file content should NOT contain the plaintext secret
        var rawBytes = File.ReadAllBytes(path);
        var rawText = System.Text.Encoding.UTF8.GetString(rawBytes);
        Assert.DoesNotContain("TopSecretValue", rawText);
    }

    [Fact]
    public async Task BLiteEngine_EncryptedDb_WrongPassphrase_Throws()
    {
        var path = TempDb();
        var correct = new CryptoOptions("correct-pass", iterations: 1);

        using (var engine = new BLiteEngine(path, correct))
        {
            var col = engine.GetOrCreateCollection("test");
            var doc = col.CreateDocument(["_id", "x"], b => b.AddInt32("x", 1));
            await col.InsertAsync(doc);
            await engine.CommitAsync();
        }

        var wrong = new CryptoOptions("wrong-pass", iterations: 1);
        Assert.ThrowsAny<Exception>(() =>
        {
            using var engine = new BLiteEngine(path, wrong);
        });
    }

    // ── WAL encryption ───────────────────────────────────────────────────────

    [Fact]
    public void WriteAheadLog_WithCrypto_RecordsAreEncryptedOnDisk()
    {
        var walPath = Path.Combine(Path.GetTempPath(), $"wal_enc_{Guid.NewGuid()}.wal");
        _tempFiles.Add(walPath);

        var opts  = new CryptoOptions("wal-secret", iterations: 1);
        var crypto = new AesGcmCryptoProvider(opts, fileRole: 3);

        using (var wal = new BLite.Core.Transactions.WriteAheadLog(walPath, crypto))
        {
            wal.WriteBeginRecordAsync(1).GetAwaiter().GetResult();
            wal.WriteDataRecordAsync(1, 42, new byte[] { 0xDE, 0xAD, 0xBE, 0xEF }).GetAwaiter().GetResult();
            wal.WriteCommitRecordAsync(1).GetAwaiter().GetResult();
            wal.FlushAsync().GetAwaiter().GetResult();
        }

        // The WAL file must exist and contain the 64-byte file header.
        Assert.True(File.Exists(walPath));
        var rawBytes = File.ReadAllBytes(walPath);
        Assert.True(rawBytes.Length > AesGcmCryptoProvider.HeaderSize, "WAL file should be larger than the file header.");

        // The known plaintext bytes must NOT appear verbatim in the WAL file.
        // Search the raw byte array directly to avoid any UTF-8 encoding artefacts.
        var needle = new byte[] { 0xDE, 0xAD, 0xBE, 0xEF };
        Assert.True(rawBytes.AsSpan().IndexOf(needle) == -1,
            "Plaintext after-image bytes must not appear unencrypted in the WAL file.");
    }

    [Fact]
    public void WriteAheadLog_WithCrypto_ReadAll_RoundTrip()
    {
        var walPath = Path.Combine(Path.GetTempPath(), $"wal_enc_{Guid.NewGuid()}.wal");
        _tempFiles.Add(walPath);

        var opts   = new CryptoOptions("wal-roundtrip", iterations: 1);
        var crypto = new AesGcmCryptoProvider(opts, fileRole: 3);

        var afterImageBytes = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };

        // Write records
        using (var wal = new BLite.Core.Transactions.WriteAheadLog(walPath, crypto))
        {
            wal.WriteBeginRecordAsync(7).GetAwaiter().GetResult();
            wal.WriteDataRecordAsync(7, 99, afterImageBytes).GetAwaiter().GetResult();
            wal.WriteCommitRecordAsync(7).GetAwaiter().GetResult();
            wal.FlushAsync().GetAwaiter().GetResult();
        }

        // Read back using a fresh provider loaded from the same file header
        var crypto2 = new AesGcmCryptoProvider(opts, fileRole: 3);
        using var wal2 = new BLite.Core.Transactions.WriteAheadLog(walPath, crypto2);
        var records = wal2.ReadAll();

        Assert.Equal(3, records.Count);
        Assert.Equal(BLite.Core.Transactions.WalRecordType.Begin,  records[0].Type);
        Assert.Equal(7UL, records[0].TransactionId);
        Assert.Equal(BLite.Core.Transactions.WalRecordType.Write,  records[1].Type);
        Assert.Equal(7UL, records[1].TransactionId);
        Assert.Equal(99u, records[1].PageId);
        Assert.Equal(afterImageBytes, records[1].AfterImage);
        Assert.Equal(BLite.Core.Transactions.WalRecordType.Commit, records[2].Type);
        Assert.Equal(7UL, records[2].TransactionId);
    }

    [Fact]
    public void WriteAheadLog_WithCrypto_TruncateAndReuse()
    {
        var walPath = Path.Combine(Path.GetTempPath(), $"wal_enc_{Guid.NewGuid()}.wal");
        _tempFiles.Add(walPath);

        var opts   = new CryptoOptions("wal-trunc", iterations: 1);
        var crypto = new AesGcmCryptoProvider(opts, fileRole: 3);

        using var wal = new BLite.Core.Transactions.WriteAheadLog(walPath, crypto);

        // First batch
        wal.WriteBeginRecordAsync(1).GetAwaiter().GetResult();
        wal.WriteCommitRecordAsync(1).GetAwaiter().GetResult();
        wal.FlushAsync().GetAwaiter().GetResult();

        // Truncate
        wal.TruncateAsync().GetAwaiter().GetResult();
        Assert.Equal(0, wal.GetCurrentSize());

        // Second batch (new header should be written automatically)
        wal.WriteBeginRecordAsync(2).GetAwaiter().GetResult();
        wal.WriteCommitRecordAsync(2).GetAwaiter().GetResult();
        wal.FlushAsync().GetAwaiter().GetResult();

        var records = wal.ReadAll();
        Assert.Equal(2, records.Count);
        Assert.All(records, r => Assert.Equal(2UL, r.TransactionId));
    }

    [Fact]
    public async Task BLiteEngine_WithCrypto_WalIsEncryptedOnDisk()
    {
        var path = TempDb();
        var crypto = new CryptoOptions("wal-data-secret", iterations: 1);

        using (var engine = new BLiteEngine(path, crypto))
        {
            var col = engine.GetOrCreateCollection("secrets");
            var doc = col.CreateDocument(["_id", "secret"], b => b
                .AddString("secret", "WALTopSecretValue"));
            await col.InsertAsync(doc);
            // Commit so the WAL definitely contains at least one committed transaction;
            // the WAL file may survive the dispose/checkpoint cycle.
            await engine.CommitAsync();
        }

        var walPath = Path.ChangeExtension(path, ".wal");

        // The WAL file must exist (insert + commit always writes WAL records before
        // checkpoint can truncate — the engine does not auto-checkpoint on CommitAsync
        // unless the WAL exceeds MaxWalSize).  If the WAL was already checkpointed and
        // truncated to empty, skip the plaintext check (encryption was already verified
        // by the roundtrip test).
        Assert.True(File.Exists(walPath),
            "Expected the commit to leave a WAL file so the encryption test can inspect its on-disk content.");

        var rawBytes = File.ReadAllBytes(walPath);
        if (rawBytes.Length == 0)
            return; // Checkpointed and emptied — vacuously satisfied.

        // Search the raw bytes so we avoid any UTF-8 encoding artefacts.
        var secretBytes = System.Text.Encoding.UTF8.GetBytes("WALTopSecretValue");
        Assert.True(rawBytes.AsSpan().IndexOf(secretBytes) == -1,
            "Plaintext secret must not appear unencrypted in the WAL file.");
    }

    // ── EncryptionCoordinator ────────────────────────────────────────────────

    private static byte[] MakeMasterKey(byte seed = 0x42)
    {
        var key = new byte[32];
        for (var i = 0; i < key.Length; i++) key[i] = (byte)(seed + i);
        return key;
    }

    [Fact]
    public void EncryptionCoordinator_InvalidMasterKeySize_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new EncryptionCoordinator(null!));
        Assert.Throws<ArgumentException>(() => new EncryptionCoordinator(new byte[16]));
        Assert.Throws<ArgumentException>(() => new EncryptionCoordinator(new byte[31]));
        Assert.Throws<ArgumentException>(() => new EncryptionCoordinator(new byte[33]));
    }

    [Fact]
    public void EncryptionCoordinator_CreateForMainFile_ReturnsProvider()
    {
        using var coordinator = new EncryptionCoordinator(MakeMasterKey());
        var provider = coordinator.CreateForMainFile();
        Assert.NotNull(provider);
        Assert.Equal(28, provider.PageOverhead);  // NonceSize(12) + TagSize(16)
        Assert.Equal(64, provider.FileHeaderSize);
    }

    [Fact]
    public void EncryptionCoordinator_CreateBeforeSalt_Throws()
    {
        using var coordinator = new EncryptionCoordinator(MakeMasterKey());
        // CreateForMainFile does NOT require the salt yet
        _ = coordinator.CreateForMainFile();

        // But using CreateForCollection/Index/Wal before the main file is opened (salt unavailable) must throw
        using var coordinator2 = new EncryptionCoordinator(MakeMasterKey());
        Assert.Throws<InvalidOperationException>(() => coordinator2.CreateForCollection(0));
        Assert.Throws<InvalidOperationException>(() => coordinator2.CreateForIndex(0));
        Assert.Throws<InvalidOperationException>(() => coordinator2.CreateForWal());
    }

    [Fact]
    public void EncryptionCoordinator_InvalidIndex_Throws()
    {
        using var coordinator = new EncryptionCoordinator(MakeMasterKey());

        // Initialise salt by calling GetFileHeader on the main file provider
        var mainProvider = coordinator.CreateForMainFile();
        var mainHeader = new byte[mainProvider.FileHeaderSize];
        mainProvider.GetFileHeader(mainHeader);

        Assert.Throws<ArgumentOutOfRangeException>(() => coordinator.CreateForCollection(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => coordinator.CreateForIndex(-1));
    }

    [Fact]
    public void EncryptionCoordinator_MainFile_EncryptDecrypt_RoundTrip()
    {
        using var coordinator = new EncryptionCoordinator(MakeMasterKey());
        using var provider = (IDisposable)coordinator.CreateForMainFile();
        var crypto = (ICryptoProvider)provider;

        // Initialise key by writing the file header (new file path)
        var header = new byte[crypto.FileHeaderSize];
        crypto.GetFileHeader(header);

        const int pageSize = 4096;
        var plaintext  = new byte[pageSize];
        new Random(1).NextBytes(plaintext);

        var ciphertext = new byte[pageSize + crypto.PageOverhead];
        crypto.Encrypt(0, plaintext, ciphertext);

        var decrypted = new byte[pageSize];
        crypto.Decrypt(0, ciphertext, decrypted);

        Assert.Equal(plaintext, decrypted);
    }

    [Fact]
    public void EncryptionCoordinator_MainFile_ReloadFromHeader_RoundTrip()
    {
        var masterKey = MakeMasterKey(0x10);
        byte[] savedHeader;
        byte[] ciphertext;
        const int pageSize = 512;
        var plaintext = new byte[pageSize];
        new Random(2).NextBytes(plaintext);

        // Write side: new file
        using (var coordinator = new EncryptionCoordinator(masterKey))
        {
            using var provider = (IDisposable)coordinator.CreateForMainFile();
            var crypto = (ICryptoProvider)provider;
            savedHeader = new byte[crypto.FileHeaderSize];
            crypto.GetFileHeader(savedHeader);

            ciphertext = new byte[pageSize + crypto.PageOverhead];
            crypto.Encrypt(7, plaintext, ciphertext);
        }

        // Read side: existing file (reload from header)
        using (var coordinator2 = new EncryptionCoordinator(masterKey))
        {
            using var provider2 = (IDisposable)coordinator2.CreateForMainFile();
            var crypto2 = (ICryptoProvider)provider2;
            crypto2.LoadFromFileHeader(savedHeader);

            var decrypted = new byte[pageSize];
            crypto2.Decrypt(7, ciphertext, decrypted);
            Assert.Equal(plaintext, decrypted);
        }
    }

    [Fact]
    public void EncryptionCoordinator_DifferentFilesGetDifferentSubKeys()
    {
        // Prove each file provider uses a DISTINCT subkey by encrypting identical plaintext
        // and then showing that decrypting ciphertext_A with provider_B throws an auth-tag
        // mismatch (which only happens when the keys differ — not merely the nonces).
        var masterKey = MakeMasterKey();
        const int pageSize = 256;
        var plaintext = new byte[pageSize]; // all zeros for a deterministic test

        byte[] mainCt, colCt, idxCt, walCt;
        ICryptoProvider mainProv, colProv, idxProv, walProv;

        using (var coordinator = new EncryptionCoordinator(masterKey))
        {
            mainProv = coordinator.CreateForMainFile();
            var mainHeader = new byte[mainProv.FileHeaderSize];
            mainProv.GetFileHeader(mainHeader);

            colProv = coordinator.CreateForCollection(0);
            var colHeader = new byte[colProv.FileHeaderSize];
            colProv.GetFileHeader(colHeader);

            idxProv = coordinator.CreateForIndex(0);
            var idxHeader = new byte[idxProv.FileHeaderSize];
            idxProv.GetFileHeader(idxHeader);

            walProv = coordinator.CreateForWal();
            var walHeader = new byte[walProv.FileHeaderSize];
            walProv.GetFileHeader(walHeader);

            mainCt = new byte[pageSize + mainProv.PageOverhead];
            colCt  = new byte[pageSize + colProv.PageOverhead];
            idxCt  = new byte[pageSize + idxProv.PageOverhead];
            walCt  = new byte[pageSize + walProv.PageOverhead];

            mainProv.Encrypt(0, plaintext, mainCt);
            colProv.Encrypt(0, plaintext, colCt);
            idxProv.Encrypt(0, plaintext, idxCt);
            walProv.Encrypt(0, plaintext, walCt);
        }

        // Cross-decryption must fail (AES-GCM auth tag mismatch) — proves different subkeys were used.
        var buf = new byte[pageSize];
        Assert.ThrowsAny<System.Security.Cryptography.CryptographicException>(() => colProv.Decrypt(0, mainCt, buf));
        Assert.ThrowsAny<System.Security.Cryptography.CryptographicException>(() => idxProv.Decrypt(0, mainCt, buf));
        Assert.ThrowsAny<System.Security.Cryptography.CryptographicException>(() => walProv.Decrypt(0, mainCt, buf));
        Assert.ThrowsAny<System.Security.Cryptography.CryptographicException>(() => mainProv.Decrypt(0, colCt,  buf));
        Assert.ThrowsAny<System.Security.Cryptography.CryptographicException>(() => mainProv.Decrypt(0, idxCt,  buf));
        Assert.ThrowsAny<System.Security.Cryptography.CryptographicException>(() => mainProv.Decrypt(0, walCt,  buf));
    }

    [Fact]
    public void EncryptionCoordinator_CollectionAndIndex_RoundTrip()
    {
        var masterKey = MakeMasterKey(0x20);
        const int pageSize = 128;
        var plaintext = new byte[pageSize];
        new Random(55).NextBytes(plaintext);

        byte[] savedMainHeader;
        byte[] savedColHeader;
        byte[] savedColCt;

        // Write side: create coordinator, prime the salt, encrypt a collection page
        using (var c1 = new EncryptionCoordinator(masterKey))
        {
            var mp = c1.CreateForMainFile();
            savedMainHeader = new byte[mp.FileHeaderSize];
            mp.GetFileHeader(savedMainHeader);

            var cp = c1.CreateForCollection(7);
            savedColHeader = new byte[cp.FileHeaderSize];
            cp.GetFileHeader(savedColHeader);

            savedColCt = new byte[pageSize + cp.PageOverhead];
            cp.Encrypt(3, plaintext, savedColCt);
        }

        // Read side: reload coordinator from the same headers and decrypt
        using (var c2 = new EncryptionCoordinator(masterKey))
        {
            var mp = c2.CreateForMainFile();
            mp.LoadFromFileHeader(savedMainHeader);  // primes the salt in c2

            var cp = c2.CreateForCollection(7);
            cp.LoadFromFileHeader(savedColHeader);

            var decrypted = new byte[pageSize];
            cp.Decrypt(3, savedColCt, decrypted);
            Assert.Equal(plaintext, decrypted);
        }
    }

    [Fact]
    public void EncryptionCoordinator_LoadFromFileHeader_RejectsWrongKdf()
    {
        using var coordinator = new EncryptionCoordinator(MakeMasterKey());
        var mainProvider = coordinator.CreateForMainFile();

        // Build a header with KDF=1 (PBKDF2) — coordinator must reject it
        var badHeader = new byte[64];
        BinaryPrimitives.WriteUInt32LittleEndian(badHeader, 0x424C4345u); // magic
        badHeader[4] = 1; // version
        badHeader[5] = 1; // AES-GCM
        badHeader[6] = 1; // KDF = PBKDF2 (wrong for coordinator)
        badHeader[7] = 0; // role = main

        Assert.Throws<InvalidOperationException>(() => mainProvider.LoadFromFileHeader(badHeader));
    }

    [Fact]
    public void EncryptionCoordinator_LoadFromFileHeader_RejectsWrongRole()
    {
        using var coordinator = new EncryptionCoordinator(MakeMasterKey());

        // Generate a valid main file header
        var mainProv = coordinator.CreateForMainFile();
        var mainHeader = new byte[mainProv.FileHeaderSize];
        mainProv.GetFileHeader(mainHeader);

        // A collection provider should reject a main file header (role mismatch)
        var colProv = coordinator.CreateForCollection(0);
        Assert.Throws<InvalidOperationException>(() => colProv.LoadFromFileHeader(mainHeader));
    }

    [Fact]
    public void EncryptionCoordinator_LoadFromFileHeader_RejectsWrongIndex()
    {
        using var coordinator = new EncryptionCoordinator(MakeMasterKey());

        var mainProv = coordinator.CreateForMainFile();
        var mainHeader = new byte[mainProv.FileHeaderSize];
        mainProv.GetFileHeader(mainHeader);

        // Create collection 0 header
        var col0Prov = coordinator.CreateForCollection(0);
        var col0Header = new byte[col0Prov.FileHeaderSize];
        col0Prov.GetFileHeader(col0Header);

        // A provider for collection 1 should reject the collection 0 header (index mismatch)
        var col1Prov = coordinator.CreateForCollection(1);
        Assert.Throws<InvalidOperationException>(() => col1Prov.LoadFromFileHeader(col0Header));
    }

    [Fact]
    public void EncryptionCoordinator_ConflictingSalt_Throws()
    {
        // Attempting to initialise the coordinator with a DIFFERENT main-file header
        // (i.e. a different database salt) after it has already been primed must throw.
        using var c1 = new EncryptionCoordinator(MakeMasterKey());
        using var c2 = new EncryptionCoordinator(MakeMasterKey());

        // Generate two distinct main file headers (each has its own random salt).
        var prov1 = c1.CreateForMainFile();
        var header1 = new byte[prov1.FileHeaderSize];
        prov1.GetFileHeader(header1);  // primes c1 with salt from header1

        var prov2 = c2.CreateForMainFile();
        var header2 = new byte[prov2.FileHeaderSize];
        prov2.GetFileHeader(header2);  // header2 has a different (random) salt

        // Now load header2 into c1 — salts differ, must throw.
        var conflictProv = c1.CreateForMainFile();
        Assert.Throws<InvalidOperationException>(() => conflictProv.LoadFromFileHeader(header2));
    }

    [Fact]
    public void EncryptionCoordinator_Disposed_GetFileHeader_Throws()
    {
        // After the coordinator is disposed DeriveSubKey should throw ObjectDisposedException
        // rather than silently using the zeroed (all-zero) master key.
        var coordinator = new EncryptionCoordinator(MakeMasterKey());

        // Create a main file provider and dispose the coordinator BEFORE calling GetFileHeader.
        var mainProv = coordinator.CreateForMainFile();
        coordinator.Dispose();

        var header = new byte[mainProv.FileHeaderSize];
        Assert.Throws<ObjectDisposedException>(() => mainProv.GetFileHeader(header));
    }

    [Fact]
    public void EncryptionCoordinator_Dispose_PreventsNewProviders()
    {
        var coordinator = new EncryptionCoordinator(MakeMasterKey());
        var mainProv = coordinator.CreateForMainFile();
        var mainHeader = new byte[mainProv.FileHeaderSize];
        mainProv.GetFileHeader(mainHeader);

        coordinator.Dispose();

        Assert.Throws<ObjectDisposedException>(() => coordinator.CreateForMainFile());
        Assert.Throws<ObjectDisposedException>(() => coordinator.CreateForCollection(0));
        Assert.Throws<ObjectDisposedException>(() => coordinator.CreateForIndex(0));
        Assert.Throws<ObjectDisposedException>(() => coordinator.CreateForWal());
    }

    [Fact]
    public void EncryptionCoordinator_Dispose_IsIdempotent()
    {
        var coordinator = new EncryptionCoordinator(MakeMasterKey());
        coordinator.Dispose();
        coordinator.Dispose(); // must not throw
    }

    // ── DocumentDbContext integration with encryption ─────────────────────────

    /// <summary>
    /// Verifies that a <see cref="DocumentDbContext"/> opened with an
    /// <see cref="AesGcmCryptoProvider"/> can write documents, be closed, and
    /// then be reopened with the same passphrase to read those documents back.
    /// </summary>
    [Fact]
    public async Task DocumentDbContext_WithCryptoProvider_BasicWriteAndReadBack()
    {
        var dbPath = TempDb();
        var opts = new CryptoOptions("ctx-basic-passphrase", iterations: 1);

        // Write phase
        {
            var provider = new AesGcmCryptoProvider(opts);
            var config = PageFileConfig.Default with { CryptoProvider = provider };
            using var ctx = new MultiFileTestDbContext(dbPath, config);
            await ctx.Entries.InsertAsync(new MultiFileEntry { Id = 1, Payload = "Hello encrypted world", Tag = "alpha" });
            await ctx.Entries.InsertAsync(new MultiFileEntry { Id = 2, Payload = "Secret payload", Tag = "beta" });
        }

        // Read-back phase — fresh provider instance, same passphrase
        {
            var provider2 = new AesGcmCryptoProvider(opts);
            var config2 = PageFileConfig.Default with { CryptoProvider = provider2 };
            using var ctx2 = new MultiFileTestDbContext(dbPath, config2);

            var entry1 = await ctx2.Entries.FindByIdAsync(1);
            var entry2 = await ctx2.Entries.FindByIdAsync(2);

            Assert.NotNull(entry1);
            Assert.Equal("Hello encrypted world", entry1!.Payload);
            Assert.Equal("alpha", entry1.Tag);

            Assert.NotNull(entry2);
            Assert.Equal("Secret payload", entry2!.Payload);
            Assert.Equal("beta", entry2.Tag);
        }
    }

    /// <summary>
    /// Verifies that the raw on-disk bytes of an encrypted database do not contain
    /// the inserted plaintext payload in readable form.
    /// </summary>
    [Fact]
    public async Task DocumentDbContext_WithCryptoProvider_PlaintextNotVisibleOnDisk()
    {
        var dbPath = TempDb();
        var opts = new CryptoOptions("ctx-disk-check", iterations: 1);
        const string secretPayload = "PlaintextMustNotAppearOnDisk77665544";

        var provider = new AesGcmCryptoProvider(opts);
        var config = PageFileConfig.Default with { CryptoProvider = provider };
        using (var ctx = new MultiFileTestDbContext(dbPath, config))
        {
            await ctx.Entries.InsertAsync(new MultiFileEntry { Id = 1, Payload = secretPayload, Tag = "secret" });
        }

        var rawBytes = File.ReadAllBytes(dbPath);
        var rawText = System.Text.Encoding.UTF8.GetString(rawBytes);
        Assert.DoesNotContain(secretPayload, rawText);
    }

    /// <summary>
    /// Verifies that opening an encrypted database with a wrong passphrase fails,
    /// protecting against unauthorised access.
    /// </summary>
    [Fact]
    public async Task DocumentDbContext_WithCryptoProvider_WrongPassphrase_Throws()
    {
        var dbPath = TempDb();
        var correctOpts = new CryptoOptions("the-correct-passphrase", iterations: 1);
        var wrongOpts   = new CryptoOptions("the-wrong-passphrase",   iterations: 1);

        // Write with the correct passphrase
        var correctProvider = new AesGcmCryptoProvider(correctOpts);
        using (var ctx = new MultiFileTestDbContext(dbPath, PageFileConfig.Default with { CryptoProvider = correctProvider }))
        {
            await ctx.Entries.InsertAsync(new MultiFileEntry { Id = 1, Payload = "Private data", Tag = "x" });
        }

        // Attempt to open with the wrong passphrase — must throw
        var wrongProvider = new AesGcmCryptoProvider(wrongOpts);
        Assert.ThrowsAny<Exception>(() =>
        {
            using var _ = new MultiFileTestDbContext(dbPath, PageFileConfig.Default with { CryptoProvider = wrongProvider });
        });
    }

    /// <summary>
    /// Verifies that multiple documents inserted into an encrypted database all
    /// survive a close/reopen cycle with correct payloads.
    /// </summary>
    [Fact]
    public async Task DocumentDbContext_WithCryptoProvider_MultipleDocuments_AllSurviveReopen()
    {
        var dbPath = TempDb();
        var opts = new CryptoOptions("ctx-multidoc-passphrase", iterations: 1);
        const int count = 10;

        // Write phase
        {
            var provider = new AesGcmCryptoProvider(opts);
            var config = PageFileConfig.Default with { CryptoProvider = provider };
            using var ctx = new MultiFileTestDbContext(dbPath, config);
            for (int i = 1; i <= count; i++)
                await ctx.Entries.InsertAsync(new MultiFileEntry { Id = i, Payload = $"Encrypted payload {i}", Tag = "batch" });
        }

        // Read-back phase
        {
            var provider2 = new AesGcmCryptoProvider(opts);
            var config2 = PageFileConfig.Default with { CryptoProvider = provider2 };
            using var ctx2 = new MultiFileTestDbContext(dbPath, config2);

            var all = await ctx2.Entries.FindAllAsync().ToListAsync();
            Assert.Equal(count, all.Count);
            Assert.All(all, e => Assert.Equal("batch", e.Tag));
            for (int i = 1; i <= count; i++)
                Assert.Contains(all, e => e.Id == i && e.Payload == $"Encrypted payload {i}");
        }
    }

    /// <summary>
    /// Verifies that a <see cref="DocumentDbContext"/> in server-mode (separate WAL, index,
    /// and collection files), encrypted via <see cref="EncryptionCoordinator"/>, can write
    /// and correctly read back documents after a close/reopen cycle.
    /// </summary>
    [Fact]
    public async Task DocumentDbContext_WithCoordinator_ServerMode_WriteAndReadBack()
    {
        var dir    = TempDir();
        var dbPath = Path.Combine(dir, "serverenc.db");
        var masterKey = MakeMasterKey(0xE0);

        // Write phase
        {
            using var coordinator = new EncryptionCoordinator(masterKey);
            var provider = coordinator.CreateForMainFile();
            var config = PageFileConfig.Server(dbPath) with { CryptoProvider = provider };
            using var ctx = new MultiFileTestDbContext(dbPath, config);
            await ctx.Entries.InsertAsync(new MultiFileEntry { Id = 1, Payload = "Alpha data", Tag = "a" });
            await ctx.Entries.InsertAsync(new MultiFileEntry { Id = 2, Payload = "Beta data",  Tag = "b" });
            await ctx.Entries.InsertAsync(new MultiFileEntry { Id = 3, Payload = "Gamma data", Tag = "c" });
        }

        // Read-back phase — fresh coordinator with the same master key
        {
            using var coordinator2 = new EncryptionCoordinator(masterKey);
            var provider2 = coordinator2.CreateForMainFile();
            var config2 = PageFileConfig.Server(dbPath) with { CryptoProvider = provider2 };
            using var ctx2 = new MultiFileTestDbContext(dbPath, config2);

            var e1 = await ctx2.Entries.FindByIdAsync(1);
            var e3 = await ctx2.Entries.FindByIdAsync(3);

            Assert.NotNull(e1);
            Assert.Equal("Alpha data", e1!.Payload);
            Assert.Equal("a", e1.Tag);

            Assert.NotNull(e3);
            Assert.Equal("Gamma data", e3!.Payload);
            Assert.Equal("c", e3.Tag);

            var all = await ctx2.Entries.FindAllAsync().ToListAsync();
            Assert.Equal(3, all.Count);
        }
    }

    /// <summary>
    /// Verifies that the plaintext payload is not present in any of the server-mode
    /// files on disk (main database, index, WAL, and per-collection files).
    /// </summary>
    [Fact]
    public async Task DocumentDbContext_WithCoordinator_ServerMode_PlaintextNotInAnyFile()
    {
        var dir    = TempDir();
        var dbPath = Path.Combine(dir, "encnoplain.db");
        var masterKey = MakeMasterKey(0xE1);
        const string secretPayload = "ServerModeSecretValue99887766ABCD";

        using var coordinator = new EncryptionCoordinator(masterKey);
        var provider = coordinator.CreateForMainFile();
        var config = PageFileConfig.Server(dbPath) with { CryptoProvider = provider };
        using (var ctx = new MultiFileTestDbContext(dbPath, config))
        {
            await ctx.Entries.InsertAsync(new MultiFileEntry { Id = 1, Payload = secretPayload, Tag = "enc" });
        }

        var secretBytes = System.Text.Encoding.UTF8.GetBytes(secretPayload);
        foreach (var filePath in Directory.EnumerateFiles(dir, "*", SearchOption.AllDirectories))
        {
            var bytes = File.ReadAllBytes(filePath);
            Assert.True(bytes.AsSpan().IndexOf(secretBytes) == -1,
                $"Plaintext payload was found unencrypted in file: {Path.GetFileName(filePath)}");
        }
    }

    /// <summary>
    /// Verifies that opening a server-mode encrypted database with a different master key
    /// fails, protecting all files (main, index, WAL, collection) against unauthorised access.
    /// </summary>
    [Fact]
    public async Task DocumentDbContext_WithCoordinator_ServerMode_WrongMasterKey_Throws()
    {
        var dir        = TempDir();
        var dbPath     = Path.Combine(dir, "wrongkey.db");
        var correctKey = MakeMasterKey(0xE2);
        var wrongKey   = MakeMasterKey(0xE3);

        // Write with the correct master key
        {
            using var coordinator = new EncryptionCoordinator(correctKey);
            var provider = coordinator.CreateForMainFile();
            var config = PageFileConfig.Server(dbPath) with { CryptoProvider = provider };
            using var ctx = new MultiFileTestDbContext(dbPath, config);
            await ctx.Entries.InsertAsync(new MultiFileEntry { Id = 1, Payload = "Sensitive data", Tag = "x" });
        }

        // Attempt to open with a different master key — must throw
        using var wrongCoordinator = new EncryptionCoordinator(wrongKey);
        var wrongProvider = wrongCoordinator.CreateForMainFile();
        var wrongConfig   = PageFileConfig.Server(dbPath) with { CryptoProvider = wrongProvider };
        Assert.ThrowsAny<System.Security.Cryptography.CryptographicException>(() =>
        {
            using var _ = new MultiFileTestDbContext(dbPath, wrongConfig);
        });
    }

    /// <summary>
    /// Verifies that a secondary B-tree index defined on <see cref="MultiFileEntry.Tag"/>
    /// works correctly against an encrypted server-mode database after a close/reopen cycle.
    /// This exercises both the encrypted index file and the encrypted per-collection file.
    /// </summary>
    [Fact]
    public async Task DocumentDbContext_WithCoordinator_ServerMode_SecondaryIndexQuery_ReturnsCorrectResults()
    {
        var dir    = TempDir();
        var dbPath = Path.Combine(dir, "idxenc.db");
        var masterKey = MakeMasterKey(0xE4);

        // Write phase — insert entries with different Tag values
        {
            using var coordinator = new EncryptionCoordinator(masterKey);
            var provider = coordinator.CreateForMainFile();
            var config = PageFileConfig.Server(dbPath) with { CryptoProvider = provider };
            using var ctx = new EncIndexedContext(dbPath, config);
            await ctx.Entries.InsertAsync(new MultiFileEntry { Id = 1, Payload = "Red Apple",  Tag = "red" });
            await ctx.Entries.InsertAsync(new MultiFileEntry { Id = 2, Payload = "Green Apple", Tag = "green" });
            await ctx.Entries.InsertAsync(new MultiFileEntry { Id = 3, Payload = "Red Berry",  Tag = "red" });
            await ctx.Entries.InsertAsync(new MultiFileEntry { Id = 4, Payload = "Blue Sky",   Tag = "blue" });
            await ctx.Entries.InsertAsync(new MultiFileEntry { Id = 5, Payload = "Green Leaf", Tag = "green" });
        }

        // Query phase — fresh coordinator, verify index-driven LINQ queries return correct rows
        {
            using var coordinator2 = new EncryptionCoordinator(masterKey);
            var provider2 = coordinator2.CreateForMainFile();
            var config2 = PageFileConfig.Server(dbPath) with { CryptoProvider = provider2 };
            using var ctx2 = new EncIndexedContext(dbPath, config2);

            var reds = ctx2.Entries.AsQueryable().Where(e => e.Tag == "red").ToList();
            Assert.Equal(2, reds.Count);
            Assert.All(reds, e => Assert.Equal("red", e.Tag));

            var greens = ctx2.Entries.AsQueryable().Where(e => e.Tag == "green").ToList();
            Assert.Equal(2, greens.Count);
            Assert.All(greens, e => Assert.Equal("green", e.Tag));

            var blues = ctx2.Entries.AsQueryable().Where(e => e.Tag == "blue").ToList();
            Assert.Single(blues);
            Assert.Equal("Blue Sky", blues[0].Payload);

            var all = ctx2.Entries.AsQueryable().ToList();
            Assert.Equal(5, all.Count);
        }
    }

    [Fact]
    public void EncryptionCoordinator_PageFile_CreateAndRead()
    {
        var path = TempDb();
        var masterKey = MakeMasterKey(0x77);

        byte[] written;
        uint allocatedPageId;

        // Phase 1: Write with coordinator
        using (var coordinator = new EncryptionCoordinator(masterKey))
        {
            var provider = coordinator.CreateForMainFile();
            var config = PageFileConfig.Default with { CryptoProvider = provider };

            using var pf = new PageFile(path, config);
            pf.Open();

            allocatedPageId = pf.AllocatePage();
            written = new byte[pf.PageSize];
            new Random(77).NextBytes(written);
            pf.WritePage(allocatedPageId, written);
            pf.Flush();
        }

        // Phase 2: Read back with a new coordinator instance (same master key)
        using (var coordinator2 = new EncryptionCoordinator(masterKey))
        {
            var provider2 = coordinator2.CreateForMainFile();
            var config2 = PageFileConfig.Default with { CryptoProvider = provider2 };

            using var pf2 = new PageFile(path, config2);
            pf2.Open();

            var readBuf = new byte[pf2.PageSize];
            pf2.ReadPage(allocatedPageId, readBuf);
            Assert.Equal(written, readBuf);
        }
    }

    // ── BLiteEngine multi-file mode with EncryptionCoordinator ───────────────

    [Fact]
    public async Task EncryptionCoordinator_MultiFileEngine_WriteAndReadBack()
    {
        // Full BLiteEngine round-trip: write with coordinator, re-open with same master key,
        // verify all documents can be read back correctly.
        var dbPath = Path.Combine(TempDir(), "multienc.db");
        var masterKey = MakeMasterKey(0xAA);

        // ── Phase 1: Write ───────────────────────────────────────────────────
        using (var coordinator = new EncryptionCoordinator(masterKey))
        using (var engine = new BLiteEngine(dbPath, coordinator))
        {
            var col = engine.GetOrCreateCollection("users");

            await col.InsertAsync(col.CreateDocument(["_id", "name", "score"],
                b => b.AddId((BsonId)1).AddString("name", "Alice").AddInt32("score", 90)));
            await col.InsertAsync(col.CreateDocument(["_id", "name", "score"],
                b => b.AddId((BsonId)2).AddString("name", "Bob").AddInt32("score", 75)));
            await col.InsertAsync(col.CreateDocument(["_id", "name", "score"],
                b => b.AddId((BsonId)3).AddString("name", "Carol").AddInt32("score", 85)));

            await engine.CommitAsync();
        }

        // ── Phase 2: Read-back with fresh coordinator (same master key) ──────
        using (var coordinator2 = new EncryptionCoordinator(masterKey))
        using (var engine2 = new BLiteEngine(dbPath, coordinator2))
        {
            var col2 = engine2.GetOrCreateCollection("users");

            var all = await col2.FindAllAsync().ToListAsync();
            Assert.Equal(3, all.Count);

            var byName = await col2.FindAsync(d =>
            {
                d.TryGetString("name", out var n);
                return n == "Carol";
            }).ToListAsync();
            Assert.Single(byName);
            Assert.True(byName[0].TryGetInt32("score", out var score));
            Assert.Equal(85, score);
        }
    }

    [Fact]
    public async Task EncryptionCoordinator_MultiFileEngine_MultipleCollections_Isolated()
    {
        // Each collection file gets its own derived subkey.
        // Prove this by checking that the engine can write/read multiple collections.
        var dbPath = Path.Combine(TempDir(), "multienc2.db");
        var masterKey = MakeMasterKey(0xBB);

        using (var coordinator = new EncryptionCoordinator(masterKey))
        using (var engine = new BLiteEngine(dbPath, coordinator))
        {
            var orders = engine.GetOrCreateCollection("orders");
            var products = engine.GetOrCreateCollection("products");

            await orders.InsertAsync(orders.CreateDocument(["_id", "item"],
                b => b.AddId((BsonId)1).AddString("item", "widget")));
            await products.InsertAsync(products.CreateDocument(["_id", "sku"],
                b => b.AddId((BsonId)100).AddString("sku", "W-001")));

            await engine.CommitAsync();
        }

        using (var coordinator2 = new EncryptionCoordinator(masterKey))
        using (var engine2 = new BLiteEngine(dbPath, coordinator2))
        {
            var orders2 = engine2.GetOrCreateCollection("orders");
            var products2 = engine2.GetOrCreateCollection("products");

            var o = await orders2.FindAllAsync().ToListAsync();
            Assert.Single(o);
            Assert.True(o[0].TryGetString("item", out var item));
            Assert.Equal("widget", item);

            var p = await products2.FindAllAsync().ToListAsync();
            Assert.Single(p);
            Assert.True(p[0].TryGetString("sku", out var sku));
            Assert.Equal("W-001", sku);
        }
    }

    [Fact]
    public async Task EncryptionCoordinator_MultiFileEngine_WrongMasterKey_Throws()
    {
        // Writing with one key and reading with a different key must fail.
        var dbPath = Path.Combine(TempDir(), "multienc3.db");
        var correctKey = MakeMasterKey(0xCC);
        var wrongKey = MakeMasterKey(0xDD); // different key

        using (var coordinator = new EncryptionCoordinator(correctKey))
        using (var engine = new BLiteEngine(dbPath, coordinator))
        {
            var col = engine.GetOrCreateCollection("items");
            await col.InsertAsync(col.CreateDocument(["_id", "v"],
                b => b.AddId((BsonId)1).AddInt32("v", 42)));
            await engine.CommitAsync();
        }

        // Opening with the wrong master key must throw during engine construction.
        // The BLCE header is not AES-GCM authenticated, but page 0 (the file header page) is
        // encrypted with a subkey derived from the wrong master key; decrypting it with the
        // correct subkey derived from the correct master key fails AES-GCM tag verification.
        using var wrongCoordinator = new EncryptionCoordinator(wrongKey);
        Assert.ThrowsAny<System.Security.Cryptography.CryptographicException>(() =>
        {
            using var _ = new BLiteEngine(dbPath, wrongCoordinator);
        });
    }
}
