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
}
