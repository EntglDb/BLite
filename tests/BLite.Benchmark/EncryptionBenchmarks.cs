using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BLite.Core.Encryption;

namespace BLite.Benchmark;

/// <summary>
/// Micro-benchmarks for the page-level encryption layer added in BLite v4.5.
/// Measures:
/// <list type="bullet">
///   <item>Per-page AES-256-GCM Encrypt / Decrypt overhead (8 KB pages).</item>
///   <item><see cref="NullCryptoProvider"/> baseline (no-op cost).</item>
///   <item>Open-time PBKDF2-SHA256 key derivation cost (single-file mode, 600 000 iterations).</item>
/// </list>
/// </summary>
[InProcess]
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[HtmlExporter]
[JsonExporterAttribute.Full]
public class EncryptionBenchmarks
{
    // BLite default page size is 8 KB.
    private const int PageSize = 8192;
    private const string TestPassphrase = "correct horse battery staple";

    private byte[] _plaintext      = [];
    private byte[] _ciphertext     = [];   // PageSize + PageOverhead (28 B)
    private byte[] _decryptBuffer  = [];

    private AesGcmCryptoProvider? _aesProvider;
    private NullCryptoProvider?   _nullProvider;
    private byte[]                _aesFileHeader = [];

    [GlobalSetup]
    public void Setup()
    {
        _plaintext = new byte[PageSize];
        new Random(42).NextBytes(_plaintext);

        _nullProvider = new NullCryptoProvider();

        // Create one provider, run header exchange (which triggers PBKDF2),
        // and reuse it for all Encrypt/Decrypt iterations.
        var options = new CryptoOptions(TestPassphrase);
        _aesProvider = new AesGcmCryptoProvider(options);

        _aesFileHeader = new byte[_aesProvider.FileHeaderSize];
        _aesProvider.GetFileHeader(_aesFileHeader); // primes the AES key

        _ciphertext     = new byte[PageSize + _aesProvider.PageOverhead];
        _decryptBuffer  = new byte[PageSize];

        // Pre-encrypt one page so DecryptPage has a valid input on every iteration.
        _aesProvider.Encrypt(pageId: 1, _plaintext, _ciphertext);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _aesProvider?.Dispose();
    }

    [Benchmark(Description = "AES-256-GCM Encrypt 8KB page")]
    [BenchmarkCategory("Encrypt")]
    public void EncryptPage_AesGcm()
    {
        _aesProvider!.Encrypt(pageId: 1, _plaintext, _ciphertext);
    }

    [Benchmark(Baseline = true, Description = "Null Encrypt 8KB page (baseline)")]
    [BenchmarkCategory("Encrypt")]
    public void EncryptPage_Null()
    {
        // Null provider has zero PageOverhead — destination is the same size as plaintext.
        _nullProvider!.Encrypt(pageId: 1, _plaintext, _plaintext);
    }

    [Benchmark(Description = "AES-256-GCM Decrypt 8KB page")]
    [BenchmarkCategory("Decrypt")]
    public void DecryptPage_AesGcm()
    {
        _aesProvider!.Decrypt(pageId: 1, _ciphertext, _decryptBuffer);
    }

    [Benchmark(Baseline = true, Description = "Null Decrypt 8KB page (baseline)")]
    [BenchmarkCategory("Decrypt")]
    public void DecryptPage_Null()
    {
        _nullProvider!.Decrypt(pageId: 1, _plaintext, _decryptBuffer);
    }

    /// <summary>
    /// Measures the full open-time cost of constructing an encrypted provider for a NEW
    /// database (passphrase + GetFileHeader → PBKDF2 600k iterations + AES-GCM init).
    /// This is paid once per database open in single-file mode.
    /// </summary>
    [Benchmark(Description = "Open new DB — PBKDF2 600k + AES-GCM init")]
    [BenchmarkCategory("Open")]
    public void OpenNewDatabase_Pbkdf2()
    {
        var options = new CryptoOptions(TestPassphrase);
        using var provider = new AesGcmCryptoProvider(options);
        var header = new byte[provider.FileHeaderSize];
        provider.GetFileHeader(header);
    }

    /// <summary>
    /// Measures the open-time cost of opening an EXISTING encrypted database — i.e. parsing
    /// a pre-existing 64-byte BLCE header and re-deriving the AES key via PBKDF2 600k.
    /// </summary>
    [Benchmark(Description = "Open existing DB — PBKDF2 600k + AES-GCM init")]
    [BenchmarkCategory("Open")]
    public void OpenExistingDatabase_Pbkdf2()
    {
        var options = new CryptoOptions(TestPassphrase);
        using var provider = new AesGcmCryptoProvider(options);
        provider.LoadFromFileHeader(_aesFileHeader);
    }
}
