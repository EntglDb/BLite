using System;
using System.Security.Cryptography;
using System.Text;

namespace BLite.Core.Encryption;

/// <summary>
/// Key-derivation function used to turn a user-supplied passphrase into the AES-256 encryption key
/// in single-file mode.
/// </summary>
/// <remarks>
/// <para>
/// This enum only enumerates KDFs that are <b>user-selectable</b> via
/// <see cref="CryptoOptions(string, KdfAlgorithm, int)"/> /
/// <see cref="CryptoOptions(System.ReadOnlySpan{byte}, KdfAlgorithm, int)"/>. The HKDF-SHA256
/// derivation used internally by <see cref="EncryptionCoordinator"/> for multi-file (server)
/// mode is engaged automatically when <see cref="CryptoOptions.FromMasterKey(System.ReadOnlySpan{byte})"/>
/// is used, and is therefore not configured through this enum.
/// </para>
/// <para>
/// On-disk the chosen value is persisted as the <c>KDF</c> byte in the BLCE file header
/// (currently 1 = PBKDF2-SHA256, 2 = HKDF-SHA256 reserved for coordinator-managed files).
/// Future BLite versions may add additional values (e.g. Argon2id) without breaking the
/// public ABI, hence the explicit numeric assignments.
/// </para>
/// </remarks>
public enum KdfAlgorithm : byte
{
    /// <summary>PBKDF2 with HMAC-SHA-256 (default, 600 000 iterations — OWASP 2023).</summary>
    Pbkdf2Sha256 = 1

    // Reserved values:
    //   2 = HKDF-SHA256 (coordinator-managed files; not user-selectable here).
    //   3+ = future KDFs (e.g. Argon2id) — kept open for forward compatibility.
}

/// <summary>
/// Configuration for the transparent encryption layer.
/// Pass an instance of this class to the <see cref="BLite.Core.BLiteEngine"/> constructor
/// to enable AES-256-GCM encryption at rest.
/// </summary>
/// <remarks>
/// Three credential entry points are supported:
/// <list type="bullet">
/// <item><description>
/// <c>CryptoOptions(string)</c> — passphrase mode (PBKDF2-SHA256). Convenient but the
/// string remains on the GC heap until collected and cannot be securely zeroed (string
/// interning, immutability). Suitable for human-typed secrets.
/// </description></item>
/// <item><description>
/// <c>CryptoOptions(ReadOnlySpan&lt;byte&gt;)</c> — passphrase mode with raw bytes.
/// The bytes are copied into an internal buffer that is zeroed by <see cref="ClearSecret"/>.
/// </description></item>
/// <item><description>
/// <c>CryptoOptions.FromMasterKey(ReadOnlySpan&lt;byte&gt;)</c> — master-key mode
/// (HKDF-SHA256). Suitable for KMS-derived 256-bit keys; required for multi-file (server)
/// mode where a single master key fans out to per-file subkeys.
/// </description></item>
/// </list>
/// <para>
/// In multi-file mode the engine internally constructs an <c>EncryptionCoordinator</c> that
/// derives a unique 256-bit subkey per physical file (main, WAL, collections, indexes) using
/// HKDF. This eliminates the AES-GCM nonce-reuse risk that would arise from sharing one key
/// across files with overlapping page IDs. The coordinator is an internal implementation
/// detail; the only thing the consumer supplies is the master key via
/// <see cref="FromMasterKey(ReadOnlySpan{byte})"/>.
/// </para>
/// </remarks>
public sealed class CryptoOptions
{
    private byte[]? _passphraseBytes;
    private byte[]? _masterKey;
    private readonly bool _ownsBytes;

    /// <summary>
    /// Creates a new <see cref="CryptoOptions"/> with the supplied passphrase.
    /// </summary>
    /// <param name="passphrase">
    /// The user-supplied secret.  Must not be null or empty.
    /// The actual encryption key is derived from this value using PBKDF2-SHA256.
    /// </param>
    /// <param name="kdf">Key-derivation function (default: <see cref="KdfAlgorithm.Pbkdf2Sha256"/>).</param>
    /// <param name="iterations">
    /// Number of KDF iterations (default: 600 000, aligned with OWASP 2023 guidance for PBKDF2-SHA256).
    /// Higher values increase resistance to brute-force attacks at the cost of open time.
    /// The iteration count is persisted in the file header, so existing databases continue to open
    /// with whatever value they were created with.
    /// </param>
    public CryptoOptions(string passphrase, KdfAlgorithm kdf = KdfAlgorithm.Pbkdf2Sha256, int iterations = 600_000)
    {
        if (string.IsNullOrEmpty(passphrase))
            throw new ArgumentNullException(nameof(passphrase));
        if (iterations < 1)
            throw new ArgumentOutOfRangeException(nameof(iterations), "Iterations must be at least 1.");

        Passphrase = passphrase;
        _passphraseBytes = Encoding.UTF8.GetBytes(passphrase);
        _ownsBytes = true;
        Kdf = kdf;
        Iterations = iterations;
    }

    /// <summary>
    /// Creates a new <see cref="CryptoOptions"/> from raw passphrase bytes.
    /// The bytes are copied into an internal buffer; the caller may immediately zero the
    /// source span.  Call <see cref="ClearSecret"/> (or dispose the consuming provider) to
    /// zero the internal copy when the secret is no longer needed.
    /// </summary>
    /// <param name="passphraseBytes">
    /// The user-supplied secret as raw bytes (typically UTF-8).  Must not be empty.
    /// </param>
    /// <param name="kdf">Key-derivation function (default: <see cref="KdfAlgorithm.Pbkdf2Sha256"/>).</param>
    /// <param name="iterations">Number of KDF iterations (default: 600 000).</param>
    public CryptoOptions(ReadOnlySpan<byte> passphraseBytes, KdfAlgorithm kdf = KdfAlgorithm.Pbkdf2Sha256, int iterations = 600_000)
    {
        if (passphraseBytes.IsEmpty)
            throw new ArgumentException("Passphrase bytes must not be empty.", nameof(passphraseBytes));
        if (iterations < 1)
            throw new ArgumentOutOfRangeException(nameof(iterations), "Iterations must be at least 1.");

        Passphrase = string.Empty; // not exposed when constructed from bytes
        _passphraseBytes = passphraseBytes.ToArray();
        _ownsBytes = true;
        Kdf = kdf;
        Iterations = iterations;
    }

    /// <summary>
    /// Creates a <see cref="CryptoOptions"/> in master-key mode (HKDF-SHA256).
    /// </summary>
    /// <param name="masterKey">
    /// 32-byte master key (e.g. obtained from a KMS, secure enclave, or KEK-unwrapped
    /// data key). The bytes are copied into an internal buffer; the caller may zero the
    /// source span immediately. Call <see cref="ClearSecret"/> (or dispose the consuming
    /// engine) to zero the internal copy when the secret is no longer needed.
    /// </param>
    /// <returns>A <see cref="CryptoOptions"/> instance configured for master-key mode.</returns>
    /// <remarks>
    /// In multi-file (server) mode the engine internally derives a unique 256-bit subkey
    /// per physical file using HKDF-SHA256, eliminating the nonce-reuse risk inherent in
    /// sharing one AES-GCM key across files with overlapping page IDs.
    /// </remarks>
    public static CryptoOptions FromMasterKey(ReadOnlySpan<byte> masterKey)
    {
        if (masterKey.Length != 32)
            throw new ArgumentException("Master key must be exactly 32 bytes.", nameof(masterKey));
        return new CryptoOptions(masterKey.ToArray(), masterKeyMode: true);
    }

    private CryptoOptions(byte[] masterKey, bool masterKeyMode)
    {
        // masterKeyMode is a marker parameter to disambiguate from the public ctors;
        // the array is taken by reference because FromMasterKey already cloned it.
        _ = masterKeyMode;
        Passphrase = string.Empty;
        _masterKey = masterKey;
        _ownsBytes = true;
        Kdf = KdfAlgorithm.Pbkdf2Sha256; // unused in master-key mode
        Iterations = 0;                  // unused in master-key mode
    }

    /// <summary>
    /// The user-supplied passphrase as string, when constructed via the string overload.
    /// Returns <see cref="string.Empty"/> when constructed via the byte-span overload
    /// or via <see cref="FromMasterKey(ReadOnlySpan{byte})"/>.
    /// </summary>
    public string Passphrase { get; }

    /// <summary>Key-derivation function.</summary>
    public KdfAlgorithm Kdf { get; }

    /// <summary>Number of KDF iterations.</summary>
    public int Iterations { get; }

    /// <summary>
    /// True when this instance was created via <see cref="FromMasterKey(ReadOnlySpan{byte})"/>
    /// (HKDF mode); false when constructed from a passphrase (PBKDF2 mode).
    /// </summary>
    internal bool IsMasterKeyMode => _masterKey is not null;

    /// <summary>
    /// Returns a fresh copy of the master key bytes. Caller owns the copy and must zero it.
    /// </summary>
    internal byte[] CopyMasterKey()
    {
        var src = _masterKey ?? throw new InvalidOperationException(
            "Master key has already been cleared, or this CryptoOptions was not created via FromMasterKey.");
        var copy = new byte[src.Length];
        Buffer.BlockCopy(src, 0, copy, 0, src.Length);
        return copy;
    }

    /// <summary>
    /// Returns a fresh copy of the passphrase bytes.  The caller is responsible for zeroing
    /// the returned array (e.g. via <see cref="CryptographicOperations.ZeroMemory"/>) once
    /// the key has been derived.  Throws if <see cref="ClearSecret"/> has already been called.
    /// </summary>
    internal byte[] CopyPassphraseBytes()
    {
        var src = _passphraseBytes ?? throw new InvalidOperationException(
            "Passphrase bytes have already been cleared. Reuse of a CryptoOptions instance after " +
            "ClearSecret() (or after the consuming provider has been disposed) is not supported.");
        var copy = new byte[src.Length];
        Buffer.BlockCopy(src, 0, copy, 0, src.Length);
        return copy;
    }

    /// <summary>
    /// Zeroes any internal secret buffer (passphrase bytes or master key). Idempotent.
    /// After this call the instance can no longer be used to construct an encryption provider.
    /// </summary>
    public void ClearSecret()
    {
        if (!_ownsBytes) return;
        if (_passphraseBytes is not null)
        {
            CryptographicOperations.ZeroMemory(_passphraseBytes);
            _passphraseBytes = null;
        }
        if (_masterKey is not null)
        {
            CryptographicOperations.ZeroMemory(_masterKey);
            _masterKey = null;
        }
    }
}
