using System;
using System.Buffers.Binary;
using System.Security.Cryptography;

namespace BLite.Core.Encryption;

/// <summary>
/// AES-256-GCM transparent page-level encryption provider.
/// </summary>
/// <remarks>
/// <para><b>On-disk layout per physical page</b></para>
/// <code>
/// [ nonce (12 bytes) ][ ciphertext (LogicalPageSize bytes) ][ GCM authentication tag (16 bytes) ]
/// </code>
/// <para>
/// A fresh random nonce is generated for every write.  This prevents the nonce-reuse
/// vulnerability that would arise from deterministic nonce schemes when a page is
/// overwritten (same key + same deterministic nonce = broken AES-GCM confidentiality).
/// </para>
/// <para>Total physical page size = LogicalPageSize + <see cref="PageOverhead"/> (28 bytes).</para>
///
/// <para><b>64-byte file header layout</b></para>
/// <code>
/// Offset  Size  Field
///   0       4   Magic: 0x424C4345 ("BLCE")
///   4       1   Version: 1
///   5       1   Algorithm: 1 = AES-256-GCM
///   6       1   KDF: 1 = PBKDF2-SHA256
///   7       1   FileRole: 0=main, 1=collection, 2=index, 3=WAL
///   8      32   Database salt (random, generated at creation)
///  40       4   KDF iterations
///  44       2   FileIndex (0-based)
///  46      18   Reserved (zeroed)
/// </code>
/// </remarks>
public sealed class AesGcmCryptoProvider : ICryptoProvider, IDisposable
{
    /// <summary>GCM authentication tag size in bytes.</summary>
    public const int TagSize = 16;

    /// <summary>AES key size for AES-256 in bytes.</summary>
    private const int KeySize = 32;

    /// <summary>GCM nonce (IV) size in bytes.</summary>
    private const int NonceSize = 12;

    /// <summary>Size of the per-file crypto header in bytes.</summary>
    public const int HeaderSize = 64;

    private static readonly uint Magic = 0x424C4345u; // "BLCE"
    private const byte CurrentVersion = 1;
    private const byte AlgorithmAesGcm = 1;
    private const byte KdfPbkdf2 = 1;

    // Role of this file in the database (stored in file header).
    private readonly byte _fileRole;

    // 0-based index of this file within its role (stored in file header).
    private readonly ushort _fileIndex;

    // KDF configuration.
    private readonly string _passphrase;
    private readonly int _iterations;

    // Key material — set by GetFileHeader (new file) or LoadFromFileHeader (existing file).
    private byte[]? _key;

    // Cached AesGcm instance created once after key derivation.
    // AesGcm is thread-safe for concurrent Encrypt/Decrypt calls once constructed.
    private AesGcm? _aesGcm;

    /// <summary>
    /// Creates a new <see cref="AesGcmCryptoProvider"/> for a given file.
    /// Key material is derived later when <see cref="GetFileHeader"/> (new file) or
    /// <see cref="LoadFromFileHeader"/> (existing file) is called.
    /// </summary>
    /// <param name="options">Passphrase and KDF settings.</param>
    /// <param name="fileRole">
    /// Role of the file: 0 = main, 1 = collection, 2 = index, 3 = WAL.
    /// </param>
    /// <param name="fileIndex">0-based index of this file within its role.</param>
    public AesGcmCryptoProvider(CryptoOptions options, byte fileRole = 0, ushort fileIndex = 0)
    {
        if (options == null) throw new ArgumentNullException(nameof(options));
        _passphrase = options.Passphrase;
        _iterations = options.Iterations;
        _fileRole = fileRole;
        _fileIndex = fileIndex;
    }

    /// <inheritdoc/>
    /// <remarks>
    /// 12 bytes (nonce) + 16 bytes (GCM tag) = 28 bytes per physical page.
    /// </remarks>
    public int PageOverhead => NonceSize + TagSize;

    /// <inheritdoc/>
    public int FileHeaderSize => HeaderSize;

    /// <inheritdoc/>
    public void Encrypt(uint pageId, ReadOnlySpan<byte> plaintext, Span<byte> ciphertext)
    {
        var aes = GetAesGcm(); // throws InvalidOperationException if key not yet derived

        // Physical layout: [ nonce (12) | ciphertext (plaintext.Length) | tag (16) ]
        var nonceRegion     = ciphertext[..NonceSize];
        var encryptedRegion = ciphertext.Slice(NonceSize, plaintext.Length);
        var tagRegion       = ciphertext.Slice(NonceSize + plaintext.Length, TagSize);

        // Generate a fresh random nonce for every write.  Using a deterministic nonce
        // (e.g. derived from pageId) would allow nonce reuse when a page is overwritten
        // with the same key, breaking AES-GCM confidentiality and integrity.
        RandomNumberGenerator.Fill(nonceRegion);

        aes.Encrypt(nonceRegion, plaintext, encryptedRegion, tagRegion);
    }

    /// <inheritdoc/>
    public void Decrypt(uint pageId, ReadOnlySpan<byte> ciphertext, Span<byte> plaintext)
    {
        var aes = GetAesGcm(); // throws InvalidOperationException if key not yet derived

        // Physical layout: [ nonce (12) | ciphertext (plaintext.Length) | tag (16) ]
        var nonceRegion     = ciphertext[..NonceSize];
        var encryptedRegion = ciphertext.Slice(NonceSize, plaintext.Length);
        var tagRegion       = ciphertext.Slice(NonceSize + plaintext.Length, TagSize);

        aes.Decrypt(nonceRegion, encryptedRegion, tagRegion, plaintext);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Generates a fresh random 32-byte salt, derives the AES-256 key, and serialises
    /// the 64-byte file header into <paramref name="header"/>.
    /// </remarks>
    public void GetFileHeader(Span<byte> header)
    {
        if (header.Length != HeaderSize)
            throw new ArgumentException($"Header must be exactly {HeaderSize} bytes.", nameof(header));

        header.Clear();

        // Magic
        BinaryPrimitives.WriteUInt32LittleEndian(header, Magic);
        // Version
        header[4] = CurrentVersion;
        // Algorithm
        header[5] = AlgorithmAesGcm;
        // KDF
        header[6] = KdfPbkdf2;
        // FileRole
        header[7] = _fileRole;

        // Generate random 32-byte salt at offset 8
        var salt = header.Slice(8, 32);
        RandomNumberGenerator.Fill(salt);

        // KDF iterations at offset 40
        BinaryPrimitives.WriteInt32LittleEndian(header.Slice(40, 4), _iterations);

        // FileIndex at offset 44
        BinaryPrimitives.WriteUInt16LittleEndian(header.Slice(44, 2), _fileIndex);

        // Derive key and create the cached AesGcm instance.
        _key = KeyDerivation.DeriveKeyPbkdf2(_passphrase, salt, _iterations);
        CreateAesGcm();
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Validates the magic/version/algorithm/KDF fields, then derives the encryption key
    /// from the passphrase and the salt stored in the header.
    /// </remarks>
    public void LoadFromFileHeader(ReadOnlySpan<byte> header)
    {
        if (header.Length != HeaderSize)
            throw new ArgumentException($"Header must be exactly {HeaderSize} bytes.", nameof(header));

        // Validate magic
        var magic = BinaryPrimitives.ReadUInt32LittleEndian(header);
        if (magic != Magic)
            throw new InvalidOperationException(
                $"Encrypted file header magic mismatch. Expected 0x{Magic:X8}, got 0x{magic:X8}. " +
                "The file may not be an encrypted BLite database.");

        // Validate version
        var version = header[4];
        if (version != CurrentVersion)
            throw new InvalidOperationException(
                $"Unsupported encrypted file header version {version}. Only version {CurrentVersion} is supported.");

        // Validate algorithm
        var algorithm = header[5];
        if (algorithm != AlgorithmAesGcm)
            throw new InvalidOperationException(
                $"Unsupported encryption algorithm {algorithm}. Only AES-256-GCM (1) is supported.");

        // Validate KDF
        var kdf = header[6];
        if (kdf != KdfPbkdf2)
            throw new InvalidOperationException(
                $"Unsupported key derivation function {kdf}. Only PBKDF2-SHA256 (1) is supported.");

        // Read salt (offset 8, 32 bytes)
        var salt = header.Slice(8, 32);

        // Read iterations (offset 40, 4 bytes)
        var iterations = BinaryPrimitives.ReadInt32LittleEndian(header.Slice(40, 4));
        if (iterations < 1)
            throw new InvalidOperationException($"Invalid KDF iteration count {iterations} in file header.");

        // Derive key and create the cached AesGcm instance.
        _key = KeyDerivation.DeriveKeyPbkdf2(_passphrase, salt, iterations);
        CreateAesGcm();
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        _aesGcm?.Dispose();
        _aesGcm = null;
        // Zero out key material to reduce window of sensitive data in memory.
        if (_key != null)
        {
            Array.Clear(_key, 0, _key.Length);
            _key = null;
        }
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    /// <summary>
    /// Creates and caches the <see cref="AesGcm"/> instance after key derivation.
    /// Called once from <see cref="GetFileHeader"/> or <see cref="LoadFromFileHeader"/>.
    /// </summary>
    private void CreateAesGcm()
    {
        _aesGcm?.Dispose();
        // The single-parameter AesGcm constructor is deprecated in NET7+ in favour of the
        // overload that accepts an explicit tag size, which validates the tag length at
        // construction time and is more explicit about the expected tag size.
#if NET7_0_OR_GREATER
        _aesGcm = new AesGcm(_key!, TagSize);
#else
        _aesGcm = new AesGcm(_key!);
#endif
    }

    private AesGcm GetAesGcm()
    {
        if (_aesGcm == null)
            throw new InvalidOperationException(
                "Encryption key has not been derived. Call GetFileHeader (new file) or " +
                "LoadFromFileHeader (existing file) before performing page I/O.");
        return _aesGcm;
    }
}
