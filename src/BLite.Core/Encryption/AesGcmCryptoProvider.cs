using System;
using System.Buffers.Binary;
using System.Security.Cryptography;

namespace BLite.Core.Encryption;

/// <summary>
/// AES-256-GCM transparent page-level encryption provider.
/// </summary>
/// <remarks>
/// <para><b>On-disk layout per page</b></para>
/// <code>
/// [ ciphertext (LogicalPageSize bytes) ][ GCM authentication tag (16 bytes) ]
/// </code>
/// <para>Total physical page size = LogicalPageSize + 16.</para>
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
///
/// <para><b>Nonce construction (12 bytes)</b></para>
/// <code>
/// [0]     fileRole (1 byte)
/// [1..2]  fileIndex, little-endian (2 bytes)
/// [3..6]  pageId, little-endian (4 bytes)
/// [7..11] databaseSalt[0..4] (5 bytes)
/// </code>
/// </remarks>
public sealed class AesGcmCryptoProvider : ICryptoProvider
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

    // Role of this file in the database (used for nonce construction).
    private readonly byte _fileRole;

    // 0-based index of this file within its role (used for nonce construction).
    private readonly ushort _fileIndex;

    // KDF configuration.
    private readonly string _passphrase;
    private readonly int _iterations;

    // Key material — set by GetFileHeader (new file) or LoadFromFileHeader (existing file).
    // Salt[0..4] is also used in the nonce; we keep a copy of the relevant prefix.
    private byte[]? _key;
    private readonly byte[] _saltPrefix = new byte[5]; // salt[0..4], used in nonce

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
    public int PageOverhead => TagSize;

    /// <inheritdoc/>
    public int FileHeaderSize => HeaderSize;

    /// <inheritdoc/>
    public void Encrypt(uint pageId, ReadOnlySpan<byte> plaintext, Span<byte> ciphertext)
    {
        EnsureKeyDerived();

        // ciphertext layout: [ encrypted bytes (plaintext.Length) ][ tag (TagSize) ]
        var encryptedRegion = ciphertext[..plaintext.Length];
        var tagRegion = ciphertext.Slice(plaintext.Length, TagSize);

        Span<byte> nonce = stackalloc byte[NonceSize];
        BuildNonce(pageId, nonce);

#if NET7_0_OR_GREATER
        using var aes = new AesGcm(_key!, TagSize);
#else
        using var aes = new AesGcm(_key!);
#endif
        aes.Encrypt(nonce, plaintext, encryptedRegion, tagRegion);
    }

    /// <inheritdoc/>
    public void Decrypt(uint pageId, ReadOnlySpan<byte> ciphertext, Span<byte> plaintext)
    {
        EnsureKeyDerived();

        // ciphertext layout: [ encrypted bytes (plaintext.Length) ][ tag (TagSize) ]
        var encryptedRegion = ciphertext[..plaintext.Length];
        var tagRegion = ciphertext.Slice(plaintext.Length, TagSize);

        Span<byte> nonce = stackalloc byte[NonceSize];
        BuildNonce(pageId, nonce);

#if NET7_0_OR_GREATER
        using var aes = new AesGcm(_key!, TagSize);
#else
        using var aes = new AesGcm(_key!);
#endif
        aes.Decrypt(nonce, encryptedRegion, tagRegion, plaintext);
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

        // Derive key and cache salt prefix for nonce construction
        _key = KeyDerivation.DeriveKeyPbkdf2(_passphrase, salt, _iterations);
        salt[..5].CopyTo(_saltPrefix);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Validates the magic/version/algorithm fields, then derives the encryption key
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

        // Read salt (offset 8, 32 bytes)
        var salt = header.Slice(8, 32);

        // Read iterations (offset 40, 4 bytes)
        var iterations = BinaryPrimitives.ReadInt32LittleEndian(header.Slice(40, 4));
        if (iterations < 1)
            throw new InvalidOperationException($"Invalid KDF iteration count {iterations} in file header.");

        // Derive key
        _key = KeyDerivation.DeriveKeyPbkdf2(_passphrase, salt, iterations);
        salt[..5].CopyTo(_saltPrefix);
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    private void EnsureKeyDerived()
    {
        if (_key == null)
            throw new InvalidOperationException(
                "Encryption key has not been derived. Call GetFileHeader (new file) or " +
                "LoadFromFileHeader (existing file) before performing page I/O.");
    }

    /// <summary>
    /// Builds the 12-byte AES-GCM nonce for the given page.
    /// Layout: fileRole (1) | fileIndex LE (2) | pageId LE (4) | saltPrefix (5)
    /// </summary>
    private void BuildNonce(uint pageId, Span<byte> nonce)
    {
        nonce[0] = _fileRole;
        BinaryPrimitives.WriteUInt16LittleEndian(nonce.Slice(1, 2), _fileIndex);
        BinaryPrimitives.WriteUInt32LittleEndian(nonce.Slice(3, 4), pageId);
        _saltPrefix.CopyTo(nonce.Slice(7, 5));
    }
}
