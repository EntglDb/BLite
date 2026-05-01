using System;
using System.Buffers.Binary;
using System.Security.Cryptography;

namespace BLite.Core.Encryption;

/// <summary>
/// AES-256-GCM <see cref="ICryptoProvider"/> whose encryption key is derived by
/// an owning <see cref="EncryptionCoordinator"/> via HKDF-SHA256.
/// </summary>
/// <remarks>
/// <para>
/// <b>Internal type.</b> This class is part of the encryption plumbing for multi-file
/// (server) mode and is never constructed by end users. The supported public entry point
/// is <see cref="CryptoOptions.FromMasterKey(System.ReadOnlySpan{byte})"/>; the engine
/// then constructs the coordinator and the appropriate <see cref="CoordinatedFileProvider"/>
/// instances internally.
/// </para>
/// <para>
/// For the <b>main file</b> (role 0) the sub-key cannot be derived until the database
/// salt is known, so key initialisation is deferred to <see cref="GetFileHeader"/>
/// (new file) or <see cref="LoadFromFileHeader"/> (existing file).  Both methods extract
/// the salt, store it in the coordinator, and then derive the sub-key.
/// </para>
/// <para>
/// For <b>secondary files</b> (collections, indexes, WAL) the salt is already known at
/// construction time, so the sub-key is derived immediately.
/// </para>
/// <para>
/// On-disk BLCE header layout (64 bytes — identical structure to
/// <see cref="AesGcmCryptoProvider"/> but with KDF byte = 2 to signal HKDF):
/// </para>
/// <code>
/// Offset  Size  Field
///   0       4   Magic: 0x424C4345 ("BLCE")
///   4       1   Version: 1
///   5       1   Algorithm: 1 = AES-256-GCM
///   6       1   KDF: 2 = HKDF-SHA256 (coordinator mode)
///   7       1   FileRole: 0=main, 1=collection, 2=index, 3=WAL
///   8      32   Database salt (meaningful for main file; zeroed for secondary files)
///  40       4   Reserved (zeroed; KDF iterations not applicable for HKDF)
///  44       2   FileIndex (0-based)
///  46      18   Reserved (zeroed)
/// </code>
/// </remarks>
internal sealed class CoordinatedFileProvider : ICryptoProvider, IDisposable
{
    // Header / cryptographic constants are owned by EncryptionCoordinator and shared with
    // this file provider as `internal` constants — single source of truth.
    private const int HeaderSize = EncryptionCoordinator.HeaderSize;
    private const int TagSize    = EncryptionCoordinator.TagSize;
    private const int NonceSize  = EncryptionCoordinator.NonceSize;
    private const int SaltOffset = EncryptionCoordinator.SaltOffset;
    private const int SaltSize   = EncryptionCoordinator.SaltSize;
    private const byte HeaderVersion   = EncryptionCoordinator.HeaderVersion;
    private const byte AlgorithmAesGcm = EncryptionCoordinator.AlgorithmAesGcm;
    private const byte KdfHkdf         = EncryptionCoordinator.KdfHkdf;

    private readonly EncryptionCoordinator _coordinator;
    private readonly byte _fileRole;
    private readonly ushort _fileIndex;
    // Volatile so the reference set in the constructor (secondary files) or in
    // GetFileHeader/LoadFromFileHeader (main file) is published safely to threads
    // performing Encrypt/Decrypt under PageFile's read-lock.
    private volatile AesGcm? _aesGcm;

    internal CoordinatedFileProvider(
        EncryptionCoordinator coordinator, byte fileRole, ushort fileIndex)
    {
        _coordinator = coordinator;
        _fileRole    = fileRole;
        _fileIndex   = fileIndex;

        // Secondary files: salt is already available — derive and cache the sub-key now.
        if (fileRole != 0)
        {
            var subKey = coordinator.DeriveSubKey(fileRole, fileIndex);
            InitialiseAesGcm(subKey);
        }
        // Main file (role 0): key derivation deferred until GetFileHeader / LoadFromFileHeader.
    }

    // ── ICryptoProvider ───────────────────────────────────────────────────────

    /// <inheritdoc/>
    public int PageOverhead => NonceSize + TagSize; // 28 bytes

    /// <inheritdoc/>
    public int FileHeaderSize => HeaderSize; // 64 bytes

    /// <inheritdoc/>
    public void Encrypt(uint pageId, ReadOnlySpan<byte> plaintext, Span<byte> ciphertext)
    {
        var aes = GetAesGcm();

        var nonceRegion     = ciphertext[..NonceSize];
        var encryptedRegion = ciphertext.Slice(NonceSize, plaintext.Length);
        var tagRegion       = ciphertext.Slice(NonceSize + plaintext.Length, TagSize);

        // Fresh random nonce per write to prevent nonce reuse.
        RandomNumberGenerator.Fill(nonceRegion);
        aes.Encrypt(nonceRegion, plaintext, encryptedRegion, tagRegion);
    }

    /// <inheritdoc/>
    public void Decrypt(uint pageId, ReadOnlySpan<byte> ciphertext, Span<byte> plaintext)
    {
        var aes = GetAesGcm();

        var nonceRegion     = ciphertext[..NonceSize];
        var encryptedRegion = ciphertext.Slice(NonceSize, plaintext.Length);
        var tagRegion       = ciphertext.Slice(NonceSize + plaintext.Length, TagSize);

        aes.Decrypt(nonceRegion, encryptedRegion, tagRegion, plaintext);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Generates a random 32-byte database salt (main file only), writes the BLCE
    /// header, notifies the coordinator of the salt, and initialises the AES-GCM key.
    /// </remarks>
    public void GetFileHeader(Span<byte> header)
    {
        if (header.Length != HeaderSize)
            throw new ArgumentException(
                $"Header must be exactly {HeaderSize} bytes.", nameof(header));

        header.Clear();

        BinaryPrimitives.WriteUInt32LittleEndian(header, EncryptionCoordinator.HeaderMagic);
        header[4] = HeaderVersion;
        header[5] = AlgorithmAesGcm;
        header[6] = KdfHkdf;
        header[7] = _fileRole;

        if (_fileRole == 0)
        {
            // Generate a fresh random database salt for a new database.
            var saltSlice = header.Slice(SaltOffset, SaltSize);
            RandomNumberGenerator.Fill(saltSlice);
            _coordinator.SetDatabaseSalt(saltSlice);
        }
        // Secondary files write zeros in the salt region (key is derived from main salt).

        BinaryPrimitives.WriteUInt16LittleEndian(header.Slice(44, 2), _fileIndex);

        // Derive and initialise the sub-key (for main file the salt is now available).
        var subKey = _coordinator.DeriveSubKey(_fileRole, _fileIndex);
        InitialiseAesGcm(subKey);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Validates the BLCE header fields, extracts the database salt (main file only),
    /// and initialises the AES-GCM key.  Rejects files created with PBKDF2 (KDF=1)
    /// to prevent accidental cross-mode key mismatches.
    /// </remarks>
    public void LoadFromFileHeader(ReadOnlySpan<byte> header)
    {
        if (header.Length != HeaderSize)
            throw new ArgumentException(
                $"Header must be exactly {HeaderSize} bytes.", nameof(header));

        var magic = BinaryPrimitives.ReadUInt32LittleEndian(header);
        if (magic != EncryptionCoordinator.HeaderMagic)
            throw new InvalidOperationException(
                $"Encrypted file header magic mismatch. " +
                $"Expected 0x{EncryptionCoordinator.HeaderMagic:X8}, got 0x{magic:X8}. " +
                "The file may not be an encrypted BLite database.");

        var version = header[4];
        if (version != HeaderVersion)
            throw new InvalidOperationException(
                $"Unsupported encrypted file header version {version}. " +
                $"Only version {HeaderVersion} is supported.");

        var algorithm = header[5];
        if (algorithm != AlgorithmAesGcm)
            throw new InvalidOperationException(
                $"Unsupported encryption algorithm {algorithm}. " +
                "Only AES-256-GCM (1) is supported.");

        var kdf = header[6];
        if (kdf != KdfHkdf)
            throw new InvalidOperationException(
                $"File KDF mismatch (got {kdf}, expected {KdfHkdf}). " +
                "This file was not created with an EncryptionCoordinator. " +
                "Use AesGcmCryptoProvider directly to open PBKDF2-encrypted files.");

        var storedRole = header[7];
        if (storedRole != _fileRole)
            throw new InvalidOperationException(
                $"File role mismatch: provider expected role {_fileRole}, " +
                $"but the file header contains role {storedRole}.");

        var storedIndex = BinaryPrimitives.ReadUInt16LittleEndian(header.Slice(44, 2));
        if (storedIndex != _fileIndex)
            throw new InvalidOperationException(
                $"File index mismatch: provider expected index {_fileIndex}, " +
                $"but the file header contains index {storedIndex}.");

        if (_fileRole == 0)
        {
            // Extract the 32-byte database salt and notify the coordinator.
            _coordinator.SetDatabaseSalt(header.Slice(SaltOffset, SaltSize));
        }

        // Derive and initialise the sub-key (main file: salt is now set; others: already set).
        var subKey = _coordinator.DeriveSubKey(_fileRole, _fileIndex);
        InitialiseAesGcm(subKey);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Delegates to the owning <see cref="EncryptionCoordinator"/> so that the sibling
    /// file receives a unique HKDF-SHA256-derived sub-key.
    /// The main-file provider must have been opened (salt primed) before calling
    /// this for any secondary role (collection, index, WAL).
    /// </remarks>
    public ICryptoProvider CreateSiblingProvider(byte fileRole, ushort fileIndex)
    {
        return fileRole switch
        {
            0 => _coordinator.CreateForMainFile(),
            1 => _coordinator.CreateForCollection(fileIndex),
            2 => _coordinator.CreateForIndex(fileIndex),
            3 => _coordinator.CreateForWal(),
            _ => throw new ArgumentOutOfRangeException(nameof(fileRole),
                     $"Unknown file role: {fileRole}. Valid roles are 0 (main), 1 (collection), 2 (index), 3 (WAL).")
        };
    }

    // ── IDisposable ───────────────────────────────────────────────────────────

    public void Dispose()
    {
        _aesGcm?.Dispose();
        _aesGcm = null;
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private void InitialiseAesGcm(byte[] key)
    {
        _aesGcm?.Dispose();
#if NET7_0_OR_GREATER
        _aesGcm = new AesGcm(key, TagSize);
#else
        _aesGcm = new AesGcm(key);
#endif
        // Zero the derived key array immediately after handing it to AesGcm,
        // which makes its own internal copy.
        CryptographicOperations.ZeroMemory(key);
    }

    private AesGcm GetAesGcm()
    {
        if (_aesGcm == null)
            throw new InvalidOperationException(
                "Encryption key has not been initialised. " +
                "Call GetFileHeader (new file) or LoadFromFileHeader (existing file) " +
                "before performing page I/O.");
        return _aesGcm;
    }
}
