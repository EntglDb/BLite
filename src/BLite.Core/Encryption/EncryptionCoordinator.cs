using System;
using System.Buffers.Binary;
using System.Security.Cryptography;

namespace BLite.Core.Encryption;

/// <summary>
/// Owns the master key and derives a unique 256-bit sub-key per physical file using
/// HKDF-SHA256, eliminating nonce-reuse risk in multi-file (server) mode.
/// </summary>
/// <remarks>
/// <para>
/// In multi-file mode BLite opens several physical files per logical database (main file,
/// collection files, index files, WAL).  Using the same AES-256-GCM key for every file
/// would cause nonce reuse when two files share the same <c>pageId</c> — a catastrophic
/// AES-GCM failure.  The coordinator solves this by deriving a separate 256-bit sub-key
/// for every file:
/// </para>
/// <code>
/// SubKey_i = HKDF-SHA256(
///     inputKeyMaterial : masterKey,     // 32 bytes (provided externally)
///     salt             : databaseSalt,  // 32 bytes, stored in main FileHeader
///     info             : fileRole || fileIndexLow || fileIndexHigh  // 3-byte context
/// )
/// </code>
/// <para><b>Typical usage</b></para>
/// <list type="number">
///   <item>Create the coordinator from an externally-provided 32-byte master key.</item>
///   <item>
///     Call <see cref="CreateForMainFile"/> and pass the returned <see cref="ICryptoProvider"/>
///     to the main <c>PageFile</c>.  When that file is opened (or created), the coordinator
///     automatically reads (or generates) the 32-byte database salt stored in the BLCE header
///     and caches it for subsequent sub-key derivations.
///   </item>
///   <item>
///     Call <see cref="CreateForCollection"/>, <see cref="CreateForIndex"/>, and
///     <see cref="CreateForWal"/> as additional files are needed.  Each call derives a
///     unique sub-key from the cached database salt.
///   </item>
///   <item>Keep the coordinator alive for the engine lifetime; dispose it on shutdown.</item>
/// </list>
/// <para><b>Thread safety</b></para>
/// <para>
/// The factory methods are NOT thread-safe against concurrent calls.  In the expected
/// usage pattern (sequential initialization of files at engine startup) this is fine.
/// </para>
/// </remarks>
public sealed class EncryptionCoordinator : IDisposable
{
    // ── BLCE header constants ─────────────────────────────────────────────────

    /// <summary>Magic bytes at the start of every BLCE file header ("BLCE").</summary>
    private static readonly uint HeaderMagic = 0x424C4345u;

    private const byte HeaderVersion = 1;
    private const byte AlgorithmAesGcm = 1;

    /// <summary>
    /// KDF identifier used for coordinator-managed files (HKDF-SHA256).
    /// Distinguishes them from stand-alone PBKDF2-encrypted files (KDF=1).
    /// </summary>
    private const byte KdfHkdf = 2;

    private const int HeaderSize = 64;
    private const int TagSize    = 16;
    private const int NonceSize  = 12;
    private const int KeySize    = 32;
    private const int SaltOffset = 8;
    private const int SaltSize   = 32;

    // ── State ─────────────────────────────────────────────────────────────────

    private byte[] _masterKey;          // zeroed on Dispose
    private byte[]? _databaseSalt;      // 32 bytes, extracted from main file header
    private bool _disposed;

    // ── Construction ─────────────────────────────────────────────────────────

    /// <summary>
    /// Creates a new <see cref="EncryptionCoordinator"/> from a 32-byte master key.
    /// </summary>
    /// <param name="masterKey">
    /// A 32-byte master key supplied by the host application (e.g. retrieved from a KMS).
    /// The coordinator makes a private copy; the caller may zero the original immediately
    /// after construction.  The internal copy is zeroed when <see cref="Dispose"/> is called.
    /// </param>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="masterKey"/> is <c>null</c> or not exactly 32 bytes.
    /// </exception>
    public EncryptionCoordinator(byte[] masterKey)
    {
        if (masterKey == null || masterKey.Length != KeySize)
            throw new ArgumentException(
                $"Master key must be exactly {KeySize} bytes.", nameof(masterKey));

        _masterKey = (byte[])masterKey.Clone();
    }

    // ── Public factory methods ────────────────────────────────────────────────

    /// <summary>
    /// Creates an <see cref="ICryptoProvider"/> for the main database file.
    /// </summary>
    /// <remarks>
    /// The returned provider writes or reads a standard 64-byte BLCE file header when
    /// the <c>PageFile</c> is opened.  As part of that header exchange the coordinator
    /// caches the 32-byte database salt; subsequent calls to
    /// <see cref="CreateForCollection"/>, <see cref="CreateForIndex"/>, and
    /// <see cref="CreateForWal"/> require the salt to be available.
    /// </remarks>
    public ICryptoProvider CreateForMainFile()
    {
        ThrowIfDisposed();
        return new CoordinatedFileProvider(this, fileRole: 0, fileIndex: 0);
    }

    /// <summary>
    /// Derives a unique AES-256-GCM sub-key for collection file
    /// <paramref name="collectionIndex"/> and returns an <see cref="ICryptoProvider"/>
    /// initialised with that sub-key.
    /// </summary>
    /// <param name="collectionIndex">
    /// 0-based index of the collection file (0–65 535).
    /// </param>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the main file has not been opened yet (database salt unavailable).
    /// </exception>
    public ICryptoProvider CreateForCollection(int collectionIndex)
    {
        if (collectionIndex < 0 || collectionIndex > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(collectionIndex),
                $"Collection index must be between 0 and {ushort.MaxValue}.");
        ThrowIfDisposed();
        EnsureSaltAvailable();
        return new CoordinatedFileProvider(this, fileRole: 1, fileIndex: (ushort)collectionIndex);
    }

    /// <summary>
    /// Derives a unique AES-256-GCM sub-key for index file <paramref name="indexIndex"/>
    /// and returns an <see cref="ICryptoProvider"/> initialised with that sub-key.
    /// </summary>
    /// <param name="indexIndex">
    /// 0-based index of the index file (0–65 535).
    /// </param>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the main file has not been opened yet (database salt unavailable).
    /// </exception>
    public ICryptoProvider CreateForIndex(int indexIndex)
    {
        if (indexIndex < 0 || indexIndex > ushort.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(indexIndex),
                $"Index index must be between 0 and {ushort.MaxValue}.");
        ThrowIfDisposed();
        EnsureSaltAvailable();
        return new CoordinatedFileProvider(this, fileRole: 2, fileIndex: (ushort)indexIndex);
    }

    /// <summary>
    /// Derives a unique AES-256-GCM sub-key for the WAL and returns an
    /// <see cref="ICryptoProvider"/> initialised with that sub-key.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the main file has not been opened yet (database salt unavailable).
    /// </exception>
    public ICryptoProvider CreateForWal()
    {
        ThrowIfDisposed();
        EnsureSaltAvailable();
        return new CoordinatedFileProvider(this, fileRole: 3, fileIndex: 0);
    }

    // ── IDisposable ───────────────────────────────────────────────────────────

    /// <summary>
    /// Zeroes sensitive key material from memory.
    /// </summary>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        CryptographicOperations.ZeroMemory(_masterKey);
        if (_databaseSalt != null)
            CryptographicOperations.ZeroMemory(_databaseSalt);
    }

    // ── Internal helpers (accessible to the nested private class) ─────────────

    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(EncryptionCoordinator));
    }

    private void EnsureSaltAvailable()
    {
        if (_databaseSalt == null)
            throw new InvalidOperationException(
                "The database salt has not been initialised. " +
                "Call CreateForMainFile() first and ensure the main PageFile has been opened " +
                "(which triggers GetFileHeader or LoadFromFileHeader on the returned provider).");
    }

    /// <summary>
    /// Stores the 32-byte database salt extracted from the main file header.
    /// Called by <see cref="CoordinatedFileProvider"/> during header exchange.
    /// </summary>
    private void SetDatabaseSalt(ReadOnlySpan<byte> salt)
    {
        _databaseSalt = salt.ToArray();
    }

    /// <summary>
    /// Derives the 32-byte AES-256 sub-key for the specified file role / index.
    /// HKDF info = [ fileRole (1 byte) | fileIndex low byte | fileIndex high byte ].
    /// </summary>
    private byte[] DeriveSubKey(byte fileRole, ushort fileIndex)
    {
        var info = new byte[3]
        {
            fileRole,
            (byte)(fileIndex & 0xFF),
            (byte)((fileIndex >> 8) & 0xFF)
        };
        return KeyDerivation.DeriveKeyHkdf(_masterKey, _databaseSalt!, info, KeySize);
    }

    // ── Nested provider ───────────────────────────────────────────────────────

    /// <summary>
    /// AES-256-GCM provider whose encryption key is derived by the coordinator.
    /// </summary>
    /// <remarks>
    /// <para>
    /// For the <b>main file</b> (role 0) the sub-key cannot be derived until the
    /// database salt is known, so key initialisation is deferred to
    /// <see cref="GetFileHeader"/> (new file) or <see cref="LoadFromFileHeader"/>
    /// (existing file).  Both methods extract the salt, store it in the coordinator,
    /// and then derive the sub-key.
    /// </para>
    /// <para>
    /// For <b>secondary files</b> (collections, indexes, WAL) the salt is already
    /// known at construction time, so the sub-key is derived immediately.
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
    private sealed class CoordinatedFileProvider : ICryptoProvider, IDisposable
    {
        private readonly EncryptionCoordinator _coordinator;
        private readonly byte _fileRole;
        private readonly ushort _fileIndex;
        private AesGcm? _aesGcm;

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

        // ── ICryptoProvider ───────────────────────────────────────────────────

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

            BinaryPrimitives.WriteUInt32LittleEndian(header, HeaderMagic);
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
            if (magic != HeaderMagic)
                throw new InvalidOperationException(
                    $"Encrypted file header magic mismatch. " +
                    $"Expected 0x{HeaderMagic:X8}, got 0x{magic:X8}. " +
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

        // ── IDisposable ───────────────────────────────────────────────────────

        public void Dispose()
        {
            _aesGcm?.Dispose();
            _aesGcm = null;
        }

        // ── Private helpers ───────────────────────────────────────────────────

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
}
