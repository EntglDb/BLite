using System;
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
    // Visible to CoordinatedFileProvider (same assembly, separate file).

    /// <summary>Magic bytes at the start of every BLCE file header ("BLCE").</summary>
    internal static readonly uint HeaderMagic = 0x424C4345u;

    internal const byte HeaderVersion = 1;
    internal const byte AlgorithmAesGcm = 1;

    /// <summary>
    /// KDF identifier used for coordinator-managed files (HKDF-SHA256).
    /// Distinguishes them from stand-alone PBKDF2-encrypted files (KDF=1).
    /// </summary>
    internal const byte KdfHkdf = 2;

    internal const int HeaderSize = 64;
    internal const int TagSize    = 16;
    internal const int NonceSize  = 12;
    internal const int KeySize    = 32;
    internal const int SaltOffset = 8;
    internal const int SaltSize   = 32;

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
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="masterKey"/> is <c>null</c>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="masterKey"/> is not exactly 32 bytes.
    /// </exception>
    public EncryptionCoordinator(byte[] masterKey)
    {
        if (masterKey == null)
            throw new ArgumentNullException(nameof(masterKey));
        if (masterKey.Length != KeySize)
            throw new ArgumentException(
                $"Master key must be exactly {KeySize} bytes.", nameof(masterKey));

        _masterKey = (byte[])masterKey.Clone();
    }

    /// <summary>
    /// Internal constructor used by <see cref="BLite.Core.BLiteEngine"/> and
    /// <see cref="BLite.Core.DocumentDbContext"/> when an end-user supplies
    /// <see cref="CryptoOptions.FromMasterKey(System.ReadOnlySpan{byte})"/>.
    /// Takes ownership of a freshly-copied master key extracted from the options.
    /// </summary>
    internal EncryptionCoordinator(CryptoOptions options)
    {
        if (options is null) throw new ArgumentNullException(nameof(options));
        if (!options.IsMasterKeyMode)
            throw new ArgumentException(
                "CryptoOptions must be created via FromMasterKey to drive an EncryptionCoordinator.",
                nameof(options));
        // CopyMasterKey already returns a fresh, owned array.
        _masterKey = options.CopyMasterKey();
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
    /// On first call, caches the salt so subsequent sub-file providers can derive subkeys.
    /// Subsequent calls with the same salt are idempotent (re-open of the same file).
    /// Throws if a different (conflicting) salt is supplied after the first initialisation.
    /// </summary>
    internal void SetDatabaseSalt(ReadOnlySpan<byte> salt)
    {
        if (_databaseSalt != null)
        {
            if (!salt.SequenceEqual(_databaseSalt))
                throw new InvalidOperationException(
                    "A different database salt was supplied after the coordinator was already " +
                    "initialised. The same main file header must be used for the lifetime of a " +
                    "single coordinator instance.");
            // Same salt — no-op (idempotent re-open of the same file).
            return;
        }

        _databaseSalt = salt.ToArray();
    }

    /// <summary>
    /// Derives the 32-byte AES-256 sub-key for the specified file role / index.
    /// HKDF info = [ fileRole (1 byte) | fileIndex low byte | fileIndex high byte ].
    /// </summary>
    internal byte[] DeriveSubKey(byte fileRole, ushort fileIndex)
    {
        ThrowIfDisposed();
        var info = new byte[3]
        {
            fileRole,
            (byte)(fileIndex & 0xFF),
            (byte)(fileIndex >> 8)
        };
        return KeyDerivation.DeriveKeyHkdf(_masterKey, _databaseSalt!, info, KeySize);
    }
}
