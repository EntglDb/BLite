using System.Buffers;
using System.IO.MemoryMappedFiles;
using BLite.Core.Encryption;

namespace BLite.Core.Storage;

/// <summary>
/// Configuration for page-based storage
/// </summary>
public readonly struct PageFileConfig
{
    public int PageSize { get; init; }
    /// <summary>
    /// The file grows in fixed increments of this size.
    /// Must be a multiple of <see cref="PageSize"/>.
    /// Waste is bounded to at most one block regardless of database size.
    /// </summary>
    public int GrowthBlockSize { get; init; }
    public MemoryMappedFileAccess Access { get; init; }

    /// <summary>
    /// Optional explicit path for the WAL file.
    /// If null, the WAL is placed next to the data file with the same name and .wal extension (default behavior).
    /// Set this to place the WAL on a separate disk or directory for better I/O performance.
    /// </summary>
    public string? WalPath { get; init; }

    /// <summary>
    /// Optional explicit path for the index file (.idx).
    /// If null, all pages (data + index) are stored in the same file (default embedded behavior).
    /// Set this to place index pages on a separate file for parallel I/O with data pages.
    /// </summary>
    public string? IndexFilePath { get; init; }

    /// <summary>
    /// Optional directory where per-collection data files are stored.
    /// If null, all collections share the main data file (default embedded behavior).
    /// If set, each collection gets its own {CollectionName}.db file in this directory,
    /// enabling independent I/O, per-collection backup, and instant space reclaim on drop.
    /// The main data file still stores metadata (page 0, page 1, dictionary, KV store).
    /// </summary>
    public string? CollectionDataDirectory { get; init; }

    /// <summary>
    /// Lock acquisition timeouts for read and write operations.
    /// Controls how long the engine waits for a contended lock before throwing
    /// <see cref="System.TimeoutException"/> (analogous to SQLite's <c>SQLITE_BUSY</c>).
    /// Defaults to <see cref="LockTimeout.Default"/> (500 ms read / 500 ms write).
    /// Use <see cref="LockTimeout.Immediate"/> for fail-fast behaviour.
    /// </summary>
    public LockTimeout LockTimeout { get; init; }

    /// <summary>
    /// Optional transparent page-level encryption provider.
    /// When <c>null</c> (the default), data is stored in plaintext and the file layout is
    /// identical to all previous versions of BLite (zero overhead, full backwards compatibility).
    /// <para>
    /// A single provider is sufficient for the entire database.  The
    /// <see cref="BLite.Core.Storage.StorageEngine"/> automatically derives sibling providers
    /// for the WAL, the index file, and per-collection files by calling
    /// <see cref="ICryptoProvider.CreateSiblingProvider"/> — so every physical file is
    /// encrypted with a distinct key while the caller only configures one provider.
    /// </para>
    /// <para><b>Single-file vs server (multi-file) layout</b></para>
    /// <list type="bullet">
    /// <item><description>
    /// In single-file mode (<see cref="WalDirectoryPath"/>, <see cref="IndexFilePath"/>,
    /// <see cref="CollectionDataDirectory"/> all <c>null</c>) any <see cref="ICryptoProvider"/>
    /// implementation is acceptable, including the simple
    /// <see cref="BLite.Core.Encryption.AesGcmCryptoProvider"/> derived from a passphrase.
    /// </description></item>
    /// <item><description>
    /// In server (multi-file) mode the provider <b>MUST</b> implement
    /// <see cref="ICryptoProvider.CreateSiblingProvider"/> by deriving a unique sub-key per
    /// physical file (HKDF), otherwise WAL records, index pages, and collection pages would
    /// share the same AES-GCM key and reuse the same nonce space — a fatal break of GCM
    /// security. The supported way to obtain such a provider is via
    /// <see cref="BLite.Core.Encryption.CryptoOptions.FromMasterKey(System.ReadOnlySpan{byte})"/>;
    /// the engine then constructs the appropriate <see cref="ICryptoProvider"/> internally.
    /// </description></item>
    /// </list>
    /// <para><b>Ownership</b></para>
    /// <para>
    /// The <see cref="StorageEngine"/> takes ownership of the provider placed on the config
    /// (and of every sibling provider it derives) and disposes them when the engine itself is
    /// disposed. Do not dispose the provider yourself once the config has been handed to the
    /// engine.
    /// </para>
    /// </summary>
    public ICryptoProvider? CryptoProvider { get; init; }

    /// <summary>
    /// When <c>true</c>, opt in to multi-process database access (N readers + 1 writer
    /// across OS processes on the same host).
    /// <para>
    /// Defaults to <c>false</c>, in which case BLite preserves its long-standing
    /// single-process semantics (every file opened with <see cref="FileShare.None"/>,
    /// in-process synchronization only).
    /// </para>
    /// <para>
    /// This flag is the configuration entry point for the multi-process WAL feature
    /// described in <c>roadmap/v5/MULTI_PROCESS_WAL.md</c> (issue: "Multi-Process WAL
    /// Access via .wal-shm sidecar file"). The full implementation — relaxed file shares,
    /// the <c>.wal-shm</c> sidecar, cross-process writer / checkpoint locks, reader-slot
    /// registration, and stale-PID recovery — is rolled out in subsequent phases. Until
    /// those phases land, setting this flag has no observable runtime effect.
    /// </para>
    /// <para>
    /// Not supported on WASM / browser runtimes (no filesystem locking, no shared
    /// <see cref="System.IO.MemoryMappedFiles.MemoryMappedFile"/>) and not safe on
    /// network filesystems (NFS / SMB) where file-locking semantics are unreliable.
    /// </para>
    /// </summary>
    public bool AllowMultiProcessAccess { get; init; }

    /// <summary>
    /// Small pages for embedded scenarios with many tiny documents
    /// </summary>
    public static PageFileConfig Small => new()
    {
        PageSize = 8192,          // 8KB pages
        GrowthBlockSize = 512 * 1024,  // grow by 512KB at a time
        Access = MemoryMappedFileAccess.ReadWrite,
        LockTimeout = LockTimeout.Default
    };

    /// <summary>
    /// Default balanced configuration for document databases (16KB like MySQL InnoDB)
    /// </summary>
    public static PageFileConfig Default => new()
    {
        PageSize = 16384,         // 16KB pages
        GrowthBlockSize = 1024 * 1024, // grow by 1MB at a time
        Access = MemoryMappedFileAccess.ReadWrite,
        LockTimeout = LockTimeout.Default
    };

    /// <summary>
    /// Large pages for databases with big documents (32KB like MongoDB WiredTiger)
    /// </summary>
    public static PageFileConfig Large => new()
    {
        PageSize = 32768,              // 32KB pages
        GrowthBlockSize = 2 * 1024 * 1024, // grow by 2MB at a time
        Access = MemoryMappedFileAccess.ReadWrite,
        LockTimeout = LockTimeout.Default
    };

    /// <summary>
    /// Server-optimized configuration: separate WAL, index, and per-collection files.
    /// Designed for BLite.Server where each connection serves multiple clients.
    /// </summary>
    /// <param name="databasePath">
    /// Full path to the .db file (e.g. <c>/data/blite/mydb.db</c>).
    /// Sub-file paths are derived from the database filename so that multiple databases
    /// in the same parent directory do not share WAL or index files.
    /// </param>
    /// <param name="baseConfig">
    /// Optional base configuration that controls page size, growth block, and memory-map access.
    /// When <c>null</c>, <see cref="Default"/> (16 KB pages) is used.
    /// Use <see cref="Small"/> or <see cref="Large"/> to override the page size while keeping
    /// the server-layout paths.
    /// </param>
    public static PageFileConfig Server(string databasePath, PageFileConfig? baseConfig = null)
    {
        var @base = baseConfig ?? Default;
        return @base with
        {
            WalPath = Path.Combine(
                Path.GetDirectoryName(databasePath) ?? ".",
                "wal",
                Path.GetFileNameWithoutExtension(databasePath) + ".wal"),
            IndexFilePath = Path.ChangeExtension(databasePath, ".idx"),
            CollectionDataDirectory = Path.Combine(
                Path.GetDirectoryName(databasePath) ?? ".",
                "collections",
                Path.GetFileNameWithoutExtension(databasePath))
        };
    }

    /// <summary>
    /// Detects the page size from an existing database file by reading the page-0 header.
    /// Returns a matching <see cref="PageFileConfig"/> with <see cref="MemoryMappedFileAccess.ReadWrite"/> access.
    /// Returns <c>null</c> if the file does not exist or is empty.
    /// </summary>
    public static PageFileConfig? DetectFromFile(string path)
    {
        if (!File.Exists(path))
            return null;

        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        if (fs.Length < 32)
            return null;

        var arr = new byte[32];
        int totalRead = 0;
        while (totalRead < 32)
        {
            int n = fs.Read(arr, totalRead, 32 - totalRead);
            if (n == 0) return null;
            totalRead += n;
        }
        var header = PageHeader.ReadFrom(arr);
        if (header.PageType != PageType.Header)
            return null;

        int detectedPageSize = header.FreeBytes + 32;

        PageFileConfig baseConfig = detectedPageSize switch
        {
            8192  => Small,
            16384 => Default,
            32768 => Large,
            _     => new PageFileConfig
            {
                PageSize = detectedPageSize,
                GrowthBlockSize = detectedPageSize * 64,
                Access = MemoryMappedFileAccess.ReadWrite,
                LockTimeout = LockTimeout.Default
            }
        };

        // Probe for multi-file companion files.
        // If the per-collection directory exists, this is a full server-layout database;
        // return the canonical Server config so all companion paths are set consistently.
        var dir     = Path.GetDirectoryName(path) ?? ".";
        var name    = Path.GetFileNameWithoutExtension(path);
        var collDir = Path.Combine(dir, "collections", name);

        if (Directory.Exists(collDir))
            return Server(path, baseConfig);

        // Partial multi-file: separate index file and/or WAL, single data file.
        var idxPath = Path.ChangeExtension(path, ".idx");
        var walPath = Path.Combine(dir, "wal", name + ".wal");
        var hasIdx  = File.Exists(idxPath);
        var hasWal  = File.Exists(walPath);

        if (hasIdx || hasWal)
        {
            return baseConfig with
            {
                IndexFilePath = hasIdx ? idxPath : null,
                WalPath       = hasWal ? walPath : null,
            };
        }

        return baseConfig;
    }
}

/// <summary>
/// Page-based file storage with memory-mapped I/O.
/// Manages fixed-size pages for efficient storage and retrieval.
/// Implements <see cref="IPageStorage"/> — the pluggable storage backend abstraction.
/// </summary>
public sealed class PageFile : IPageStorage
{
    private readonly string _filePath;
    private readonly PageFileConfig _config;
    private FileStream? _fileStream;
    private MemoryMappedFile? _mappedFile;
    // Persistent full-file read accessor — created once on Open(), recreated on file growth.
    // Enables zero-alloc page reads via unsafe pointer instead of
    // CreateViewAccessor-per-read (which allocates an object + 16 KB temp array each time).
    private MemoryMappedViewAccessor? _readAccessor;

    // ── Encryption ────────────────────────────────────────────────────────────
    // Null means no encryption (zero overhead, unchanged file format).
    // Non-null means page I/O is transparently encrypted/decrypted.
    private readonly ICryptoProvider? _cryptoProvider;

    // Cached derived values — computed once in the constructor from _config and _cryptoProvider.
    // Physical page size = logical page size + per-page crypto overhead (0 or 16).
    private readonly int _physicalPageSize;
    // Byte offset from the start of the file to the first page (0 normally, 64 for AES-GCM).
    private readonly int _cryptoFileHeaderSize;

    // _rwLock guards _mappedFile, _nextPageId, and _firstFreePageId.
    // Read lock:  ReadPage(), WritePage() when no file growth is needed.
    // Write lock: Open(), AllocatePage(), FreePage(), WritePage() when file grows,
    //             Flush(), Dispose() — any operation that recreates _mappedFile or
    //             modifies structural state.
    // ReaderWriterLockSlim allows many concurrent readers while serialising writers,
    // eliminating the race where a concurrent resize (write lock) disposes _mappedFile
    // while ReadPage() (read lock) is creating a view accessor from it.
    private readonly ReaderWriterLockSlim _rwLock = new(LockRecursionPolicy.NoRecursion);

    // Derived from config — distinct read/write timeouts.
    private int ReadLockTimeoutMs => _config.LockTimeout.ReadTimeoutMs;
    private int WriteLockTimeoutMs => _config.LockTimeout.WriteTimeoutMs;

    // _asyncLock serialises FlushAsync() and BackupAsync(), which must hold exclusive
    // access across an await boundary and therefore cannot use ReaderWriterLockSlim.
    private readonly SemaphoreSlim _asyncLock = new(1, 1);

    private volatile bool _disposed;
    private uint _nextPageId;
    private uint _firstFreePageId;

    public uint NextPageId => _nextPageId;

    public PageFile(string filePath, PageFileConfig config)
    {
        _filePath = filePath ?? throw new ArgumentNullException(nameof(filePath));
        _config = config;
        _cryptoProvider = config.CryptoProvider;
        _physicalPageSize = _config.PageSize + (_cryptoProvider?.PageOverhead ?? 0);
        _cryptoFileHeaderSize = _cryptoProvider?.FileHeaderSize ?? 0;
    }

    public int PageSize => _config.PageSize;

    /// <summary>
    /// Rounds up <paramref name="requiredLength"/> to the next multiple of
    /// <see cref="PageFileConfig.GrowthBlockSize"/>.
    /// Growth is bounded: the file never over-allocates more than one block.
    /// </summary>
    private long AlignToBlock(long requiredLength)
    {
        long block = _config.GrowthBlockSize;
        return (requiredLength + block - 1) / block * block;
    }

    /// <summary>
    /// Opens the page file, creating it if it doesn't exist
    /// </summary>
    public void Open()
    {
        if (!_rwLock.TryEnterWriteLock(WriteLockTimeoutMs))
            throw new TimeoutException("Timed out acquiring PageFile write lock (Open).");
        try
        {
            if (_fileStream != null)
                return; // Already open

            var fileExists = File.Exists(_filePath);
            
            _fileStream = new FileStream(
                _filePath,
                FileMode.OpenOrCreate,
                FileAccess.ReadWrite,
                FileShare.None,
                bufferSize: 4096,
#if NET6_0_OR_GREATER
                FileOptions.RandomAccess | FileOptions.Asynchronous);
#else
                FileOptions.Asynchronous);
#endif

            bool isNew = !fileExists || _fileStream.Length == 0;

            if (isNew)
            {
                // Write the per-file crypto header (if any) BEFORE the memory map is created.
                // GetFileHeader generates the random salt and derives the encryption key.
                if (_cryptoProvider != null && _cryptoFileHeaderSize > 0)
                {
                    var cryptoHdr = new byte[_cryptoFileHeaderSize];
                    _cryptoProvider.GetFileHeader(cryptoHdr);
                    _fileStream.Position = 0;
                    _fileStream.Write(cryptoHdr);
                }

                // Allocate space for the crypto header + first two pages (Header + Collection Metadata).
                // For encrypted files, allocate exactly without pre-growth: extra pages
                // would be all-zeros on disk, and attempting to decrypt zeros as AES-GCM
                // ciphertext produces an AuthenticationTagMismatch error.
                // For plain files, round up to the nearest growth block for I/O efficiency.
                var initialSize = _cryptoProvider != null && _cryptoFileHeaderSize > 0
                    ? _cryptoFileHeaderSize + (long)_physicalPageSize * 2
                    : AlignToBlock((long)_physicalPageSize * 2);
                _fileStream.SetLength(initialSize);
            }
            else if (_fileStream.Length >= GetMinimumExistingFileSize())
            {
                if (_cryptoProvider != null && _cryptoFileHeaderSize > 0)
                {
                    // Encrypted file: read and validate the per-file crypto header,
                    // then derive the key. This must happen before any page I/O.
                    var cryptoHdr = new byte[_cryptoFileHeaderSize];
                    ReadBytesFromStream(_fileStream, 0, cryptoHdr);
                    _cryptoProvider.LoadFromFileHeader(cryptoHdr);
                }
                else
                {
                    // Plain file: validate that the page size matches the configuration.
                    Span<byte> probe = stackalloc byte[32];
#if NET6_0_OR_GREATER
                    _fileStream.Position = 0;
                    _fileStream.ReadExactly(probe);
#else
                    var probeArr = new byte[32];
                    int probeRead = 0;
                    while (probeRead < 32)
                    {
                        int n = _fileStream.Read(probeArr, probeRead, 32 - probeRead);
                        if (n == 0) break;
                        probeRead += n;
                    }
                    probeArr.AsSpan().CopyTo(probe);
#endif
                    _fileStream.Position = 0;
                    var fileHeader = PageHeader.ReadFrom(probe);
                    int actualPageSize = fileHeader.FreeBytes + 32;
                    if (actualPageSize != _config.PageSize)
                        throw new InvalidOperationException(
                            $"Page size mismatch: file was created with {actualPageSize} byte pages, "
                          + $"but the configuration specifies {_config.PageSize} byte pages. "
                          + $"Use PageFileConfig.DetectFromFile() or the correct preset.");
                }
            }

            // Guard against truncated or corrupt files — the data region must be large
            // enough to contain the crypto header and at least one complete page, and
            // must be exactly aligned to the physical page size.
            if (_fileStream.Length < _cryptoFileHeaderSize)
                throw new System.IO.InvalidDataException(
                    $"Corrupt or truncated page file '{_filePath}': length {_fileStream.Length} is " +
                    $"smaller than the crypto header size {_cryptoFileHeaderSize}.");

            long dataLength = _fileStream.Length - _cryptoFileHeaderSize;
            if (dataLength % _physicalPageSize != 0)
                throw new System.IO.InvalidDataException(
                    $"Corrupt page file '{_filePath}': payload length {dataLength} is not aligned " +
                    $"to the physical page size {_physicalPageSize}.");

            // Calculate next page ID accounting for the crypto header and physical page size.
            _nextPageId = checked((uint)(dataLength / _physicalPageSize));

            _mappedFile = MemoryMappedFile.CreateFromFile(
                _fileStream,
                null,
                _fileStream.Length,
                _config.Access,
                HandleInheritability.None,
                leaveOpen: true);

            // Persistent read accessor — covers the whole mapped region (size=0 means entire file).
            // Used by ReadPageCore for zero-alloc reads via AcquirePointer.
            _readAccessor?.Dispose();
            _readAccessor = _mappedFile.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read);

            if (isNew)
            {
                // Write the initial page-0 (file header) and page-1 (collection metadata)
                // via WritePageCore so that encryption is applied when a crypto provider is set.
                InitializeHeader();
            }
            else
            {
                // Read free list head from Page 0 (decrypts automatically when a provider is set).
                if ((_fileStream.Length - _cryptoFileHeaderSize) >= _physicalPageSize)
                {
                    var headerBuf = new byte[_config.PageSize];
                    ReadPageCore(0, headerBuf);
                    var hdr = PageHeader.ReadFrom(headerBuf);
                    _firstFreePageId = hdr.NextPageId;
                }
            }
        }
        finally
        {
            _rwLock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Returns the minimum file length (in bytes) for a non-empty existing file,
    /// used to decide whether to attempt header validation during <see cref="Open"/>.
    /// </summary>
    private int GetMinimumExistingFileSize()
        => _cryptoFileHeaderSize > 0 ? _cryptoFileHeaderSize : 32;

    /// <summary>Reads exactly <paramref name="buffer"/>.Length bytes from the file at the given offset.</summary>
    /// <exception cref="System.IO.EndOfStreamException">
    /// Thrown if the stream ends before the buffer is filled, indicating a truncated file.
    /// </exception>
    private static void ReadBytesFromStream(FileStream stream, long offset, byte[] buffer)
    {
        stream.Position = offset;
        int read = 0;
        while (read < buffer.Length)
        {
            int n = stream.Read(buffer, read, buffer.Length - read);
            if (n == 0)
                throw new System.IO.EndOfStreamException(
                    $"Unexpected end of stream: expected {buffer.Length} bytes at offset {offset}, " +
                    $"but only {read} were available. The file may be truncated or corrupt.");
            read += n;
        }
    }

    private void InitializeHeader()
    {
        // 1. Initialize Header (Page 0)
        var header = new PageHeader
        {
            PageId = 0,
            PageType = PageType.Header,
            FreeBytes = (ushort)(_config.PageSize - 32),
            NextPageId = 0, // No free pages initially
            TransactionId = 0,
            Checksum = 0,
            FormatVersion = PageHeader.CurrentFormatVersion
        };

        var buffer = ArrayPool<byte>.Shared.Rent(_config.PageSize);
        try
        {
            Array.Clear(buffer, 0, _config.PageSize);
            header.WriteTo(buffer);

            // WritePageCore handles encryption transparently.
            WritePageCore(0, buffer);

            // 2. Initialize Collection Metadata (Page 1)
            Array.Clear(buffer, 0, _config.PageSize);
            var metaHeader = new SlottedPageHeader
            {
                PageId = 1,
                PageType = PageType.Collection,
                SlotCount = 0,
                FreeSpaceStart = SlottedPageHeader.Size,
                FreeSpaceEnd = (ushort)_config.PageSize,
                NextOverflowPage = 0,
                TransactionId = 0
            };
            metaHeader.WriteTo(buffer);

            WritePageCore(1, buffer);
        }
        finally
        {
            // clearArray: true — the buffer held plaintext page headers; clear it before
            // returning to the pool so subsequent renters cannot read sensitive data.
            ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
        }

        _fileStream!.Flush();
    }

    // ── Lock-free core helpers ─────────────────────────────────────────────
    // These methods perform the raw MMF read/write without acquiring any lock.
    // They MUST only be called by code that already holds _rwLock in an
    // appropriate mode:
    //   ReadPageCore    — ReadLock or WriteLock
    //   WritePageCore   — ReadLock or WriteLock
    //   EnsureCapacityCore — WriteLock only (modifies _mappedFile)

    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(PageFile));
    }

    private unsafe void ReadPageCore(uint pageId, Span<byte> destination)
    {
        // Physical byte offset of this page within the file.
        var offset = _cryptoFileHeaderSize + (long)pageId * _physicalPageSize;
        byte* basePtr = null;
        _readAccessor!.SafeMemoryMappedViewHandle.AcquirePointer(ref basePtr);
        try
        {
            var physicalPage = new ReadOnlySpan<byte>(basePtr + offset, _physicalPageSize);
            if (_cryptoProvider != null)
            {
                // Decrypt physical page (ciphertext + tag) into the logical destination.
                _cryptoProvider.Decrypt(pageId, physicalPage, destination[.._config.PageSize]);
            }
            else
            {
                // Zero-alloc fast path: one memcpy, no heap allocation.
                physicalPage.CopyTo(destination);
            }
        }
        finally
        {
            _readAccessor.SafeMemoryMappedViewHandle.ReleasePointer();
        }
    }

    private unsafe void ReadPageHeaderCore(uint pageId, Span<byte> destination)
    {
        var offset = _cryptoFileHeaderSize + (long)pageId * _physicalPageSize;
        byte* basePtr = null;
        _readAccessor!.SafeMemoryMappedViewHandle.AcquirePointer(ref basePtr);
        try
        {
            if (_cryptoProvider != null)
            {
                // For encrypted pages we must decrypt the entire page to access its header.
                // Use a rented buffer to avoid stack pressure.
                var buf = ArrayPool<byte>.Shared.Rent(_config.PageSize);
                try
                {
                    var physicalPage = new ReadOnlySpan<byte>(basePtr + offset, _physicalPageSize);
                    _cryptoProvider.Decrypt(pageId, physicalPage, buf.AsSpan(0, _config.PageSize));
                    buf.AsSpan(0, destination.Length).CopyTo(destination);
                }
                finally
                {
                    // clearArray: true — buf holds decrypted page data; clear before returning
                    // to the pool so subsequent renters cannot read sensitive plaintext.
                    ArrayPool<byte>.Shared.Return(buf, clearArray: true);
                }
            }
            else
            {
                // Like ReadPageCore but copies only destination.Length bytes from the page start.
                new ReadOnlySpan<byte>(basePtr + offset, destination.Length).CopyTo(destination);
            }
        }
        finally
        {
            _readAccessor.SafeMemoryMappedViewHandle.ReleasePointer();
        }
    }

    private void WritePageCore(uint pageId, ReadOnlySpan<byte> source)
    {
        var offset = _cryptoFileHeaderSize + (long)pageId * _physicalPageSize;

        if (_cryptoProvider != null)
        {
            // Encrypt the plaintext page into a temporary buffer, then write to the MMF.
            // The in-memory (plaintext) buffer is never modified.
            var tempBuf = ArrayPool<byte>.Shared.Rent(_physicalPageSize);
            try
            {
                _cryptoProvider.Encrypt(pageId, source[.._config.PageSize], tempBuf.AsSpan(0, _physicalPageSize));
                using var accessor = _mappedFile!.CreateViewAccessor(offset, _physicalPageSize, MemoryMappedFileAccess.Write);
                accessor.WriteArray(0, tempBuf, 0, _physicalPageSize);
                accessor.Flush();
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(tempBuf);
            }
        }
        else
        {
            using var accessor = _mappedFile!.CreateViewAccessor(offset, _config.PageSize, MemoryMappedFileAccess.Write);
            accessor.WriteArray(0, source.ToArray(), 0, _config.PageSize);
            // FlushViewOfFile is required to guarantee that writes made through the
            // memory-mapped view are visible to subsequent ReadFile / RandomAccess.ReadAsync
            // calls on the same handle. Without it, the OS does not guarantee coherency
            // between the mapped-view pages and the buffered-file-I/O view.
            accessor.Flush();
        }
    }

    // ── Grow-file helper ───────────────────────────────────────────────────
    // Must be called under _rwLock write lock. Extends the file and recreates
    // _mappedFile when the requested offset does not fit in the current mapping.
    private void EnsureCapacityCore(long requiredOffset)
    {
        if (requiredOffset + _physicalPageSize <= _fileStream!.Length)
            return;

        // For encrypted files, grow exactly to fit the new page.  Extra (unwritten) pages
        // contain all-zeros and cannot be decrypted by AES-GCM (auth tag mismatch).
        // For plain files, round up to the block boundary for I/O efficiency.
        var newSize = _cryptoProvider != null && _cryptoFileHeaderSize > 0
            ? requiredOffset + _physicalPageSize
            : AlignToBlock(requiredOffset + _physicalPageSize);

        _fileStream.SetLength(newSize);
        _mappedFile!.Dispose();
        _mappedFile = MemoryMappedFile.CreateFromFile(
            _fileStream,
            null,
            _fileStream.Length,
            _config.Access,
            HandleInheritability.None,
            leaveOpen: true);

        // Recreate the persistent read accessor so it covers the newly grown region.
        _readAccessor?.Dispose();
        _readAccessor = _mappedFile.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read);
    }

    // ── Public page I/O ────────────────────────────────────────────────────

    /// <summary>
    /// Reads a page by ID into the provided span (synchronous).
    /// Acquires a shared read lock so multiple threads can read concurrently,
    /// while being safely excluded from any concurrent file-resize operation.
    /// </summary>
    public void ReadPage(uint pageId, Span<byte> destination)
    {
        ThrowIfDisposed();
        if (destination.Length < _config.PageSize)
            throw new ArgumentException($"Destination must be at least {_config.PageSize} bytes");

        if (_mappedFile == null)
            throw new InvalidOperationException("File not open");

        if (!_rwLock.TryEnterReadLock(ReadLockTimeoutMs))
            throw new TimeoutException("Timed out acquiring PageFile read lock (ReadPage).");
        try
        {
            ReadPageCore(pageId, destination);
        }
        finally
        {
            _rwLock.ExitReadLock();
        }
    }

    /// <summary>
    /// Reads up to <paramref name="destination"/>.Length bytes from the start of a page.
    /// Use this to read only the page header without copying the full page payload,
    /// reducing memory traffic compared to a full <see cref="ReadPage"/> call.
    /// <paramref name="destination"/> must not exceed <see cref="PageSize"/> bytes.
    /// </summary>
    public void ReadPageHeader(uint pageId, Span<byte> destination)
    {
        ThrowIfDisposed();
        if (destination.Length > _config.PageSize)
            throw new ArgumentException($"Destination must not exceed {_config.PageSize} bytes");

        if (_mappedFile == null)
            throw new InvalidOperationException("File not open");

        if (!_rwLock.TryEnterReadLock(ReadLockTimeoutMs))
            throw new TimeoutException("Timed out acquiring PageFile read lock (ReadPageHeader).");
        try
        {
            ReadPageHeaderCore(pageId, destination);
        }
        finally
        {
            _rwLock.ExitReadLock();
        }
    }

    /// <summary>
    /// Reads a page by ID into the provided buffer (asynchronous).
    /// Uses the memory-mapped file path (same as <see cref="ReadPage"/>) so that
    /// repeated reads of hot pages are pure in-memory copies from the OS page cache.
    /// The method is non-async and returns a completed <see cref="ValueTask"/> synchronously;
    /// callers see the async signature but pay no state-machine or I/O overhead.
    /// WAL/in-memory paths should be handled by the caller before invoking this method.
    /// </summary>
    /// <param name="pageId">The page to read.</param>
    /// <param name="destination">Buffer of at least <see cref="PageSize"/> bytes.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public ValueTask ReadPageAsync(uint pageId, Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ThrowIfDisposed();
        if (destination.Length < _config.PageSize)
            throw new ArgumentException($"Destination must be at least {_config.PageSize} bytes");

        if (_mappedFile == null)
            throw new InvalidOperationException("File not open");

        if (!_rwLock.TryEnterReadLock(ReadLockTimeoutMs))
            throw new TimeoutException("Timed out acquiring PageFile read lock (ReadPageAsync).");
        try
        {
            ReadPageCore(pageId, destination.Span[.._config.PageSize]);
        }
        finally
        {
            _rwLock.ExitReadLock();
        }

#if NET5_0_OR_GREATER
        return ValueTask.CompletedTask;
#else
        return default;
#endif
    }

    /// <summary>
    /// Writes a page at the specified ID from the provided span.
    /// If the write fits within the current file size, a shared read lock is used
    /// (allowing other concurrent reads and non-growing writes).
    /// If the file must grow, an exclusive write lock is taken to safely dispose
    /// and recreate the memory-mapped file before writing.
    /// </summary>
    public void WritePage(uint pageId, ReadOnlySpan<byte> source)
    {
        ThrowIfDisposed();
        if (source.Length < _config.PageSize)
            throw new ArgumentException($"Source must be at least {_config.PageSize} bytes");

        if (_mappedFile == null)
            throw new InvalidOperationException("File not open");

        var offset = _cryptoFileHeaderSize + (long)pageId * _physicalPageSize;

        // Fast path: file is already large enough — share the mapping with readers.
        if (offset + _physicalPageSize <= _fileStream!.Length)
        {
            if (!_rwLock.TryEnterReadLock(ReadLockTimeoutMs))
                throw new TimeoutException("Timed out acquiring PageFile read lock (WritePage).");
            try
            {
                WritePageCore(pageId, source);
            }
            finally
            {
                _rwLock.ExitReadLock();
            }
            return;
        }

        // Slow path: the file must grow.  Exclusively lock so that no reader
        // can hold a reference to the old _mappedFile while we dispose it.
        if (!_rwLock.TryEnterWriteLock(WriteLockTimeoutMs))
            throw new TimeoutException("Timed out acquiring PageFile write lock (WritePage-grow).");
        try
        {
            // Double-check: another writer may have grown the file already.
            EnsureCapacityCore(offset);
            WritePageCore(pageId, source);
        }
        finally
        {
            _rwLock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Allocates a new page (reuses free page if available) and returns its ID.
    /// Holds the write lock for the duration because it may grow the file and
    /// always modifies <see cref="_nextPageId"/> or <see cref="_firstFreePageId"/>.
    /// </summary>
    public uint AllocatePage()
    {
        ThrowIfDisposed();
        if (!_rwLock.TryEnterWriteLock(WriteLockTimeoutMs))
            throw new TimeoutException("Timed out acquiring PageFile write lock (AllocatePage).");
        try
        {
            if (_fileStream == null)
                throw new InvalidOperationException("File not open");

            // 1. Try to reuse a free page
            if (_firstFreePageId != 0)
            {
                var recycledPageId = _firstFreePageId;
                
                // Read the recycled page to update the free list head
                var buffer = new byte[_config.PageSize];
                ReadPageCore(recycledPageId, buffer);
                var header = PageHeader.ReadFrom(buffer);
                
                // The new head is what the recycled page pointed to
                _firstFreePageId = header.NextPageId;
                
                // UpdateAsync file header (Page 0) to point to new head
                UpdateFileHeaderFreePtrCore(_firstFreePageId);
                
                return recycledPageId;
            }

            // 2. No free pages, append new one
            var pageId = _nextPageId++;
            
            // Extend file if necessary (EnsureCapacityCore replaces the mapping in-place)
            EnsureCapacityCore(_cryptoFileHeaderSize + (long)pageId * _physicalPageSize);
            
            return pageId;
        }
        finally
        {
            _rwLock.ExitWriteLock();
        }
    }
    
    /// <summary>
    /// Marks a page as free and adds it to the free list.
    /// Holds the write lock because it modifies <see cref="_firstFreePageId"/>.
    /// </summary>
    public void FreePage(uint pageId)
    {
        ThrowIfDisposed();
        if (!_rwLock.TryEnterWriteLock(WriteLockTimeoutMs))
            throw new TimeoutException("Timed out acquiring PageFile write lock (FreePage).");
        try
        {
            if (_fileStream == null) throw new InvalidOperationException("File not open");
            if (pageId == 0) throw new InvalidOperationException("Cannot free header page 0");
            
            // 1. Create a free page header pointing to current head
            var header = new PageHeader
            {
                PageId = pageId,
                PageType = PageType.Free,
                NextPageId = _firstFreePageId, // Point to previous head
                TransactionId = 0,
                Checksum = 0
            };
            
            var buffer = new byte[_config.PageSize];
            header.WriteTo(buffer);
            
            // 2. Write the freed page (file already large enough, no growth needed)
            WritePageCore(pageId, buffer);
            
            // 3. UpdateAsync head to point to this page
            _firstFreePageId = pageId;
            
            // 4. UpdateAsync file header (Page 0)
            UpdateFileHeaderFreePtrCore(_firstFreePageId);
        }
        finally
        {
            _rwLock.ExitWriteLock();
        }
    }
    
    // Called only while _rwLock write lock is held.
    private void UpdateFileHeaderFreePtrCore(uint newHead)
    {
        // Read Page 0
        var buffer = new byte[_config.PageSize];
        ReadPageCore(0, buffer);
        var header = PageHeader.ReadFrom(buffer);
        
        // UpdateAsync NextPageId (which we use as FirstFreePageId)
        header.NextPageId = newHead;
        
        // Write back
        header.WriteTo(buffer);
        WritePageCore(0, buffer);
    }

    /// <summary>
    /// Flushes all pending writes to disk.
    /// Called by CheckpointManager after applying WAL changes.
    /// Uses the write lock to serialise with concurrent file-growth operations.
    /// </summary>
    public void Flush()
    {
        ThrowIfDisposed();
        if (!_rwLock.TryEnterWriteLock(WriteLockTimeoutMs))
            throw new TimeoutException("Timed out acquiring PageFile write lock (Flush).");
        try
        {
            _fileStream?.Flush(flushToDisk: true);
        }
        finally
        {
            _rwLock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Async version of <see cref="Flush"/>. Uses <see cref="_asyncLock"/> to
    /// avoid blocking a thread-pool thread while waiting, and to allow the lock
    /// to be held across the <c>await</c> boundary.
    /// </summary>
    public async Task FlushAsync(CancellationToken ct = default)
    {
        if (!await _asyncLock.WaitAsync(WriteLockTimeoutMs, ct).ConfigureAwait(false))
            throw new TimeoutException("Timed out acquiring PageFile async lock (FlushAsync).");
        try
        {
            if (_fileStream != null)
                await _fileStream.FlushAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            _asyncLock.Release();
        }
    }

    /// <summary>
    /// Creates a consistent file-level backup by flushing all dirty pages, then
    /// copying the .db file (and .wal if present) to <paramref name="destinationPath"/>.
    /// <see cref="_asyncLock"/> is held for the duration of the copy so that no
    /// concurrent <see cref="FlushAsync"/> can interleave with the stream positioning.
    /// Resize operations (which hold <see cref="_rwLock"/> write lock) may race with
    /// this method at the PageFile level; callers that require a fully consistent
    /// snapshot must hold the engine-level commit lock before calling this method.
    /// </summary>
    public async Task BackupAsync(string destinationPath, CancellationToken ct = default)
    {
#if NET8_0_OR_GREATER
        ArgumentException.ThrowIfNullOrWhiteSpace(destinationPath);
#else
        if (string.IsNullOrWhiteSpace(destinationPath))
            throw new ArgumentException("The value cannot be null, empty, or whitespace.", nameof(destinationPath));
#endif

        if (!await _asyncLock.WaitAsync(WriteLockTimeoutMs, ct).ConfigureAwait(false))
            throw new TimeoutException("Timed out acquiring PageFile async lock (BackupAsync).");
        try
        {
            if (_fileStream == null)
                throw new InvalidOperationException("PageFile is not open.");

            // 1. Flush dirty pages from MMF to the OS page cache, then to disk.
            _fileStream.Flush(flushToDisk: true);

            // 2. Copy .db file under the lock so no concurrent FlushAsync can race.
            //    Re-use the existing _fileStream handle: _fileStream was opened with
            //    FileShare.None so any attempt to open a second handle (even read-only
            //    with FileShare.ReadWrite) would be rejected by the OS.
            var destDir = Path.GetDirectoryName(destinationPath);
            if (!string.IsNullOrEmpty(destDir))
                Directory.CreateDirectory(destDir);

            var savedPosition = _fileStream.Position;
            _fileStream.Position = 0;

            await using var dst = new FileStream(
                destinationPath, FileMode.Create, FileAccess.Write, FileShare.None,
                bufferSize: 1024 * 1024, FileOptions.Asynchronous);

            await _fileStream.CopyToAsync(dst, ct).ConfigureAwait(false);
            await dst.FlushAsync(ct).ConfigureAwait(false);

            _fileStream.Position = savedPosition;
        }
        finally
        {
            _asyncLock.Release();
        }
    }

    /// <summary>
    /// Scans trailing pages in the file for free pages and truncates the file to remove them,
    /// shrinking it to the minimum required size.
    /// Acquires <see cref="_asyncLock"/> then <see cref="_rwLock"/> write lock, flushes
    /// dirty pages, unmaps, truncates, and remaps the memory-mapped file.
    /// </summary>
    public async Task TruncateToMinimumAsync(CancellationToken ct = default)
    {
        ThrowIfDisposed();
        if (!await _asyncLock.WaitAsync(WriteLockTimeoutMs, ct).ConfigureAwait(false))
            throw new TimeoutException("Timed out acquiring PageFile async lock (TruncateToMinimumAsync).");
        try
        {
            if (!_rwLock.TryEnterWriteLock(WriteLockTimeoutMs))
                throw new TimeoutException("Timed out acquiring PageFile write lock (TruncateToMinimumAsync).");
            try
            {
                if (_fileStream == null || _mappedFile == null)
                    return;

                // 1. Flush dirty pages to disk.
                _fileStream.Flush(flushToDisk: true);

                // 2. Walk backwards from the last page to find the last non-free page.
                //    Pages 0 (header) and 1 (collection metadata) are always kept.
                //    ReadPageHeaderCore copies only destination.Length bytes, which
                //    must be <= PageSize but can be as small as the header itself.
                var headerBuf = new byte[32]; // PageHeader layout size is 32 bytes
                uint lastUsedPage = 1; // minimum: keep pages 0 and 1
                for (uint p = _nextPageId - 1; p > 1; p--)
                {
                    ReadPageHeaderCore(p, headerBuf);
                    var hdr = PageHeader.ReadFrom(headerBuf);
                    if (hdr.PageType != PageType.Free)
                    {
                        lastUsedPage = p;
                        break;
                    }
                }

                var newLength = _cryptoFileHeaderSize + ((long)lastUsedPage + 1) * _physicalPageSize;
                if (newLength >= _fileStream.Length)
                    return; // Nothing to truncate

                // 3. Rebuild the free list excluding pages that will be truncated away.
                var newPageCount = (uint)((newLength - _cryptoFileHeaderSize) / _physicalPageSize);
                var validFreePages = new List<uint>();
                uint freeId = _firstFreePageId;
                var seen = new System.Collections.Generic.HashSet<uint>();
                while (freeId != 0 && seen.Add(freeId))
                {
                    if (freeId < newPageCount)
                        validFreePages.Add(freeId);
                    ReadPageHeaderCore(freeId, headerBuf);
                    var freeHdr = PageHeader.ReadFrom(headerBuf);
                    freeId = freeHdr.NextPageId;
                }

                // 4. Unmap, truncate, remap.
                _readAccessor?.Dispose();
                _readAccessor = null;
                _mappedFile.Dispose();
                _mappedFile = null;

                _fileStream.SetLength(newLength);
                _nextPageId = newPageCount;

                _mappedFile = MemoryMappedFile.CreateFromFile(
                    _fileStream,
                    null,
                    newLength,
                    _config.Access,
                    HandleInheritability.None,
                    leaveOpen: true);
                _readAccessor = _mappedFile.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read);

                // 5. Rewrite the free list chain with only valid (retained) entries.
                _firstFreePageId = 0;
                if (validFreePages.Count > 0)
                {
                    var pageBuf = new byte[_config.PageSize];
                    for (int i = validFreePages.Count - 1; i >= 0; i--)
                    {
                        var pid = validFreePages[i];
                        ReadPageCore(pid, pageBuf);
                        var freePageHdr = PageHeader.ReadFrom(pageBuf);
                        freePageHdr.NextPageId = _firstFreePageId;
                        freePageHdr.WriteTo(pageBuf);
                        WritePageCore(pid, pageBuf);
                        _firstFreePageId = pid;
                    }
                }
                UpdateFileHeaderFreePtrCore(_firstFreePageId);
                _fileStream.Flush(flushToDisk: true);
            }
            finally
            {
                _rwLock.ExitWriteLock();
            }
        }
        finally
        {
            _asyncLock.Release();
        }
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        // Acquire _asyncLock first to drain any in-flight FlushAsync / BackupAsync,
        // then acquire _rwLock write lock to block all concurrent reads and writes.
        // Lock order (_asyncLock before _rwLock) must be respected everywhere.
        // Best-effort: if locks cannot be acquired, proceed anyway so Dispose() never throws.
        bool asyncLockAcquired = _asyncLock.Wait(WriteLockTimeoutMs);
        bool rwLockAcquired = _rwLock.TryEnterWriteLock(WriteLockTimeoutMs);
        try
        {
            if (_disposed)
                return;

            // 1. Flush any pending writes from memory-mapped file
            if (_fileStream != null)
            {
                _fileStream.Flush(flushToDisk: true);
            }
            
            // 2. Close memory-mapped file first (and the persistent read accessor that depends on it)
            _readAccessor?.Dispose();
            _readAccessor = null;
            _mappedFile?.Dispose();
            _mappedFile = null;
            
            // 3. Then close file stream
            _fileStream?.Dispose();
            _fileStream = null;

            // 4. Dispose crypto provider (clears key material from memory).
            if (_cryptoProvider is IDisposable disposableCrypto)
                try { disposableCrypto.Dispose(); } catch { /* best-effort */ }

            _disposed = true;
        }
        finally
        {
            if (rwLockAcquired)
                _rwLock.ExitWriteLock();

            // Dispose only if we hold the write lock; if another thread still holds
            // a read/write lock, disposing ReaderWriterLockSlim would produce undefined
            // behaviour or SynchronizationLockException — violating "Dispose never throws".
            if (rwLockAcquired)
            {
                try { _rwLock.Dispose(); } catch { /* best-effort */ }
            }

            if (asyncLockAcquired)
            {
                _asyncLock.Release();
                try { _asyncLock.Dispose(); } catch { /* best-effort */ }
            }
        }

        GC.SuppressFinalize(this);
    }
}
