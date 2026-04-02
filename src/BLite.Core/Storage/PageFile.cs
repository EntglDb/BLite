using System.IO.MemoryMappedFiles;

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
    /// Small pages for embedded scenarios with many tiny documents
    /// </summary>
    public static PageFileConfig Small => new()
    {
        PageSize = 8192,          // 8KB pages
        GrowthBlockSize = 512 * 1024,  // grow by 512KB at a time
        Access = MemoryMappedFileAccess.ReadWrite
    };

    /// <summary>
    /// Default balanced configuration for document databases (16KB like MySQL InnoDB)
    /// </summary>
    public static PageFileConfig Default => new()
    {
        PageSize = 16384,         // 16KB pages
        GrowthBlockSize = 1024 * 1024, // grow by 1MB at a time
        Access = MemoryMappedFileAccess.ReadWrite
    };

    /// <summary>
    /// Large pages for databases with big documents (32KB like MongoDB WiredTiger)
    /// </summary>
    public static PageFileConfig Large => new()
    {
        PageSize = 32768,              // 32KB pages
        GrowthBlockSize = 2 * 1024 * 1024, // grow by 2MB at a time
        Access = MemoryMappedFileAccess.ReadWrite
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
                Access = MemoryMappedFileAccess.ReadWrite
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
/// </summary>
public sealed class PageFile : IDisposable
{
    private readonly string _filePath;
    private readonly PageFileConfig _config;
    private FileStream? _fileStream;
    private MemoryMappedFile? _mappedFile;
    // Persistent full-file read accessor — created once on Open(), recreated on file growth.
    // Enables zero-alloc page reads via unsafe pointer instead of
    // CreateViewAccessor-per-read (which allocates an object + 16 KB temp array each time).
    private MemoryMappedViewAccessor? _readAccessor;

    // _rwLock guards _mappedFile, _nextPageId, and _firstFreePageId.
    // Read lock:  ReadPage(), WritePage() when no file growth is needed.
    // Write lock: Open(), AllocatePage(), FreePage(), WritePage() when file grows,
    //             Flush(), Dispose() — any operation that recreates _mappedFile or
    //             modifies structural state.
    // ReaderWriterLockSlim allows many concurrent readers while serialising writers,
    // eliminating the race where a concurrent resize (write lock) disposes _mappedFile
    // while ReadPage() (read lock) is creating a view accessor from it.
    private readonly ReaderWriterLockSlim _rwLock = new(LockRecursionPolicy.NoRecursion);

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
        _rwLock.EnterWriteLock();
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

            if (!fileExists || _fileStream.Length == 0)
            {
                // Allocate exactly enough for Header + Collection Metadata,
                // rounded up to the nearest growth block.
                _fileStream.SetLength(AlignToBlock((long)_config.PageSize * 2));
                InitializeHeader();
            }
            else if (_fileStream.Length >= 32)
            {
                // Validate page size matches the existing file
                Span<byte> probe = stackalloc byte[32];
#if NET6_0_OR_GREATER
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

                //skip for now
                //if (fileHeader.FormatVersion < PageHeader.CurrentFormatVersion)
                    //throw new InvalidOperationException(
                    //    $"Database format version {fileHeader.FormatVersion} is not supported. "
                    //  + $"This build requires format version {PageHeader.CurrentFormatVersion}. "
                    //  + "The database was created with an older version of BLite that stored integer "
                    //  + "index keys in little-endian order. Please re-create the database.");
            }

            // Initialize next page ID based on file length
            _nextPageId = (uint)(_fileStream.Length / _config.PageSize);

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

            // Read free list head from Page 0
            if (_fileStream.Length >= _config.PageSize)
            {
                var headerSpan = new byte[32]; // PageHeader.Size
                using var accessor = _mappedFile.CreateViewAccessor(0, 32, MemoryMappedFileAccess.Read);
                accessor.ReadArray(0, headerSpan, 0, 32);
                var header = PageHeader.ReadFrom(headerSpan);
                _firstFreePageId = header.NextPageId;
            }
        }
        finally
        {
            _rwLock.ExitWriteLock();
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

        Span<byte> buffer = stackalloc byte[_config.PageSize];
        header.WriteTo(buffer);

        _fileStream!.Position = 0;
        _fileStream.Write(buffer);

        // 2. Initialize Collection Metadata (Page 1)
        // This page is reserved for storing index definitions
        buffer.Clear();
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

        _fileStream.Position = _config.PageSize;
        _fileStream.Write(buffer);
        
        _fileStream.Flush();
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
        // Zero-alloc fast path: acquire the base pointer of the persistent full-file
        // accessor and copy exactly one page.  One memcpy, no heap allocation.
        var offset = (long)pageId * _config.PageSize;
        byte* basePtr = null;
        _readAccessor!.SafeMemoryMappedViewHandle.AcquirePointer(ref basePtr);
        try
        {
            new ReadOnlySpan<byte>(basePtr + offset, _config.PageSize).CopyTo(destination);
        }
        finally
        {
            _readAccessor.SafeMemoryMappedViewHandle.ReleasePointer();
        }
    }

    private void WritePageCore(uint pageId, ReadOnlySpan<byte> source)
    {
        var offset = (long)pageId * _config.PageSize;
        using var accessor = _mappedFile!.CreateViewAccessor(offset, _config.PageSize, MemoryMappedFileAccess.Write);
        accessor.WriteArray(0, source.ToArray(), 0, _config.PageSize);
        // FlushViewOfFile is required to guarantee that writes made through the
        // memory-mapped view are visible to subsequent ReadFile / RandomAccess.ReadAsync
        // calls on the same handle. Without it, the OS does not guarantee coherency
        // between the mapped-view pages and the buffered-file-I/O view.
        accessor.Flush();
    }

    // ── Grow-file helper ───────────────────────────────────────────────────
    // Must be called under _rwLock write lock. Extends the file and recreates
    // _mappedFile when the requested offset does not fit in the current mapping.
    private void EnsureCapacityCore(long requiredOffset)
    {
        if (requiredOffset + _config.PageSize <= _fileStream!.Length)
            return;

        var newSize = AlignToBlock(requiredOffset + _config.PageSize);
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

        _rwLock.EnterReadLock();
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

        _rwLock.EnterReadLock();
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

        var offset = (long)pageId * _config.PageSize;

        // Fast path: file is already large enough — share the mapping with readers.
        if (offset + _config.PageSize <= _fileStream!.Length)
        {
            _rwLock.EnterReadLock();
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
        _rwLock.EnterWriteLock();
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
        _rwLock.EnterWriteLock();
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
            EnsureCapacityCore((long)pageId * _config.PageSize);
            
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
        _rwLock.EnterWriteLock();
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
        _rwLock.EnterWriteLock();
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
        await _asyncLock.WaitAsync(ct).ConfigureAwait(false);
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

        await _asyncLock.WaitAsync(ct).ConfigureAwait(false);
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

    public void Dispose()
    {
        if (_disposed)
            return;

        // Acquire _asyncLock first to drain any in-flight FlushAsync / BackupAsync,
        // then acquire _rwLock write lock to block all concurrent reads and writes.
        // Lock order (_asyncLock before _rwLock) must be respected everywhere.
        _asyncLock.Wait();
        _rwLock.EnterWriteLock();
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
            
            _disposed = true;
        }
        finally
        {
            _rwLock.ExitWriteLock();
            _rwLock.Dispose();
            _asyncLock.Release();
            _asyncLock.Dispose();
        }

        GC.SuppressFinalize(this);
    }
}
