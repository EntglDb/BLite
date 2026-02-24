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
    private readonly SemaphoreSlim _lock = new(1, 1);
    private bool _disposed;
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
        _lock.Wait();
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
                FileOptions.RandomAccess | FileOptions.Asynchronous);

            if (!fileExists || _fileStream.Length == 0)
            {
                // Allocate exactly enough for Header + Collection Metadata,
                // rounded up to the nearest growth block.
                _fileStream.SetLength(AlignToBlock((long)_config.PageSize * 2));
                InitializeHeader();
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
            _lock.Release();
        }
    }

    /// <summary>
    /// Initializes the file header (page 0) and collection metadata (page 1)
    /// </summary>
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
            Checksum = 0
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

    // ... (ReadPage / WritePage unchanged) ...
    /// <summary>
    /// Reads a page by ID into the provided span (synchronous).
    /// </summary>
    public void ReadPage(uint pageId, Span<byte> destination)
    {
        if (destination.Length < _config.PageSize)
            throw new ArgumentException($"Destination must be at least {_config.PageSize} bytes");

        if (_mappedFile == null)
            throw new InvalidOperationException("File not open");

        var offset = (long)pageId * _config.PageSize;
        
        using var accessor = _mappedFile.CreateViewAccessor(offset, _config.PageSize, MemoryMappedFileAccess.Read);
        var temp = new byte[_config.PageSize];
        accessor.ReadArray(0, temp, 0, _config.PageSize);
        temp.CopyTo(destination);
    }

    /// <summary>
    /// Reads a page by ID into the provided buffer (asynchronous).
    /// Uses <see cref="RandomAccess.ReadAsync"/> for true OS-level async I/O (IOCP on Windows).
    /// WAL/in-memory paths should be handled by the caller before invoking this method.
    /// </summary>
    /// <param name="pageId">The page to read.</param>
    /// <param name="destination">Buffer of at least <see cref="PageSize"/> bytes.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async ValueTask ReadPageAsync(uint pageId, Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        if (destination.Length < _config.PageSize)
            throw new ArgumentException($"Destination must be at least {_config.PageSize} bytes");

        if (_fileStream == null)
            throw new InvalidOperationException("File not open");

        var offset = (long)pageId * _config.PageSize;
        var slice = destination[.._config.PageSize];

        var bytesRead = await RandomAccess.ReadAsync(_fileStream.SafeFileHandle, slice, offset, cancellationToken).ConfigureAwait(false);

        if (bytesRead < _config.PageSize)
            throw new IOException($"Incomplete page read: expected {_config.PageSize} bytes, got {bytesRead} (pageId={pageId})");
    }

    /// <summary>
    /// Writes a page at the specified ID from the provided span
    /// </summary>
    public void WritePage(uint pageId, ReadOnlySpan<byte> source)
    {
        if (source.Length < _config.PageSize)
            throw new ArgumentException($"Source must be at least {_config.PageSize} bytes");

        if (_mappedFile == null)
            throw new InvalidOperationException("File not open");

        var offset = (long)pageId * _config.PageSize;
        
        // Ensure file is large enough
        if (offset + _config.PageSize > _fileStream!.Length)
        {
            _lock.Wait();
            try
            {
                if (offset + _config.PageSize > _fileStream.Length)
                {
                    var newSize = AlignToBlock(offset + _config.PageSize);
                    _fileStream.SetLength(newSize);

                    // Recreate memory-mapped file with new size
                    _mappedFile.Dispose();
                    _mappedFile = MemoryMappedFile.CreateFromFile(
                        _fileStream,
                        null,
                        _fileStream.Length,
                        _config.Access,
                        HandleInheritability.None,
                        leaveOpen: true);
                }
            }
            finally
            {
                _lock.Release();
            }
        }

        // Write to memory-mapped file
        using (var accessor = _mappedFile.CreateViewAccessor(offset, _config.PageSize, MemoryMappedFileAccess.Write))
        {
            accessor.WriteArray(0, source.ToArray(), 0, _config.PageSize);
        }
    }

    /// <summary>
    /// Allocates a new page (reuses free page if available) and returns its ID
    /// </summary>
    public uint AllocatePage()
    {
        _lock.Wait();
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
                ReadPage(recycledPageId, buffer);
                var header = PageHeader.ReadFrom(buffer);
                
                // The new head is what the recycled page pointed to
                _firstFreePageId = header.NextPageId;
                
                // Update file header (Page 0) to point to new head
                UpdateFileHeaderFreePtr(_firstFreePageId);
                
                return recycledPageId;
            }

            // 2. No free pages, append new one
            var pageId = _nextPageId++;
            
            // Extend file if necessary
            var requiredLength = (long)(pageId + 1) * _config.PageSize;
            if (requiredLength > _fileStream.Length)
            {
                var newSize = AlignToBlock(requiredLength);
                _fileStream.SetLength(newSize);
                
                // Recreate memory-mapped file with new size
                _mappedFile?.Dispose();
                _mappedFile = MemoryMappedFile.CreateFromFile(
                    _fileStream,
                    null,
                    _fileStream.Length,
                    _config.Access,
                    HandleInheritability.None,
                    leaveOpen: true);
            }
            
            return pageId;
        }
        finally
        {
            _lock.Release();
        }
    }
    
    /// <summary>
    /// Marks a page as free and adds it to the free list
    /// </summary>
    public void FreePage(uint pageId)
    {
        _lock.Wait();
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
            
            // 2. Write the freed page
            WritePage(pageId, buffer);
            
            // 3. Update head to point to this page
            _firstFreePageId = pageId;
            
            // 4. Update file header (Page 0)
            UpdateFileHeaderFreePtr(_firstFreePageId);
        }
        finally
        {
            _lock.Release();
        }
    }
    
    private void UpdateFileHeaderFreePtr(uint newHead)
    {
        // Read Page 0
        var buffer = new byte[_config.PageSize];
        ReadPage(0, buffer);
        var header = PageHeader.ReadFrom(buffer);
        
        // Update NextPageId (which we use as FirstFreePageId)
        header.NextPageId = newHead;
        
        // Write back
        header.WriteTo(buffer);
        WritePage(0, buffer);
    }

    /// <summary>
    /// Flushes all pending writes to disk.
    /// Called by CheckpointManager after applying WAL changes.
    /// </summary>
    public void Flush()
    {
        _lock.Wait();
        try
        {
            _fileStream?.Flush(flushToDisk: true);
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    /// Async version of <see cref="Flush"/>. Uses <see cref="SemaphoreSlim.WaitAsync"/> to
    /// avoid blocking a thread-pool thread while waiting.
    /// </summary>
    public async Task FlushAsync(CancellationToken ct = default)
    {
        await _lock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_fileStream != null)
                await _fileStream.FlushAsync(ct).ConfigureAwait(false);
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    /// Creates a consistent file-level backup by flushing all dirty pages, then
    /// copying the .db file (and .wal if present) to <paramref name="destinationPath"/>.
    /// The lock is held for the duration of the copy so no resize or flush can
    /// interleave. Reads are opened with <see cref="FileShare.ReadWrite"/> so the
    /// engine does not need to be stopped.
    /// </summary>
    public async Task BackupAsync(string destinationPath, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(destinationPath);

        await _lock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            if (_fileStream == null)
                throw new InvalidOperationException("PageFile is not open.");

            // 1. Flush dirty pages from MMF to the OS page cache, then to disk.
            _fileStream.Flush(flushToDisk: true);

            // 2. Copy .db file under the lock so no resize can race.
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
            _lock.Release();
        }
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _lock.Wait();
        try
        {
            // 1. Flush any pending writes from memory-mapped file
            if (_fileStream != null)
            {
                _fileStream.Flush(flushToDisk: true);
            }
            
            // 2. Close memory-mapped file first
            _mappedFile?.Dispose();
            
            // 3. Then close file stream
            _fileStream?.Dispose();
            
            _disposed = true;
        }
        finally
        {
            _lock.Release();
            _lock.Dispose();
        }

        GC.SuppressFinalize(this);
    }
}
