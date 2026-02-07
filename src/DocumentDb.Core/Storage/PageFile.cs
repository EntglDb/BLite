using System.IO.MemoryMappedFiles;

namespace DocumentDb.Core.Storage;

/// <summary>
/// Configuration for page-based storage
/// </summary>
public readonly struct PageFileConfig
{
    public int PageSize { get; init; }
    public long InitialFileSize { get; init; }
    public MemoryMappedFileAccess Access { get; init; }

    public static PageFileConfig Default => new()
    {
        PageSize = 8192, // 8KB pages
        InitialFileSize = 1024 * 1024, // 1MB initial size
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
    private readonly object _lock = new();
    private bool _disposed;
    private uint _nextPageId;

    public PageFile(string filePath, PageFileConfig config)
    {
        _filePath = filePath ?? throw new ArgumentNullException(nameof(filePath));
        _config = config;
    }

    public int PageSize => _config.PageSize;

    /// <summary>
    /// Opens the page file, creating it if it doesn't exist
    /// </summary>
    public void Open()
    {
        lock (_lock)
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
                FileOptions.RandomAccess);

            if (!fileExists || _fileStream.Length == 0)
            {
                // Initialize new file
                _fileStream.SetLength(_config.InitialFileSize);
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
        }
    }

    /// <summary>
    /// Initializes the file header (page 0)
    /// </summary>
    private void InitializeHeader()
    {
        var header = new PageHeader
        {
            PageId = 0,
            PageType = PageType.Header,
            FreeBytes = (ushort)(_config.PageSize - 32),
            NextPageId = 0,
            TransactionId = 0,
            Checksum = 0
        };

        Span<byte> headerPage = stackalloc byte[_config.PageSize];
        header.WriteTo(headerPage);

        _fileStream!.Position = 0;
        _fileStream.Write(headerPage);
        _fileStream.Flush();
    }

    /// <summary>
    /// Reads a page by ID into the provided span
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
            lock (_lock)
            {
                if (offset + _config.PageSize > _fileStream.Length)
                {
                    var newSize = (_fileStream.Length + _config.PageSize) * 2;
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
        }

        using var accessor = _mappedFile.CreateViewAccessor(offset, _config.PageSize, MemoryMappedFileAccess.Write);
        accessor.WriteArray(0, source.ToArray(), 0, _config.PageSize);
    }

    /// <summary>
    /// Allocates a new page and returns its ID
    /// </summary>
    public uint AllocatePage()
    {
        lock (_lock)
        {
            if (_fileStream == null)
                throw new InvalidOperationException("File not open");

            var pageId = _nextPageId++;
            
            // Extend file if necessary
            var requiredLength = (long)(pageId + 1) * _config.PageSize;
            if (requiredLength > _fileStream.Length)
            {
                var newSize = Math.Max(requiredLength, _fileStream.Length * 2);
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
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        lock (_lock)
        {
            _mappedFile?.Dispose();
            _fileStream?.Dispose();
            _disposed = true;
        }

        GC.SuppressFinalize(this);
    }
}
