using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace BLite.Core.Storage;

/// <summary>
/// In-memory page storage backend. Stores all pages in a <see cref="ConcurrentDictionary{TKey,TValue}"/>
/// with no file-system dependencies, making it suitable for:
/// <list type="bullet">
///   <item>Ephemeral (non-persistent) embedded databases</item>
///   <item>Unit and integration tests that must not touch the file system</item>
///   <item>Browser-hosted .NET WASM applications (as a foundation before a full IndexedDB/OPFS backend)</item>
/// </list>
/// <para>
/// Create an in-memory <see cref="BLiteEngine"/> via <see cref="BLiteEngine.CreateInMemory"/>.
/// </para>
/// </summary>
public sealed class MemoryPageStorage : IPageStorage
{
    private readonly int _pageSize;
    private readonly ConcurrentDictionary<uint, byte[]> _pages = new();
    private readonly Stack<uint> _freeList = new();
    private readonly object _allocationLock = new();
    private uint _nextPageId;
    private bool _disposed;

    /// <summary>
    /// Initialises a new in-memory page storage with the given page size.
    /// Call <see cref="Open"/> to initialise the header pages before use.
    /// </summary>
    /// <param name="pageSize">Fixed size in bytes of every page.</param>
    public MemoryPageStorage(int pageSize)
    {
        if (pageSize < 512)
            throw new ArgumentOutOfRangeException(nameof(pageSize), "Page size must be at least 512 bytes.");
        _pageSize = pageSize;
    }

    /// <inheritdoc/>
    public int PageSize => _pageSize;

    /// <inheritdoc/>
    public uint NextPageId => _nextPageId;

    /// <summary>
    /// Initialises page 0 (file header) and page 1 (collection metadata) if they have not
    /// already been written.  Subsequent calls are no-ops.
    /// </summary>
    public void Open()
    {
        if (_nextPageId > 0)
            return; // Already open

        // Page 0: File Header
        var headerPage = new byte[_pageSize];
        var header = new PageHeader
        {
            PageId = 0,
            PageType = PageType.Header,
            FreeBytes = (ushort)(_pageSize - 32),
            NextPageId = 0,
            TransactionId = 0,
            Checksum = 0,
            FormatVersion = PageHeader.CurrentFormatVersion,
            DictionaryRootPageId = 0,
            KvRootPageId = 0
        };
        header.WriteTo(headerPage);
        _pages[0] = headerPage;

        // Page 1: Collection Metadata (slotted page)
        var metaPage = new byte[_pageSize];
        var metaHeader = new SlottedPageHeader
        {
            PageId = 1,
            PageType = PageType.Collection,
            SlotCount = 0,
            FreeSpaceStart = SlottedPageHeader.Size,
            FreeSpaceEnd = (ushort)_pageSize,
            NextOverflowPage = 0,
            TransactionId = 0
        };
        metaHeader.WriteTo(metaPage);
        _pages[1] = metaPage;

        _nextPageId = 2;
    }

    /// <inheritdoc/>
    public void ReadPage(uint pageId, Span<byte> destination)
    {
        ThrowIfDisposed();
        if (destination.Length < _pageSize)
            throw new ArgumentException($"Destination must be at least {_pageSize} bytes.");

        if (_pages.TryGetValue(pageId, out var page))
            page.AsSpan(0, _pageSize).CopyTo(destination);
        else
            destination.Slice(0, _pageSize).Clear(); // Return zeroes for uninitialized pages
    }

    /// <inheritdoc/>
    public void ReadPageHeader(uint pageId, Span<byte> destination)
    {
        ThrowIfDisposed();
        if (destination.Length > _pageSize)
            throw new ArgumentException($"Destination must not exceed {_pageSize} bytes.");

        if (_pages.TryGetValue(pageId, out var page))
            page.AsSpan(0, destination.Length).CopyTo(destination);
        else
            destination.Clear();
    }

    /// <inheritdoc/>
    public ValueTask ReadPageAsync(uint pageId, Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ReadPage(pageId, destination.Span.Slice(0, _pageSize));
#if NET5_0_OR_GREATER
        return ValueTask.CompletedTask;
#else
        return default;
#endif
    }

    /// <inheritdoc/>
    public void WritePage(uint pageId, ReadOnlySpan<byte> source)
    {
        ThrowIfDisposed();
        if (source.Length < _pageSize)
            throw new ArgumentException($"Source must be at least {_pageSize} bytes.");

        // Reuse the existing buffer if one already exists for this page.
        if (_pages.TryGetValue(pageId, out var existing))
        {
            source.Slice(0, _pageSize).CopyTo(existing);
        }
        else
        {
            var copy = new byte[_pageSize];
            source.Slice(0, _pageSize).CopyTo(copy);
            _pages[pageId] = copy;
        }
    }

    /// <inheritdoc/>
    public uint AllocatePage()
    {
        ThrowIfDisposed();
        lock (_allocationLock)
        {
            if (_freeList.Count > 0)
                return _freeList.Pop();

            return _nextPageId++;
        }
    }

    /// <inheritdoc/>
    public void FreePage(uint pageId)
    {
        ThrowIfDisposed();
        if (pageId == 0)
            throw new InvalidOperationException("Cannot free the header page (page 0).");

        lock (_allocationLock)
        {
            _freeList.Push(pageId);
        }
    }

    /// <summary>No-op: in-memory storage has no durable destination to flush to.</summary>
    public void Flush() { }

    /// <summary>No-op: in-memory storage has no durable destination to flush to.</summary>
    public Task FlushAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    /// <inheritdoc/>
    /// <exception cref="NotSupportedException">
    /// Always thrown — in-memory storage cannot be backed up to a file path.
    /// Serialise the data layer and reload it if persistence is required.
    /// </exception>
    public Task BackupAsync(string destinationPath, CancellationToken cancellationToken = default)
        => throw new NotSupportedException(
            "In-memory storage does not support file-based backup. " +
            "Use a file-based PageFile backend if backup is required.");

    /// <inheritdoc/>
    /// <remarks>In-memory storage has no file to truncate; this method is a no-op.</remarks>
    public Task TruncateToMinimumAsync(CancellationToken cancellationToken = default)
        => Task.CompletedTask;

    /// <inheritdoc/>
    public void Dispose()
    {
        _disposed = true;
        _pages.Clear();
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(MemoryPageStorage));
    }
}
