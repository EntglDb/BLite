using System;
using System.Threading;
using System.Threading.Tasks;
using BLite.Core;
using BLite.Core.Storage;
using BLite.Wasm.Interop;

namespace BLite.Wasm.Storage;

/// <summary>
/// IndexedDB page storage backend for browser WASM.
/// <para>
/// Each page is stored as a <c>Uint8Array</c> blob in an IndexedDB object store,
/// keyed by <c>(databaseName, pageId)</c>. This provides universal browser support
/// (all modern browsers including Safari 7+) with persistent, cross-session storage.
/// </para>
/// <para>
/// Throughput is lower than <see cref="OpfsPageStorage"/> but IndexedDB works on the
/// main thread (no Worker requirement), making it the safest choice for maximum
/// compatibility in Blazor WASM contexts.
/// </para>
/// <para>
/// Binary data is exchanged with JavaScript as base64 strings because
/// <c>[JSImport]</c> does not support <c>byte[]</c> on async (<c>Task</c>-returning)
/// methods.
/// </para>
/// </summary>
public sealed class IndexedDbPageStorage : IPageStorage
{
    private readonly string _dbName;
    private readonly int _pageSize;
    private readonly Stack<uint> _freeList = new();
    private readonly object _allocationLock = new();
    private uint _nextPageId;
    private bool _opened;
    private bool _disposed;

    /// <summary>
    /// Initialises a new IndexedDB page storage for the given database name.
    /// Call <see cref="OpenAsync"/> to initialise before use.
    /// </summary>
    /// <param name="dbName">Logical database name (becomes the IndexedDB database name prefix).</param>
    /// <param name="pageSize">Fixed page size in bytes (minimum 512).</param>
    public IndexedDbPageStorage(string dbName, int pageSize)
    {
        if (string.IsNullOrWhiteSpace(dbName))
            throw new ArgumentException("Database name must not be null or empty.", nameof(dbName));
        if (pageSize < 512)
            throw new ArgumentOutOfRangeException(nameof(pageSize), "Page size must be at least 512 bytes.");

        _dbName = dbName;
        _pageSize = pageSize;
    }

    /// <inheritdoc/>
    public int PageSize => _pageSize;

    /// <inheritdoc/>
    public uint NextPageId => _nextPageId;

    /// <summary>Returns <c>true</c> if IndexedDB is available in the current browser context.</summary>
    public static bool IsAvailable() => IndexedDbInterop.IsAvailable();

    /// <summary>
    /// Opens the IndexedDB database asynchronously. Must be called once before any I/O.
    /// If the database already exists, the next-page-id counter is restored.
    /// Otherwise, header pages are initialised.
    /// </summary>
    public async Task OpenAsync()
    {
        ThrowIfDisposed();
        if (_opened)
            return;

        await IndexedDbInterop.EnsureLoadedAsync();
        var storedNextPageId = (uint)await IndexedDbInterop.OpenAsync(_dbName);

        if (storedNextPageId >= 2)
        {
            // Existing database — restore counter.
            _nextPageId = storedNextPageId;
        }
        else
        {
            // Fresh database — write header pages.
            await InitializeHeaderPagesAsync();
        }

        _opened = true;
    }

    /// <summary>
    /// Synchronous <see cref="IPageStorage.Open"/> — calls <see cref="OpenAsync"/> blocking.
    /// In WASM single-threaded contexts this relies on cooperative scheduling.
    /// Prefer <see cref="OpenAsync"/> when possible.
    /// </summary>
    public void Open()
    {
        OpenAsync().GetAwaiter().GetResult();
    }

    /// <inheritdoc/>
    public void ReadPage(uint pageId, Span<byte> destination)
    {
        ThrowIfDisposed();
        ThrowIfNotOpened();

        if (destination.Length < _pageSize)
            throw new ArgumentException($"Destination must be at least {_pageSize} bytes.");

        // IndexedDB is inherently async; block on the Task in the WASM single-threaded scheduler.
        var base64 = IndexedDbInterop.ReadPageAsync(_dbName, (int)pageId, _pageSize).GetAwaiter().GetResult();
        var data = Convert.FromBase64String(base64);
        data.AsSpan(0, Math.Min(data.Length, _pageSize)).CopyTo(destination);
    }

    /// <inheritdoc/>
    public void ReadPageHeader(uint pageId, Span<byte> destination)
    {
        ThrowIfDisposed();
        ThrowIfNotOpened();

        if (destination.Length > _pageSize)
            throw new ArgumentException($"Destination must not exceed {_pageSize} bytes.");

        var base64 = IndexedDbInterop.ReadPageAsync(_dbName, (int)pageId, _pageSize).GetAwaiter().GetResult();
        var data = Convert.FromBase64String(base64);
        data.AsSpan(0, Math.Min(data.Length, destination.Length)).CopyTo(destination);
    }

    /// <inheritdoc/>
    public async ValueTask ReadPageAsync(uint pageId, Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ThrowIfDisposed();
        ThrowIfNotOpened();

        var base64 = await IndexedDbInterop.ReadPageAsync(_dbName, (int)pageId, _pageSize);
        var data = Convert.FromBase64String(base64);
        data.AsSpan(0, Math.Min(data.Length, _pageSize)).CopyTo(destination.Span);
    }

    /// <inheritdoc/>
    public void WritePage(uint pageId, ReadOnlySpan<byte> source)
    {
        ThrowIfDisposed();
        ThrowIfNotOpened();

        if (source.Length < _pageSize)
            throw new ArgumentException($"Source must be at least {_pageSize} bytes.");

        var buffer = new byte[_pageSize];
        source.Slice(0, _pageSize).CopyTo(buffer);
        var base64 = Convert.ToBase64String(buffer);
        IndexedDbInterop.WritePageAsync(_dbName, (int)pageId, base64).GetAwaiter().GetResult();
    }

    /// <inheritdoc/>
    public uint AllocatePage()
    {
        ThrowIfDisposed();
        ThrowIfNotOpened();

        lock (_allocationLock)
        {
            if (_freeList.Count > 0)
                return _freeList.Pop();

            var id = _nextPageId++;
            // Persist the counter so it survives browser restarts.
            IndexedDbInterop.SaveNextPageIdAsync(_dbName, (int)_nextPageId).GetAwaiter().GetResult();
            return id;
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

    /// <summary>No-op: IndexedDB writes are transactionally durable once the IDB transaction completes.</summary>
    public void Flush() { }

    /// <summary>No-op: IndexedDB writes are transactionally durable once the IDB transaction completes.</summary>
    public Task FlushAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    /// <inheritdoc/>
    /// <exception cref="NotSupportedException">
    /// IndexedDB storage does not support file-path-based backup.
    /// </exception>
    public Task BackupAsync(string destinationPath, CancellationToken cancellationToken = default)
        => throw new NotSupportedException(
            "IndexedDB storage does not support file-based backup. " +
            "Read pages through the IPageStorage API to create a logical backup.");

    /// <inheritdoc/>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        if (_opened)
            IndexedDbInterop.Close(_dbName);
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private async Task InitializeHeaderPagesAsync()
    {
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
        await IndexedDbInterop.WritePageAsync(_dbName, 0, Convert.ToBase64String(headerPage));

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
        await IndexedDbInterop.WritePageAsync(_dbName, 1, Convert.ToBase64String(metaPage));

        await IndexedDbInterop.SaveNextPageIdAsync(_dbName, 2);
        _nextPageId = 2;
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(IndexedDbPageStorage));
    }

    private void ThrowIfNotOpened()
    {
        if (!_opened)
            throw new InvalidOperationException($"{nameof(IndexedDbPageStorage)} must be opened before performing I/O. Call {nameof(OpenAsync)}().");
    }
}
