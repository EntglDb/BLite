using System;
using System.Threading;
using System.Threading.Tasks;
using BLite.Core;
using BLite.Core.Storage;
using BLite.Wasm.Interop;

namespace BLite.Wasm.Storage;

/// <summary>
/// OPFS (Origin Private File System) page storage backend for browser WASM.
/// <para>
/// Pages are stored as sequential regions in a single OPFS file using the
/// <see cref="https://developer.mozilla.org/en-US/docs/Web/API/FileSystemSyncAccessHandle">
/// FileSystemSyncAccessHandle</see> API, which provides synchronous, high-performance
/// I/O from a dedicated Worker thread.
/// </para>
/// <para>
/// Supported in Chrome 102+, Firefox 111+, and Safari 15.2+ in Worker contexts.
/// Use <see cref="IsAvailable"/> to feature-detect at runtime.
/// </para>
/// </summary>
public sealed class OpfsPageStorage : IPageStorage
{
    private readonly string _dbName;
    private readonly int _pageSize;
    private readonly Stack<uint> _freeList = new(); // Guarded by _allocationLock for all access.
    private readonly object _allocationLock = new();
    private uint _nextPageId;
    private bool _opened;
    private bool _disposed;

    /// <summary>
    /// Initialises a new OPFS page storage for the given database name.
    /// Call <see cref="OpenAsync"/> to initialise before use.
    /// </summary>
    /// <param name="dbName">Logical database name (becomes the OPFS file name).</param>
    /// <param name="pageSize">Fixed page size in bytes (minimum 512).</param>
    public OpfsPageStorage(string dbName, int pageSize)
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

    /// <summary>Returns <c>true</c> if the OPFS SyncAccessHandle API is available in the current browser context.</summary>
    public static bool IsAvailable() => OpfsInterop.IsAvailable();

    /// <summary>
    /// Opens the OPFS file asynchronously. Must be called once before any I/O.
    /// If the file already contains data, the page count is restored from the file size.
    /// Otherwise, header pages are initialised.
    /// </summary>
    public async Task OpenAsync()
    {
        ThrowIfDisposed();
        if (_opened)
            return;

        await OpfsInterop.EnsureLoadedAsync();
        var fileSize = (long)await OpfsInterop.OpenAsync(_dbName, _pageSize);

        if (fileSize >= _pageSize * 2)
        {
            // Existing database — derive page count from file size.
            _nextPageId = (uint)(fileSize / _pageSize);
        }
        else
        {
            // Fresh database — write header pages.
            InitializeHeaderPages();
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

        OpfsInterop.ReadPage(_dbName, (int)pageId, destination.Slice(0, _pageSize));
    }

    /// <inheritdoc/>
    public void ReadPageHeader(uint pageId, Span<byte> destination)
    {
        ThrowIfDisposed();
        ThrowIfNotOpened();

        if (destination.Length > _pageSize)
            throw new ArgumentException($"Destination must not exceed {_pageSize} bytes.");

        // OPFS SyncAccessHandle reads are position-based; read the full page then copy header.
        Span<byte> tmp = stackalloc byte[_pageSize <= 16384 ? _pageSize : 0];
        byte[]? rented = null;
        if (tmp.Length == 0)
        {
            rented = System.Buffers.ArrayPool<byte>.Shared.Rent(_pageSize);
            tmp = rented.AsSpan(0, _pageSize);
        }

        try
        {
            OpfsInterop.ReadPage(_dbName, (int)pageId, tmp);
            tmp.Slice(0, destination.Length).CopyTo(destination);
        }
        finally
        {
            if (rented != null)
                System.Buffers.ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <inheritdoc/>
    public ValueTask ReadPageAsync(uint pageId, Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ReadPage(pageId, destination.Span.Slice(0, _pageSize));
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc/>
    public void WritePage(uint pageId, ReadOnlySpan<byte> source)
    {
        ThrowIfDisposed();
        ThrowIfNotOpened();

        if (source.Length < _pageSize)
            throw new ArgumentException($"Source must be at least {_pageSize} bytes.");

        // JSImport requires a mutable Span; copy into a writable buffer.
        var buffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_pageSize);
        try
        {
            source.Slice(0, _pageSize).CopyTo(buffer);
            OpfsInterop.WritePage(_dbName, (int)pageId, buffer.AsSpan(0, _pageSize));
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(buffer);
        }
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

    /// <inheritdoc/>
    public void Flush()
    {
        ThrowIfDisposed();
        if (_opened)
            OpfsInterop.Flush(_dbName);
    }

    /// <inheritdoc/>
    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        Flush();
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    /// <exception cref="NotSupportedException">
    /// OPFS storage does not support file-path-based backup.
    /// </exception>
    public Task BackupAsync(string destinationPath, CancellationToken cancellationToken = default)
        => throw new NotSupportedException(
            "OPFS storage does not support file-based backup. " +
            "Read pages through the IPageStorage API to create a logical backup.");

    /// <inheritdoc/>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        if (_opened)
            OpfsInterop.Close(_dbName);
    }

    // ─── Helpers ─────────────────────────────────────────────────────────────

    private void InitializeHeaderPages()
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
        OpfsInterop.WritePage(_dbName, 0, headerPage.AsSpan(0, _pageSize));

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
        OpfsInterop.WritePage(_dbName, 1, metaPage.AsSpan(0, _pageSize));

        OpfsInterop.Flush(_dbName);

        _nextPageId = 2;
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(OpfsPageStorage));
    }

    private void ThrowIfNotOpened()
    {
        if (!_opened)
            throw new InvalidOperationException($"{nameof(OpfsPageStorage)} must be opened before performing I/O. Call {nameof(OpenAsync)}().");
    }
}
