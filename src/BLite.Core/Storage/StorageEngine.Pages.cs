using BLite.Core.Transactions;

namespace BLite.Core.Storage;

public sealed partial class StorageEngine
{
    /// <summary>
    /// Reads a page with transaction isolation.
    /// 1. Check WAL cache for uncommitted writes (Read Your Own Writes)
    /// 2. Check WAL index for committed writes (lazy replay)
    /// 3. Read from PageFile (committed baseline)
    /// </summary>
    /// <param name="pageId">Page to read</param>
    /// <param name="transactionId">Optional transaction ID for isolation</param>
    /// <param name="destination">Buffer to write page data</param>
    public void ReadPage(uint pageId, ulong? transactionId, Span<byte> destination)
    {
        // 1. Check transaction-local WAL cache (Read Your Own Writes)
        // transactionId=0 or null means "no active transaction, read committed only"
        if (transactionId.HasValue && 
            transactionId.Value != 0 &&
            _walCache.TryGetValue(transactionId.Value, out var txnPages) &&
            txnPages.TryGetValue(pageId, out var uncommittedData))
        {
            var length = Math.Min(uncommittedData.Length, destination.Length);
            uncommittedData.AsSpan(0, length).CopyTo(destination);
            return;
        }
        
        // 2. Check WAL index (committed but not checkpointed)
        if (_walIndex.TryGetValue(pageId, out var committedData))
        {
            var length = Math.Min(committedData.Length, destination.Length);
            committedData.AsSpan(0, length).CopyTo(destination);
            return;
        }
        
        // 3. Read committed baseline from PageFile
        GetPageFile(pageId, out var physId).ReadPage(physId, destination);
    }

    /// <summary>
    /// Reads only the first <paramref name="destination"/>.Length bytes of a page
    /// (the page header), applying the same transaction-isolation logic as
    /// <see cref="ReadPage"/>.  Use this instead of <see cref="ReadPage"/> when only
    /// the header fields are needed: it avoids renting a full-page buffer and reduces
    /// the amount of data copied from the memory-mapped file, which is significant
    /// when scanning many pages during cold-start reconstruction.
    /// </summary>
    /// <param name="pageId">Page to read.</param>
    /// <param name="transactionId">Optional transaction ID for isolation.</param>
    /// <param name="destination">
    ///   Buffer to write header bytes into; must be at most <see cref="PageSize"/> bytes.
    /// </param>
    public void ReadPageHeader(uint pageId, ulong? transactionId, Span<byte> destination)
    {
        if (transactionId.HasValue &&
            transactionId.Value != 0 &&
            _walCache.TryGetValue(transactionId.Value, out var txnPages) &&
            txnPages.TryGetValue(pageId, out var uncommittedData))
        {
            var length = Math.Min(uncommittedData.Length, destination.Length);
            uncommittedData.AsSpan(0, length).CopyTo(destination);
            return;
        }

        if (_walIndex.TryGetValue(pageId, out var committedData))
        {
            var length = Math.Min(committedData.Length, destination.Length);
            committedData.AsSpan(0, length).CopyTo(destination);
            return;
        }

        GetPageFile(pageId, out var physId).ReadPageHeader(physId, destination);
    }

    /// <summary>
    /// Writes a page within a transaction.
    /// Data goes to WAL cache immediately and becomes visible to that transaction only.
    /// Will be written to WAL on commit.
    /// </summary>
    /// <param name="pageId">Page to write</param>
    /// <param name="transactionId">Transaction ID owning this write</param>
    /// <param name="data">Page data</param>
    public void WritePage(uint pageId, ulong transactionId, ReadOnlySpan<byte> data)
    {
        if (transactionId == 0)
            throw new InvalidOperationException("Cannot write without a transaction (transactionId=0 is reserved)");

        // Get or create transaction-local cache
        var txnPages = _walCache.GetOrAdd(transactionId,
            _ => new System.Collections.Concurrent.ConcurrentDictionary<uint, byte[]>());

        // If this page was already written in this transaction, copy new data into the
        // existing buffer instead of allocating a fresh one.  Pages are always exactly
        // PageSize bytes, so the buffer can always be reused without resizing.
        if (txnPages.TryGetValue(pageId, out var existing))
        {
            data.CopyTo(existing);
        }
        else
        {
            txnPages[pageId] = data.ToArray();
        }
    }

    /// <summary>
    /// Writes a page immediately to disk (non-transactional).
    /// Used for initialization and metadata updates outside of transactions.
    /// </summary>
    /// <param name="pageId">Page to write</param>
    /// <param name="data">Page data</param>
    public void WritePageImmediate(uint pageId, ReadOnlySpan<byte> data)
    {
        GetPageFile(pageId, out var physId).WritePage(physId, data);
    }

    /// <summary>
    /// Reads a page with transaction isolation (asynchronous).
    /// Mirrors <see cref="ReadPage"/> but uses <see cref="Memory{T}"/> instead of <see cref="Span{T}"/>
    /// so it can cross <c>await</c> boundaries.
    /// <list type="number">
    ///   <item>WAL cache (uncommitted, per-transaction) — resolved synchronously, no I/O.</item>
    ///   <item>WAL index (committed, not yet checkpointed) — resolved synchronously, no I/O.</item>
    ///   <item>PageFile — true async OS read via <see cref="RandomAccess.ReadAsync"/>.</item>
    /// </list>
    /// </summary>
    /// <param name="pageId">Page to read.</param>
    /// <param name="transactionId">Optional transaction ID for isolation (null = read committed only).</param>
    /// <param name="destination">Buffer of at least <see cref="PageSize"/> bytes.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public ValueTask ReadPageAsync(uint pageId, ulong? transactionId, Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        // 1. WAL cache — uncommitted writes visible only to the owning transaction (synchronous, no I/O)
        if (transactionId.HasValue &&
            transactionId.Value != 0 &&
            _walCache.TryGetValue(transactionId.Value, out var txnPages) &&
            txnPages.TryGetValue(pageId, out var uncommittedData))
        {
            var length = Math.Min(uncommittedData.Length, destination.Length);
            uncommittedData.AsMemory(0, length).CopyTo(destination);
#if NET5_0_OR_GREATER
            return ValueTask.CompletedTask;
#else
            return default;
#endif
        }

        // 2. WAL index — committed but not yet checkpointed (synchronous, no I/O)
        if (_walIndex.TryGetValue(pageId, out var committedData))
        {
            var length = Math.Min(committedData.Length, destination.Length);
            committedData.AsMemory(0, length).CopyTo(destination);
#if NET5_0_OR_GREATER
            return ValueTask.CompletedTask;
#else
            return default;
#endif
        }

        // 3. PageFile — true async OS read
        return GetPageFile(pageId, out var physId).ReadPageAsync(physId, destination, cancellationToken);
    }

    /// <summary>
    /// Gets the number of pages currently allocated in the page file.
    /// Useful for full database scans.
    /// </summary>
    public uint PageCount => _pageFile.NextPageId;

    /// <summary>
    /// Shrinks all opened page storage files by removing trailing free pages.
    /// Truncates the main page file, the optional index file, and any per-collection
    /// files that are currently open.
    /// </summary>
    public async Task TruncateToMinimumAsync(CancellationToken ct = default)
    {
        await _pageFile.TruncateToMinimumAsync(ct).ConfigureAwait(false);

        if (_indexFile != null && !ReferenceEquals(_indexFile, _pageFile))
        {
            ct.ThrowIfCancellationRequested();
            await _indexFile.TruncateToMinimumAsync(ct).ConfigureAwait(false);
        }

        if (_collectionFiles != null)
        {
            foreach (var lazy in _collectionFiles.Values)
            {
                ct.ThrowIfCancellationRequested();
                if (lazy.IsValueCreated)
                    await lazy.Value.TruncateToMinimumAsync(ct).ConfigureAwait(false);
            }
        }
    }
}
