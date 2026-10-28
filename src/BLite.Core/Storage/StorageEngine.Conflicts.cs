using System.Collections.Concurrent;

namespace BLite.Core.Storage;

public sealed partial class StorageEngine
{
    // -------------------------------------------------------------------------
    // Write-write conflict detection (#304)
    //
    // Each transaction writes private copies of the pages it changes and a commit publishes them
    // whole. Without a check, two transactions that change the same page both commit and the
    // second one's copy, made from the page as it was before the first commit, silently replaces
    // the first one's changes.
    //
    // Every page has a version: the sequence number of the last commit that published it. A
    // transaction remembers, for each page, the version it first saw (at its first read from the
    // committed state, or at its first write if it never read the page). At commit, inside the
    // group-commit batch and before any WAL record is written, a page whose version has moved on
    // since, or that an earlier commit of the same batch also writes, makes the commit fail with
    // BLiteTransactionConflictException. The first committer wins.
    //
    // Orderings that keep the check from missing a conflict (it may report a spurious one, which
    // the caller retries, but never let a lost write through):
    //   • A read captures the version before it copies the page; a commit publishes the image
    //     before it raises the version. A reader never pairs a new version with old data.
    //   • Pruning and a truncation by another process raise the floor every page version is
    //     compared against, so a version can only grow.
    // -------------------------------------------------------------------------

    /// <summary>Sequence number of the last commit (or replay, or floor raise) that changed a page version.</summary>
    private long _commitSequence;

    /// <summary>Version of every page that has no entry in <see cref="_pageVersions"/>.</summary>
    private long _pageVersionFloor;

    /// <summary>Last commit sequence that published each page, above <see cref="_pageVersionFloor"/>.</summary>
    private readonly ConcurrentDictionary<uint, long> _pageVersions = new();

    /// <summary>Per active transaction: the version of each page it first read or wrote.</summary>
    private readonly ConcurrentDictionary<ulong, ConcurrentDictionary<uint, long>> _baseVersions = new();

    /// <summary>Number of page versions above which a commit tries to forget them.</summary>
    internal const int PageVersionPruneThreshold = 4096;

    private long GetPageVersion(uint pageId)
    {
        long floor = Volatile.Read(ref _pageVersionFloor);
        return _pageVersions.TryGetValue(pageId, out long version) && version > floor ? version : floor;
    }

    /// <summary>
    /// Records <paramref name="version"/>, captured before the page was copied, as the version
    /// <paramref name="transactionId"/> based its view of <paramref name="pageId"/> on, unless it
    /// saw the page before. Reads without a transaction, or by one that is no longer active, record nothing.
    /// </summary>
    private void RecordRead(ulong? transactionId, uint pageId, long version)
    {
        if (!transactionId.HasValue || transactionId.Value == 0)
            return;

        if (!_baseVersions.TryGetValue(transactionId.Value, out var bases))
        {
            if (!_activeTransactions.ContainsKey(transactionId.Value))
                return;
            bases = _baseVersions.GetOrAdd(transactionId.Value, _ => new ConcurrentDictionary<uint, long>());
        }
        bases.TryAdd(pageId, version);
    }

    /// <summary>Records the version of a page a transaction writes without having read it first.</summary>
    private void RecordFirstWrite(ulong transactionId, uint pageId)
    {
        var bases = _baseVersions.GetOrAdd(transactionId, _ => new ConcurrentDictionary<uint, long>());
        if (!bases.ContainsKey(pageId))
            bases.TryAdd(pageId, GetPageVersion(pageId));
    }

    /// <summary>
    /// Returns the first page of <paramref name="pages"/> that another commit has published since
    /// <paramref name="transactionId"/> first saw it, or that <paramref name="writtenEarlierInBatch"/>
    /// holds; <c>null</c> if there is none. Called under <c>_commitLock</c>, except for the vote of
    /// <see cref="WouldConflict"/>.
    /// </summary>
    private uint? FindConflict(ulong transactionId, ConcurrentDictionary<uint, byte[]> pages, HashSet<uint>? writtenEarlierInBatch)
    {
        _baseVersions.TryGetValue(transactionId, out var bases);
        foreach (var pageId in pages.Keys)
        {
            if (writtenEarlierInBatch != null && writtenEarlierInBatch.Contains(pageId))
                return pageId;
            if (bases != null && bases.TryGetValue(pageId, out long baseVersion) && GetPageVersion(pageId) != baseVersion)
                return pageId;
        }
        return null;
    }

    /// <summary>
    /// Raises the version of every page in <paramref name="pageIds"/> to a new commit sequence.
    /// Call after their new images are published. Called under <c>_commitLock</c>.
    /// </summary>
    private void PublishPageVersions(IEnumerable<uint> pageIds)
    {
        long sequence = Interlocked.Increment(ref _commitSequence);
        foreach (var pageId in pageIds)
            _pageVersions[pageId] = sequence;
    }

    /// <summary>
    /// Makes every page version newer than any a transaction may have recorded, e.g. after another
    /// process truncated the shared WAL and this engine discarded what it held from it.
    /// </summary>
    private void RaisePageVersionFloor()
    {
        Volatile.Write(ref _pageVersionFloor, Interlocked.Increment(ref _commitSequence));
        _pageVersions.Clear();
    }

    /// <summary>
    /// Forgets the page versions once no active transaction has recorded any, so that the map does
    /// not grow with every page ever written. Called under <c>_commitLock</c>.
    /// </summary>
    private void TryPrunePageVersions()
    {
        if (_pageVersions.Count <= PageVersionPruneThreshold)
            return;

        foreach (var transactionId in _baseVersions.Keys)
        {
            if (!_activeTransactions.ContainsKey(transactionId))
                _baseVersions.TryRemove(transactionId, out _);
        }

        if (_baseVersions.IsEmpty)
            RaisePageVersionFloor();
    }

    /// <summary>
    /// Throws <see cref="BLiteTransactionConflictException"/> if <paramref name="transactionId"/>
    /// conflicts with a commit published since it first saw one of its pages. For the direct commit
    /// paths, which run under <c>_commitLock</c> outside group commit.
    /// </summary>
    private void ThrowIfConflicting(ulong transactionId, ConcurrentDictionary<uint, byte[]>? pages)
    {
        if (pages != null && FindConflict(transactionId, pages, null) is uint pageId)
            throw new BLiteTransactionConflictException(transactionId, pageId);
    }

    /// <summary>
    /// True if a commit of <paramref name="transactionId"/> would be rejected now. Without
    /// <c>_commitLock</c>, so only a vote for two-phase commit: a commit can still land before the
    /// commit record, and <see cref="MarkTransactionCommittedAsync"/> checks again under the lock.
    /// </summary>
    private bool WouldConflict(ulong transactionId)
        => _walCache.TryGetValue(transactionId, out var pages) && FindConflict(transactionId, pages, null) != null;

    /// <summary>Number of active transactions with recorded page versions (diagnostics and tests).</summary>
    internal int TrackedTransactionCount => _baseVersions.Count;

    /// <summary>Number of pages with a version above the floor (diagnostics and tests).</summary>
    internal int TrackedPageVersionCount => _pageVersions.Count;
}
