using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace BLite.Core.Collections;

/// <summary>
/// 16-bucket Free Space Index (FSI) for data pages.
/// Divides the page's <em>usable</em> space into 16 equal categories (nibble encoding).
/// Provides O(1) page lookup and amortised O(1) updates in steady state.
/// Memory usage is O(tracked pages); the index stores one free-space entry per tracked
/// page plus membership in a bucket array.
/// </summary>
internal sealed class FreeSpaceIndex
{
    private const int BucketCount = 16;

    // One growable uint[] per bucket; each element is a page ID.
    private readonly uint[][] _buckets;

    // Live element count per bucket (actual length of valid entries in _buckets[b]).
    private readonly int[] _counts;

    // Stores the current free bytes for each tracked page.
    // Used to (a) determine the old bucket when a page moves and
    // (b) verify boundary-bucket entries without disk I/O.
    private readonly Dictionary<uint, ushort> _freeMap;

    // Width of each bucket in bytes, computed from the *usable* page space so that
    // bucket boundaries reflect real available room (not header overhead).
    private readonly int _bucketWidth;

    public FreeSpaceIndex(int pageSize)
    {
        // Subtract the fixed page-header size so that bucket boundaries are aligned to
        // actual free bytes a caller would observe in SlottedPageHeader.AvailableFreeSpace.
        // SlottedPageHeader.Size = 24 bytes.
        const int headerSize = 24;
        int usableSpace = pageSize - headerSize;
        _bucketWidth = Math.Max(1, usableSpace / BucketCount);
        _buckets = new uint[BucketCount][];
        _counts = new int[BucketCount];
        _freeMap = new Dictionary<uint, ushort>();
        for (int i = 0; i < BucketCount; i++)
            _buckets[i] = new uint[4]; // initial capacity per bucket
    }

    /// <summary>Returns the bucket index for a given free-byte count.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int GetBucket(int freeBytes) =>
        Math.Min(freeBytes / _bucketWidth, BucketCount - 1);

    /// <summary>
    /// Registers or updates a page with its current free byte count.
    /// O(1) when the bucket doesn't change; O(N/BucketCount) when the page moves
    /// between buckets (scans the old bucket to remove the stale entry).
    /// No heap allocation in steady state (arrays only grow when new pages are added).
    /// </summary>
    public void Update(uint pageId, int freeBytes)
    {
        int newBucket = GetBucket(freeBytes);

        if (_freeMap.TryGetValue(pageId, out var oldFreeBytes))
        {
            int oldBucket = GetBucket(oldFreeBytes);

            if (oldBucket == newBucket)
            {
                // Same bucket: just refresh the stored free bytes, no structural change.
                _freeMap[pageId] = (ushort)freeBytes;
                return;
            }

            // Different bucket: remove the page from the old bucket (swap-with-last).
            RemoveFromBucket(pageId, oldBucket);
        }

        // Add the page to the new bucket.
        AddToBucket(pageId, newBucket);
        _freeMap[pageId] = (ushort)freeBytes;
    }

    /// <summary>
    /// Tries to return the stored free bytes for a page without a disk read.
    /// Returns false if the page is not tracked in this index.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryGetFreeBytes(uint pageId, out ushort freeBytes) =>
        _freeMap.TryGetValue(pageId, out freeBytes);

    /// <summary>
    /// Finds a page that has at least <paramref name="requiredBytes"/> of free space
    /// and is not locked by another transaction.
    /// Returns 0 if no suitable page is found.
    /// Complexity: O(BucketCount) bucket iterations plus O(pages_scanned) within each
    /// bucket until an unlocked candidate is found; O(1) under no lock contention.
    /// </summary>
    /// <param name="requiredBytes">Minimum free bytes required.</param>
    /// <param name="isPageLocked">
    ///   Optional predicate that returns <c>true</c> when a page is locked by another
    ///   transaction and should be skipped.  Pass <c>null</c> to treat every page as
    ///   available (useful for unit testing without a real storage engine).
    /// </param>
    public uint FindPage(int requiredBytes, Func<uint, bool>? isPageLocked = null)
    {
        int minBucket = GetBucket(requiredBytes);

        // Buckets above minBucket are guaranteed to hold enough free space.
        // Bucket b is zero-based and holds pages whose free bytes fall in the range
        // [b * _bucketWidth, (b+1) * _bucketWidth).  For any b > minBucket every
        // page in that bucket already has at least (minBucket + 1) * _bucketWidth
        // free bytes, which is > requiredBytes by construction, so no additional
        // _freeMap lookup is needed.
        for (int b = BucketCount - 1; b > minBucket; b--)
        {
            var arr = _buckets[b];
            var count = _counts[b];
            for (int i = count - 1; i >= 0; i--)
            {
                if (isPageLocked == null || !isPageLocked(arr[i]))
                    return arr[i];
            }
        }

        // Boundary bucket: entries may have less free space than required (floor division),
        // so we verify actual stored free bytes before returning a candidate.
        {
            var arr = _buckets[minBucket];
            var count = _counts[minBucket];
            for (int i = count - 1; i >= 0; i--)
            {
                uint pid = arr[i];
                if (_freeMap.TryGetValue(pid, out var fb) &&
                    fb >= requiredBytes &&
                    (isPageLocked == null || !isPageLocked(pid)))
                    return pid;
            }
        }

        return 0;
    }

    /// <summary>
    /// Finds a page that has at least <paramref name="requiredBytes"/> of free space
    /// and is not locked by another transaction.
    /// This overload accepts a two-argument predicate so callers can pass a cached
    /// method-group delegate (e.g. <c>_storage.IsPageLocked</c>) together with a
    /// per-call <paramref name="txnId"/> without allocating a closure on each call.
    /// Returns 0 if no suitable page is found.
    /// </summary>
    /// <param name="requiredBytes">Minimum free bytes required.</param>
    /// <param name="txnId">Transaction ID to exclude from locking checks.</param>
    /// <param name="isPageLocked">
    ///   Optional predicate; called as <c>isPageLocked(pageId, txnId)</c> and should
    ///   return <c>true</c> when the page is locked by another transaction.
    ///   Pass <c>null</c> to treat every page as available.
    /// </param>
    public uint FindPage(int requiredBytes, ulong txnId, Func<uint, ulong, bool>? isPageLocked)
    {
        int minBucket = GetBucket(requiredBytes);

        for (int b = BucketCount - 1; b > minBucket; b--)
        {
            var arr = _buckets[b];
            var count = _counts[b];
            for (int i = count - 1; i >= 0; i--)
            {
                if (isPageLocked == null || !isPageLocked(arr[i], txnId))
                    return arr[i];
            }
        }

        {
            var arr = _buckets[minBucket];
            var count = _counts[minBucket];
            for (int i = count - 1; i >= 0; i--)
            {
                uint pid = arr[i];
                if (_freeMap.TryGetValue(pid, out var fb) &&
                    fb >= requiredBytes &&
                    (isPageLocked == null || !isPageLocked(pid, txnId)))
                    return pid;
            }
        }

        return 0;
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    private void RemoveFromBucket(uint pageId, int bucket)
    {
        var arr = _buckets[bucket];
        var count = _counts[bucket];
        for (int i = 0; i < count; i++)
        {
            if (arr[i] == pageId)
            {
                // Swap with the last valid entry to avoid a shift.
                arr[i] = arr[count - 1];
                _counts[bucket]--;
                return;
            }
        }
        // If not found the index is already consistent (e.g., page was never added).
    }

    private void AddToBucket(uint pageId, int bucket)
    {
        int count = _counts[bucket];
        if (count == _buckets[bucket].Length)
            Array.Resize(ref _buckets[bucket], _buckets[bucket].Length * 2);
        _buckets[bucket][count] = pageId;
        _counts[bucket]++;
    }
}

