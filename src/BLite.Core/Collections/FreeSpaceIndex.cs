using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using BLite.Core.Storage;

namespace BLite.Core.Collections;

/// <summary>
/// 16-bucket Free Space Index (FSI) for data pages.
/// Divides the page's usable space into 16 equal categories (nibble encoding).
/// Provides O(1) page lookup and amortised O(1) updates in steady state.
/// Memory usage: ~16 bytes per tracked page (bucket array + free-space map).
/// </summary>
internal sealed class FreeSpaceIndex
{
    private const int BucketCount = 16;

    // One growable uint[] per bucket; each element is a page ID.
    private readonly uint[][] _buckets;

    // Live element count per bucket (actual length of valid entries in _buckets[b]).
    private readonly int[] _counts;

    // Secondary map: pageId → stored free bytes.
    // Used to (a) determine the old bucket when a page moves and (b) verify the boundary
    // bucket without reading the page from disk.
    private readonly Dictionary<uint, ushort> _freeMap;

    // Width of each bucket in bytes (pageSize / BucketCount).
    private readonly int _bucketWidth;

    public FreeSpaceIndex(int pageSize)
    {
        _bucketWidth = Math.Max(1, pageSize / BucketCount);
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
    /// Complexity: O(BucketCount) = O(1) regardless of the number of tracked pages.
    /// </summary>
    public uint FindPage(int requiredBytes, StorageEngine storage, ulong txnId)
    {
        int minBucket = GetBucket(requiredBytes);

        // Buckets above minBucket are guaranteed to hold enough free space
        // ((b+1) * _bucketWidth > requiredBytes for every b >= minBucket+1).
        for (int b = BucketCount - 1; b > minBucket; b--)
        {
            var arr = _buckets[b];
            var count = _counts[b];
            for (int i = count - 1; i >= 0; i--)
            {
                if (!storage.IsPageLocked(arr[i], txnId))
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
                    !storage.IsPageLocked(pid, txnId))
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
