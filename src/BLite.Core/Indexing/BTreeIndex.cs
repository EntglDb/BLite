using BLite.Bson;
using BLite.Core.Storage;
using BLite.Core.Transactions;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace BLite.Core.Indexing;

/// <summary>
/// B+Tree index implementation for ordered index operations.
/// </summary>
public sealed class BTreeIndex
{
    private readonly StorageEngine _storage;
    private readonly IndexOptions _options;
    private readonly uint _rootPageId;
    internal const int MaxEntriesPerNode = 64;

    /// <param name="rootPageId">
    /// The root page persisted in the collection metadata, or 0 to allocate a new empty index.
    /// The root page id never changes afterwards: a root split or collapse rewrites the root
    /// page in place (see <see cref="CreateNewRoot"/>), so uncommitted root changes stay in the
    /// owning transaction's page cache like any other page write and the persisted id stays valid.
    /// </param>
    public BTreeIndex(StorageEngine storage,
                      IndexOptions options, 
                      uint rootPageId = 0)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _options = options;
        if (rootPageId == 0)
        {
            // Allocate new root page (cannot use page 0 which is file header)
            rootPageId = _storage.AllocateIndexPage();

            // Initialize as empty leaf
            var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);
            try
            {
                // Clear buffer
                pageBuffer.AsSpan().Clear();

                // Write headers
                var pageHeader = new PageHeader
                {
                    PageId = rootPageId,
                    PageType = PageType.Index,
                    FreeBytes = (ushort)(_storage.PageSize - 32),
                    NextPageId = 0,
                    TransactionId = 0,
                    Checksum = 0
                };
                pageHeader.WriteTo(pageBuffer);

                var nodeHeader = new BTreeNodeHeader
                {
                    IsLeaf = true,
                    EntryCount = 0,
                    NextLeafPageId = 0
                };
                nodeHeader.WriteTo(pageBuffer.AsSpan(32));

                _storage.WritePageImmediate(rootPageId, pageBuffer);
            }
            finally
            {
                System.Buffers.ArrayPool<byte>.Shared.Return(pageBuffer);
            }
        }

        _rootPageId = rootPageId;
    }

    /// <summary>The root page of this index. It is fixed for the lifetime of the index.</summary>
    public uint RootPageId => _rootPageId;

    /// <summary>
    /// The largest key an index on this page size accepts: an entry for it takes at most half
    /// of the entry space of a node, leaf (4-byte length, key, 6-byte location) or internal
    /// (4-byte length, key, 4-byte child pointer, after the 4-byte leftmost child pointer).
    /// That bound is what lets a full node always split in two halves that each fit a page
    /// (#217); a key as large as a whole page could fit alone but not next to a sibling entry.
    /// The collections reject a larger key before writing anything, and <see cref="Insert"/>
    /// rejects it too.
    /// </summary>
    internal int MaxKeySize => InternalEntrySpace / 2 - 4 - 4;

    /// <summary>Where the entries of a node start: page header (32) + node header (20).</summary>
    private const int NodeDataStart = 32 + 20;

    /// <summary>Bytes available for the entries of a leaf node.</summary>
    private int LeafEntrySpace => _storage.PageSize - NodeDataStart;

    /// <summary>Bytes available for the entries of an internal node, after its leftmost child pointer.</summary>
    private int InternalEntrySpace => _storage.PageSize - NodeDataStart - 4;

    private static int LeafEntrySize(int keyLength) => 4 + keyLength + DocumentLocation.SerializedSize;

    private static int InternalEntrySize(int keyLength) => 4 + keyLength + 4;

    /// <summary>
    /// Reads a page using StorageEngine for transaction isolation.
    /// Implements "Read Your Own Writes" isolation.
    /// </summary>
    internal void ReadPage(uint pageId, ulong transactionId, Span<byte> destination)
    {
        _storage.ReadPage(pageId, transactionId, destination);
    }

    /// <summary>
    /// Writes a page using StorageEngine for transaction isolation.
    /// </summary>
    private void WritePage(uint pageId, ulong transactionId, ReadOnlySpan<byte> data)
    {
        _storage.WritePage(pageId, transactionId, data);
    }

    /// <summary>
    /// Inserts a key-location pair into the index
    /// </summary>
    public void Insert(IndexKey key, DocumentLocation location, ulong? transactionId = null)
    {
        var txnId = transactionId ?? 0;

        if (key.Data.Length > MaxKeySize)
            throw new ArgumentException(
                $"Index key of {key.Data.Length} bytes is too large: a B-Tree index on {_storage.PageSize}-byte pages holds keys of at most {MaxKeySize} bytes.",
                nameof(key));

        var entry = new IndexEntry(key, location);
        var path = new List<uint>();

        var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            // Find the leaf node for insertion; the leaf is left in pageBuffer. An entry equal to
            // a separator goes to the right of it, after any duplicates already there.
            var leafPageId = FindLeafNodeWithPath(key, path, txnId, pageBuffer, leftmost: false);
            var header = BTreeNodeHeader.ReadFrom(pageBuffer.AsSpan(32));

            if (_options.Unique)
            {
                var existing = BinarySearchLeaf(pageBuffer, header.EntryCount, key);
                if (existing.Found)
                {
                    if (!existing.Location.Equals(location))
                        throw new InvalidOperationException("Duplicate key violation for unique index");
                    return;
                }
            }

            // Insert entry into leaf, splitting it when the entry does not fit
            InsertIntoLeaf(leafPageId, entry, pageBuffer, path, txnId);
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(pageBuffer);
        }
    }

    /// <summary>
    /// Finds a document location by exact key match
    /// </summary>
    public bool TryFind(IndexKey key, out DocumentLocation location, ulong? transactionId = null)
    {
        location = default;
        var txnId = transactionId ?? 0;

        var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            // Start at the leftmost leaf that can hold the key (#213): in a non-unique index the
            // leaf right of an equal separator may have lost its copies of the key while the
            // leaf left of it still has some.
            var leafPageId = FindLeafNodeWithPath(key, null, txnId, pageBuffer, leftmost: true);
            while (true)
            {
                var header = BTreeNodeHeader.ReadFrom(pageBuffer.AsSpan(32));
                if (SearchLeafLowerBound(pageBuffer, header.EntryCount, key, out var found, out location))
                    return found;

                // Every key in this leaf is smaller: the key, if present, starts the next leaf.
                leafPageId = header.NextLeafPageId;
                if (leafPageId == 0) return false;
                ReadPage(leafPageId, txnId, pageBuffer);
            }
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(pageBuffer);
        }
    }

    /// <summary>
    /// Range scan: finds all entries between minKey and maxKey (inclusive)
    /// </summary>
    /// <summary>
    /// Range scan: finds all entries between minKey and maxKey (inclusive)
    /// </summary>
    public IEnumerable<IndexEntry> Range(IndexKey minKey, IndexKey maxKey, IndexDirection direction = IndexDirection.Forward, ulong? transactionId = null)
    {
        var txnId = transactionId ?? 0;
        var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        
        try
        {
            if (direction == IndexDirection.Forward)
            {
                // The leftmost leaf that can hold minKey: copies of it may sit left of an equal
                // separator (#213). The scan walks right from there.
                var leafPageId = FindLeafNode(minKey, txnId, leftmost: true);

                while (leafPageId != 0)
                {
                    ReadPage(leafPageId, txnId, pageBuffer);

                    var header = BTreeNodeHeader.ReadFrom(pageBuffer.AsSpan(32));
                    var dataOffset = 32 + 20; // Adjusted for 20-byte header

                    for (int i = 0; i < header.EntryCount; i++)
                    {
                        // Read the key length prefix and form a span — no IndexKey allocation.
                        int keyLength = BitConverter.ToInt32(pageBuffer.AsSpan(dataOffset, 4));
                        var entryKeySpan = pageBuffer.AsSpan(dataOffset + 4, keyLength);

                        int cmpMin = IndexKey.CompareRaw(entryKeySpan, minKey.Data);
                        if (cmpMin >= 0) // entryKey >= minKey
                        {
                            int cmpMax = IndexKey.CompareRaw(entryKeySpan, maxKey.Data);
                            if (cmpMax <= 0) // entryKey <= maxKey → in range
                            {
                                var locationOffset = dataOffset + 4 + keyLength;
                                var location = DocumentLocation.ReadFrom(pageBuffer.AsSpan(locationOffset, DocumentLocation.SerializedSize));
                                // Allocate IndexKey only for entries that are actually yielded.
                                yield return new IndexEntry(IndexKey.FromOwnedArray(pageBuffer.AsSpan(dataOffset + 4, keyLength).ToArray()), location);
                            }
                            else
                            {
                                yield break; // entryKey > maxKey → past the range
                            }
                        }
                        // else entryKey < minKey → not yet in range, advance

                        dataOffset += 4 + keyLength + DocumentLocation.SerializedSize;
                    }

                    leafPageId = header.NextLeafPageId;
                }
            }
            else // Backward
            {
                // Start from the end of the range: the rightmost leaf that can hold maxKey.
                var leafPageId = FindLeafNode(maxKey, txnId, leftmost: false);

                while (leafPageId != 0)
                {
                    ReadPage(leafPageId, txnId, pageBuffer);

                    var header = BTreeNodeHeader.ReadFrom(pageBuffer.AsSpan(32));
                    
                    // Parse all entries in leaf first (since variable length, we have to scan forward to find offsets)
                    // Optimization: Could cache offsets or scan once. For now, read all entries then iterate in reverse.
                    var entries = ReadLeafEntries(pageBuffer, header.EntryCount);
                    
                    // Iterate valid entries in reverse order
                    for (int i = entries.Count - 1; i >= 0; i--)
                    {
                        var entry = entries[i];
                        if (entry.Key <= maxKey && entry.Key >= minKey)
                        {
                            yield return entry;
                        }
                        else if (entry.Key < minKey)
                        {
                            yield break; // Exceeded range (below min)
                        }
                    }
                    
                    // Check if we need to continue to previous leaf
                    // If the first entry in this page is still >= minKey, we might have more matches in PrevLeaf
                    // "Check previous page" logic...
                    if (entries.Count > 0 && entries[0].Key >= minKey)
                    {
                        leafPageId = header.PrevLeafPageId;
                    }
                    else
                    {
                         // We found an entry < minKey (handled in loop break) OR page was empty (unlikely)
                         if (entries.Count > 0 && entries[0].Key < minKey)
                            yield break;

                         leafPageId = header.PrevLeafPageId;
                    }
                }
            }
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(pageBuffer);
        }
    }

    /// <summary>
    /// Counts leaf entries in [<paramref name="minKey"/>, <paramref name="maxKey"/>] (inclusive)
    /// without allocating any <see cref="IndexEntry"/> or key objects.
    /// Equivalent to <c>Range(min, max).Count()</c> but with zero per-entry heap allocations.
    /// </summary>
    internal int CountRange(IndexKey minKey, IndexKey maxKey, ulong? transactionId = null)
    {
        var counter = new CountingVisitor();
        ScanRange(minKey, maxKey, transactionId, ref counter);
        return counter.Count;
    }

    /// <summary>Receives the keys of a <see cref="ScanRange{TVisitor}"/>, in key order.</summary>
    internal interface IKeyVisitor
    {
        /// <summary>Handles one key; returns <c>false</c> to stop the scan.</summary>
        bool Visit(ReadOnlySpan<byte> key);
    }

    private struct CountingVisitor : IKeyVisitor
    {
        public int Count;
        public bool Visit(ReadOnlySpan<byte> key) { Count++; return true; }
    }

    /// <summary>
    /// Hands every leaf key in [<paramref name="minKey"/>, <paramref name="maxKey"/>] (inclusive)
    /// to <paramref name="visitor"/> straight from the page buffer, in ascending order, without
    /// allocating per entry. The key span is only valid during the call.
    /// </summary>
    internal void ScanRange<TVisitor>(IndexKey minKey, IndexKey maxKey, ulong? transactionId, ref TVisitor visitor)
        where TVisitor : struct, IKeyVisitor
    {
        var txnId = transactionId ?? 0;
        var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            var leafPageId = FindLeafNode(minKey, txnId, leftmost: true);
            // Once we've passed minKey in the sorted leaf chain all subsequent entries
            // are also >= minKey, so we only need the minKey check until we enter the range.
            bool inRange = false;

            while (leafPageId != 0)
            {
                ReadPage(leafPageId, txnId, pageBuffer);
                var header = BTreeNodeHeader.ReadFrom(pageBuffer.AsSpan(32));
                var dataOffset = 32 + 20;
                bool stopScan = false;

                for (int i = 0; i < header.EntryCount; i++)
                {
                    int keyLength = BitConverter.ToInt32(pageBuffer.AsSpan(dataOffset, 4));
                    var entryKeySpan = pageBuffer.AsSpan(dataOffset + 4, keyLength);

                    if (!inRange)
                    {
                        // Advance until we reach an entry >= minKey.
                        if (IndexKey.CompareRaw(entryKeySpan, minKey.Data) >= 0)
                            inRange = true;
                        else
                        {
                            dataOffset += 4 + keyLength + DocumentLocation.SerializedSize;
                            continue;
                        }
                    }

                    // inRange: compare against maxKey only.
                    if (IndexKey.CompareRaw(entryKeySpan, maxKey.Data) > 0)
                    {
                        stopScan = true; // past maxKey — no more matches in any leaf
                        break;
                    }
                    if (!visitor.Visit(entryKeySpan))
                    {
                        stopScan = true; // the visitor has seen enough
                        break;
                    }

                    dataOffset += 4 + keyLength + DocumentLocation.SerializedSize;
                }

                if (stopScan) break;
                leafPageId = header.NextLeafPageId;
            }
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(pageBuffer);
        }
    }


    /// <summary>
    /// Finds the first leaf entry in [minKey, maxKey] (inclusive) without allocating an
    /// iterator state machine.  Equivalent to <c>Range(min,max).FirstOrDefault()</c> but with
    /// zero extra heap allocations and no enumerator overhead.
    /// Returns <c>true</c> and sets <paramref name="location"/> when a match is found.
    /// </summary>
    internal bool TryFindFirst(IndexKey minKey, IndexKey maxKey, ulong txnId, out DocumentLocation location)
    {
        var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            var leafPageId = FindLeafNode(minKey, txnId, leftmost: true);

            while (leafPageId != 0)
            {
                ReadPage(leafPageId, txnId, pageBuffer);
                var header = BTreeNodeHeader.ReadFrom(pageBuffer.AsSpan(32));
                var dataOffset = 32 + 20; // header offset same as Range forward

                for (int i = 0; i < header.EntryCount; i++)
                {
                    int keyLength = BitConverter.ToInt32(pageBuffer.AsSpan(dataOffset, 4));
                    var entryKeySpan = pageBuffer.AsSpan(dataOffset + 4, keyLength);

                    if (IndexKey.CompareRaw(entryKeySpan, minKey.Data) >= 0)
                    {
                        if (IndexKey.CompareRaw(entryKeySpan, maxKey.Data) <= 0)
                        {
                            location = DocumentLocation.ReadFrom(
                                pageBuffer.AsSpan(dataOffset + 4 + keyLength, DocumentLocation.SerializedSize));
                            return location.PageId != 0;
                        }
                        // entryKey > maxKey — past the range, no match possible
                        location = default;
                        return false;
                    }

                    dataOffset += 4 + keyLength + DocumentLocation.SerializedSize;
                }

                leafPageId = header.NextLeafPageId;
            }
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(pageBuffer);
        }

        location = default;
        return false;
    }

    /// <summary>
    /// Finds the leaf to start from for <paramref name="key"/>. A separator has keys lower than
    /// or equal to it on its left and keys greater than or equal to it on its right: a leaf split
    /// promotes the first key of the right half, and in a non-unique index copies of that key
    /// can stay in the left half. So which way to go on a key equal to a separator depends on
    /// the direction the caller then walks the leaf chain (#213). <paramref name="leftmost"/>
    /// goes left and lands on the first leaf that can hold the key, where a forward scan or a
    /// lookup starts; otherwise the descent goes right and lands on the last such leaf, where a
    /// backward scan starts and an insert adds the key.
    /// </summary>
    internal uint FindLeafNode(IndexKey key, ulong transactionId, bool leftmost)
    {
        var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            return FindLeafNodeWithPath(key, null, transactionId, pageBuffer, leftmost);
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(pageBuffer);
        }
    }

    /// <summary>
    /// Descends like <see cref="FindLeafNode"/>, adds the internal nodes passed on the way to
    /// <paramref name="path"/> (when given) and leaves the leaf page in <paramref name="pageBuffer"/>.
    /// </summary>
    private uint FindLeafNodeWithPath(IndexKey key, List<uint>? path, ulong transactionId, byte[] pageBuffer, bool leftmost)
    {
        var currentPageId = _rootPageId;
        while (true)
        {
            ReadPage(currentPageId, transactionId, pageBuffer);
            var header = BTreeNodeHeader.ReadFrom(pageBuffer.AsSpan(32));

            if (header.IsLeaf)
                return currentPageId;

            path?.Add(currentPageId);
            currentPageId = FindChildNode(pageBuffer, header, key, leftmost);
        }
    }

    private static uint FindChildNode(Span<byte> nodeBuffer, BTreeNodeHeader header, IndexKey key, bool leftmost)
    {
        // Internal Node Format:
        // [Header]
        // [P0 (4 bytes)] - Pointer to child with keys <= Key1
        // [Entry 1: Key1, P1] - P1 has keys >= Key1 (and <= Key2)
        // [Entry 2: Key2, P2]
        // ...

        // Build offset table for binary search (O(N), zero-alloc)
        Span<int> offsets = stackalloc int[header.EntryCount];
        BuildInternalOffsets(nodeBuffer, offsets);
        return ChildPointer(nodeBuffer, offsets, FindChildIndex(nodeBuffer, offsets, key, leftmost));
    }

    /// <summary>Fills <paramref name="offsets"/> with the offset of each entry of an internal node.</summary>
    private static void BuildInternalOffsets(ReadOnlySpan<byte> nodeBuffer, Span<int> offsets)
    {
        int off = NodeDataStart + 4; // after P0
        for (int i = 0; i < offsets.Length; i++)
        {
            offsets[i] = off;
            int keyLength = BitConverter.ToInt32(nodeBuffer.Slice(off, 4));
            off += InternalEntrySize(keyLength);
        }
    }

    /// <summary>
    /// Which child of an internal node to descend to for <paramref name="key"/>: 0 for P0, i for
    /// the child pointer of entry i. See <see cref="FindLeafNode"/> for <paramref name="leftmost"/>.
    /// </summary>
    private static int FindChildIndex(ReadOnlySpan<byte> nodeBuffer, ReadOnlySpan<int> offsets, IndexKey key, bool leftmost)
    {
        // Binary search for the first separator above the key (leftmost: not below it);
        // the child on its left is the one to follow.
        var keyData = key.Data;
        int lo = 0, hi = offsets.Length - 1;
        int firstAbove = offsets.Length;
        while (lo <= hi)
        {
            int mid = (lo + hi) >> 1;
            int midOff = offsets[mid];
            int midKeyLen = BitConverter.ToInt32(nodeBuffer.Slice(midOff, 4));
            int cmp = IndexKey.CompareRaw(keyData, nodeBuffer.Slice(midOff + 4, midKeyLen));
            if (cmp < 0 || (leftmost && cmp == 0))
            { firstAbove = mid; hi = mid - 1; }
            else { lo = mid + 1; }
        }
        return firstAbove;
    }

    /// <summary>The page of child <paramref name="childIndex"/> (as returned by <see cref="FindChildIndex"/>).</summary>
    private static uint ChildPointer(ReadOnlySpan<byte> nodeBuffer, ReadOnlySpan<int> offsets, int childIndex)
    {
        if (childIndex == 0)
            return BitConverter.ToUInt32(nodeBuffer.Slice(NodeDataStart, 4));
        int entryOff = offsets[childIndex - 1];
        int keyLen = BitConverter.ToInt32(nodeBuffer.Slice(entryOff, 4));
        return BitConverter.ToUInt32(nodeBuffer.Slice(entryOff + 4 + keyLen, 4));
    }

    public IBTreeCursor CreateCursor(ulong transactionId)
    {
        return new BTreeCursor(this, _storage, transactionId);
    }

    // --- Query Primitives ---

    public IEnumerable<IndexEntry> Equal(IndexKey key, ulong transactionId)
    {
        using var cursor = CreateCursor(transactionId);
        if (cursor.Seek(key))
        {
            yield return cursor.Current;
            // Handle duplicates if we support them? Current impl looks unique-ish per key unless multi-value index.
            // BTreeIndex doesn't strictly prevent duplicates in structure, but usually unique keys.
            // If unique, yield one. If not, loop.
            // Assuming unique for now based on TryFind.
        }
    }

    public IEnumerable<IndexEntry> GreaterThan(IndexKey key, bool orEqual, ulong transactionId)
    {
        using var cursor = CreateCursor(transactionId);
        bool found = cursor.Seek(key); 

        if (found && !orEqual)
        {
             if (!cursor.MoveNext()) yield break;
        }
        
        // Loop forward
        do
        {
             yield return cursor.Current;
        } while (cursor.MoveNext());
    }

    public IEnumerable<IndexEntry> LessThan(IndexKey key, bool orEqual, ulong transactionId)
    {
        using var cursor = CreateCursor(transactionId);
        bool found = cursor.Seek(key);

        if (found && !orEqual)
        {
            if (!cursor.MovePrev()) yield break;
        }
        else if (!found)
        {
             // Seek landed on next greater (or invalid if end)
             // We want < key.
             // If Seek returns false, it is at Next Greater. 
             // So Current > Key.
             // MovePrev to get < Key.
             if (!cursor.MovePrev()) yield break; 
        }

        // Loop backward
        do
        {
             yield return cursor.Current;
        } while (cursor.MovePrev());
    }

    public IEnumerable<IndexEntry> Between(IndexKey start, IndexKey end, bool startInclusive, bool endInclusive, ulong transactionId)
    {
        using var cursor = CreateCursor(transactionId);
        bool found = cursor.Seek(start);

        if (found && !startInclusive)
        {
            if (!cursor.MoveNext()) yield break;
        }

        // Iterate while <= end
        do
        {
            var current = cursor.Current;
            if (current.Key > end) yield break;
            if (current.Key == end && !endInclusive) yield break;

            yield return current;

        } while (cursor.MoveNext());
    }

    public IEnumerable<IndexEntry> StartsWith(string prefix, ulong transactionId)
    {
        var startKey = IndexKey.Create(prefix);
        using var cursor = CreateCursor(transactionId);
        cursor.Seek(startKey); 
        
        do
        {
            var current = cursor.Current;
            string val = string.Empty;
            try { val = current.Key.As<string>(); } 
            catch { break; } 

            if (!val.StartsWith(prefix)) break;
            
            yield return current;

        } while (cursor.MoveNext());
    }

    public IEnumerable<IndexEntry> In(IEnumerable<IndexKey> keys, ulong transactionId)
    {
        var sortedKeys = keys.OrderBy(k => k);
        using var cursor = CreateCursor(transactionId);

        foreach (var key in sortedKeys)
        {
            if (cursor.Seek(key))
            {
                yield return cursor.Current;
            }
        }
    }
    
    public IEnumerable<IndexEntry> Like(string pattern, ulong transactionId)
    {
        string regexPattern = "^" + System.Text.RegularExpressions.Regex.Escape(pattern)
            .Replace("%", ".*")
            .Replace("_", ".") + "$";
            
        var regex = new System.Text.RegularExpressions.Regex(regexPattern, System.Text.RegularExpressions.RegexOptions.Compiled);
        
        string prefix = "";
        for (int i = 0; i < pattern.Length; i++)
        {
            if (pattern[i] == '%' || pattern[i] == '_') break;
            prefix += pattern[i];
        }

        using var cursor = CreateCursor(transactionId);

        if (!string.IsNullOrEmpty(prefix))
        {
            cursor.Seek(IndexKey.Create(prefix));
        }
        else
        {
            cursor.MoveToFirst();
        }

        do 
        {
            IndexEntry current = default;
            try { current = cursor.Current; } catch { break; } // Safe break if cursor invalid
            
            if (!string.IsNullOrEmpty(prefix))
            {
                try 
                { 
                    string val = current.Key.As<string>();
                    if (!val.StartsWith(prefix)) break;
                }
                catch { break; } 
            }
            
            bool match = false;
            try
            {
                match = regex.IsMatch(current.Key.As<string>());
            }
            catch 
            {
                 // Ignore mismatch types
            }
            
            if (match) yield return current;

        } while (cursor.MoveNext());
    }

    private void InsertIntoLeaf(uint leafPageId, IndexEntry entry, Span<byte> pageBuffer, List<uint> path, ulong transactionId)
    {
        var header = BTreeNodeHeader.ReadFrom(pageBuffer[32..]);
        int count = header.EntryCount;

        // Pass 1 — build offset table (O(N), zero-alloc).
        // Each variable-length entry is: [4-byte keyLen][keyData][6-byte location].
        Span<int> offsets = stackalloc int[count + 1];
        int off = 32 + 20;
        for (int i = 0; i < count; i++)
        {
            offsets[i] = off;
            int keyLen = BitConverter.ToInt32(pageBuffer.Slice(off, 4));
            off += 4 + keyLen + DocumentLocation.SerializedSize;
        }
        offsets[count] = off; // end-of-data sentinel

        // Pass 2 — binary search for sorted insertion point (O(log N), zero-alloc).
        var newKeyData = entry.Key.Data;
        int lo = 0, hi = count - 1;
        int insertIdx = count; // default: append at end
        while (lo <= hi)
        {
            int mid = (lo + hi) >> 1;
            int midOff = offsets[mid];
            int midKeyLen = BitConverter.ToInt32(pageBuffer.Slice(midOff, 4));
            if (IndexKey.CompareRaw(newKeyData, pageBuffer.Slice(midOff + 4, midKeyLen)) <= 0)
            { insertIdx = mid; hi = mid - 1; }
            else { lo = mid + 1; }
        }

        int insertOffset = offsets[insertIdx];
        int currentEnd = offsets[count];
        int entrySize = LeafEntrySize(newKeyData.Length);

        // A node splits when it has no room left for the entry, by count or by bytes: with
        // variable-length keys a page can fill up long before MaxEntriesPerNode (#217).
        if (count >= MaxEntriesPerNode || currentEnd + entrySize > _storage.PageSize)
        {
            SplitLeafAndInsert(leafPageId, header, pageBuffer, offsets, insertIdx, entry, path, transactionId);
            return;
        }

        // Shift existing data right (reverse byte copy for overlap safety, zero-alloc)
        if (insertOffset < currentEnd)
        {
            int bytesToShift = currentEnd - insertOffset;
            pageBuffer.Slice(insertOffset, bytesToShift).CopyTo(pageBuffer.Slice(insertOffset + entrySize));
        }

        // Write new entry at sorted insertion point
        int writePos = insertOffset;
        BitConverter.TryWriteBytes(pageBuffer.Slice(writePos, 4), newKeyData.Length);
        writePos += 4;
        newKeyData.CopyTo(pageBuffer.Slice(writePos, newKeyData.Length));
        writePos += newKeyData.Length;
        entry.Location.WriteTo(pageBuffer.Slice(writePos, DocumentLocation.SerializedSize));

        header.EntryCount++;
        header.WriteTo(pageBuffer.Slice(32, 20));
        WritePage(leafPageId, transactionId, pageBuffer);
    }

    /// <summary>
    /// Splits a leaf that has no room for <paramref name="entry"/> and inserts the entry into the
    /// half it belongs to. The split point is chosen over the existing entries plus the new one,
    /// so both halves are known to fit before anything is written, and by bytes rather than by
    /// count: with very uneven key sizes a count midpoint can leave one half larger than a page
    /// (#217). The first key of the right half is promoted to the parent.
    /// </summary>
    /// <param name="offsets">Entry offsets of the leaf in <paramref name="pageBuffer"/>, with the end of the data last.</param>
    /// <param name="insertIdx">Position of the new entry among the existing ones.</param>
    private void SplitLeafAndInsert(uint leafPageId, BTreeNodeHeader header, Span<byte> pageBuffer, ReadOnlySpan<int> offsets,
                                    int insertIdx, IndexEntry entry, List<uint> path, ulong transactionId)
    {
        int count = header.EntryCount;
        int total = count + 1;
        var newKeyData = entry.Key.Data;
        int newEntrySize = LeafEntrySize(newKeyData.Length);

        // Byte offset (relative to the data start) of each entry of the merged sequence:
        // the existing entries with the new one at insertIdx.
        Span<int> merged = stackalloc int[total + 1];
        for (int j = 0; j <= total; j++)
            merged[j] = j <= insertIdx
                ? offsets[j] - NodeDataStart
                : offsets[j - 1] - NodeDataStart + newEntrySize;

        int splitPoint = ChooseSplitPoint(merged, LeafEntrySpace, promotesMiddle: false);
        if (splitPoint < 0)
            throw new InvalidOperationException(
                $"B-Tree leaf {leafPageId} cannot be split so that both halves fit a {_storage.PageSize}-byte page; " +
                $"it holds keys larger than the {MaxKeySize}-byte limit.");

        var newEntryBytes = System.Buffers.ArrayPool<byte>.Shared.Rent(newEntrySize);
        var leftBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        var rightBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            var newEntry = newEntryBytes.AsSpan(0, newEntrySize);
            BitConverter.TryWriteBytes(newEntry.Slice(0, 4), newKeyData.Length);
            newKeyData.CopyTo(newEntry.Slice(4));
            entry.Location.WriteTo(newEntry.Slice(4 + newKeyData.Length));

            uint origNextLeaf = header.NextLeafPageId;
            var newNodeId = CreateNode(isLeaf: true, transactionId);

            // Right half: a new leaf linked between this one and its old successor.
            var right = rightBuffer.AsSpan(0, _storage.PageSize);
            right.Clear();
            new PageHeader
            {
                PageId = newNodeId, PageType = PageType.Index, FreeBytes = 0,
                NextPageId = 0, TransactionId = 0, Checksum = 0
            }.WriteTo(right);
            new BTreeNodeHeader
            {
                PageId = newNodeId, IsLeaf = true,
                EntryCount = (ushort)(total - splitPoint),
                ParentPageId = 0,
                NextLeafPageId = origNextLeaf,
                PrevLeafPageId = leafPageId
            }.WriteTo(right.Slice(32, 20));
            CopyMergedLeafEntries(pageBuffer, offsets, insertIdx, newEntry, splitPoint, total, right.Slice(NodeDataStart));
            WritePage(newNodeId, transactionId, right);

            // Left half: rewritten in place, keeping its page header.
            var left = leftBuffer.AsSpan(0, _storage.PageSize);
            left.Clear();
            pageBuffer.Slice(0, 32).CopyTo(left);
            CopyMergedLeafEntries(pageBuffer, offsets, insertIdx, newEntry, 0, splitPoint, left.Slice(NodeDataStart));
            header.EntryCount = (ushort)splitPoint;
            header.NextLeafPageId = newNodeId;
            header.WriteTo(left.Slice(32, 20));
            WritePage(leafPageId, transactionId, left);

            // Update the original next node's prev pointer to point to new right node
            if (origNextLeaf != 0)
                UpdatePrevPointer(origNextLeaf, newNodeId, transactionId);

            // Promote the first key of the right half.
            int promoteKeyLen = BitConverter.ToInt32(right.Slice(NodeDataStart, 4));
            var promoteKey = IndexKey.FromOwnedArray(right.Slice(NodeDataStart + 4, promoteKeyLen).ToArray());
            InsertIntoParent(leafPageId, promoteKey, newNodeId, path, transactionId);
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(newEntryBytes);
            System.Buffers.ArrayPool<byte>.Shared.Return(leftBuffer);
            System.Buffers.ArrayPool<byte>.Shared.Return(rightBuffer);
        }
    }

    /// <summary>
    /// Copies entries [<paramref name="from"/>, <paramref name="to"/>) of the merged sequence
    /// (the leaf entries at <paramref name="offsets"/> with <paramref name="newEntry"/> at
    /// <paramref name="insertIdx"/>) to <paramref name="destination"/>.
    /// </summary>
    private static void CopyMergedLeafEntries(ReadOnlySpan<byte> page, ReadOnlySpan<int> offsets, int insertIdx,
                                              ReadOnlySpan<byte> newEntry, int from, int to, Span<byte> destination)
    {
        int written = 0;

        // Existing entries before the new one keep their index.
        int a = from, b = Math.Min(to, insertIdx);
        if (a < b)
        {
            page.Slice(offsets[a], offsets[b] - offsets[a]).CopyTo(destination);
            written = offsets[b] - offsets[a];
        }

        if (from <= insertIdx && insertIdx < to)
        {
            newEntry.CopyTo(destination.Slice(written));
            written += newEntry.Length;
        }

        // Existing entries after the new one: merged index j is existing index j - 1.
        a = Math.Max(from, insertIdx + 1) - 1;
        b = to - 1;
        if (a < b)
            page.Slice(offsets[a], offsets[b] - offsets[a]).CopyTo(destination.Slice(written));
    }

    /// <summary>
    /// Picks where to split a node whose entries, in order, start at the byte offsets in
    /// <paramref name="offsets"/> (the last value is the end of the data). Both halves must be
    /// non-empty, hold at most <see cref="MaxEntriesPerNode"/> entries and fit in
    /// <paramref name="capacity"/> bytes; among those split points the one that balances the
    /// bytes best wins, which for keys of one size is the count midpoint.
    /// With <paramref name="promotesMiddle"/> (internal nodes) the entry at the split point moves
    /// up to the parent and belongs to neither half. Returns -1 when no split point fits, which
    /// cannot happen while every entry takes at most half of <paramref name="capacity"/>
    /// (see <see cref="MaxKeySize"/>).
    /// </summary>
    private static int ChooseSplitPoint(ReadOnlySpan<int> offsets, int capacity, bool promotesMiddle)
    {
        int count = offsets.Length - 1;
        int end = offsets[count] - offsets[0];
        int skip = promotesMiddle ? 1 : 0;
        int best = -1, bestImbalance = int.MaxValue;
        for (int k = 1; k + skip < count; k++)
        {
            int leftBytes = offsets[k] - offsets[0];
            int rightBytes = end - (offsets[k + skip] - offsets[0]);
            if (k > MaxEntriesPerNode || count - k - skip > MaxEntriesPerNode) continue;
            if (leftBytes > capacity || rightBytes > capacity) continue;
            int imbalance = Math.Abs(leftBytes - rightBytes);
            if (imbalance < bestImbalance) { best = k; bestImbalance = imbalance; }
        }
        return best;
    }

    private void UpdatePrevPointer(uint pageId, uint newPrevId, ulong transactionId)
    {
        var buffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            ReadPage(pageId, transactionId, buffer);
            var header = BTreeNodeHeader.ReadFrom(buffer.AsSpan(32));
            header.PrevLeafPageId = newPrevId;
            header.WriteTo(buffer.AsSpan(32, 20)); // Write back updated header
            WritePage(pageId, transactionId, buffer.AsSpan(0, _storage.PageSize));
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private void InsertIntoParent(uint leftChildPageId, IndexKey key, uint rightChildPageId, List<uint> path, ulong transactionId)
    {
        if (path.Count == 0 || path.Last() == leftChildPageId)
        {
            // Root split (or weird path state where last is current)
            // If path.Last == leftChild, we need to pop it to get parent.
            // But if path is empty or contains ONLY the current node, it's a root split.
            if (path.Count > 0 && path.Last() == leftChildPageId)
                path.RemoveAt(path.Count - 1);

            if (path.Count == 0)
            {
                CreateNewRoot(leftChildPageId, key, rightChildPageId, transactionId);
                return;
            }
        }

        var parentPageId = path.Last();
        path.RemoveAt(path.Count - 1); // Pop parent for recursive calls

        var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            ReadPage(parentPageId, transactionId, pageBuffer);
            var header = BTreeNodeHeader.ReadFrom(pageBuffer.AsSpan(32));

            // Read all, add the new entry right after the one pointing at the split child, then
            // write back as one node or two. The position comes from the child, not from the
            // key: in a non-unique index several separators can be equal to the key (#213).
            var (p0, entries) = ReadInternalEntries(pageBuffer.AsSpan(0, _storage.PageSize), header.EntryCount);
            int insertIndex = p0 == leftChildPageId ? 0 : entries.FindIndex(e => e.PageId == leftChildPageId) + 1;
            if (insertIndex == 0 && p0 != leftChildPageId)
                throw new InvalidOperationException($"B-Tree node {parentPageId} does not point at its split child {leftChildPageId}.");
            entries.Insert(insertIndex, new InternalEntry(key, rightChildPageId));

            // Byte offset of each entry relative to the first one, then the end of the data.
            Span<int> offsets = stackalloc int[entries.Count + 1];
            for (int i = 0; i < entries.Count; i++)
                offsets[i + 1] = offsets[i] + InternalEntrySize(entries[i].Key.Data.Length);

            // Like a leaf, an internal node splits by count or by bytes (#217).
            if (entries.Count <= MaxEntriesPerNode && offsets[entries.Count] <= InternalEntrySpace)
            {
                WriteInternalNode(parentPageId, p0, entries, transactionId);
                return;
            }

            // The entry at the split point moves up: its key becomes the separator in the
            // grandparent and its child becomes P0 of the new right node.
            int splitPoint = ChooseSplitPoint(offsets, InternalEntrySpace, promotesMiddle: true);
            if (splitPoint < 0)
                throw new InvalidOperationException(
                    $"B-Tree node {parentPageId} cannot be split so that both halves fit a {_storage.PageSize}-byte page; " +
                    $"it holds keys larger than the {MaxKeySize}-byte limit.");

            var promoteKey = entries[splitPoint].Key;
            var rightP0 = entries[splitPoint].PageId;
            var leftEntries = entries.GetRange(0, splitPoint);
            var rightEntries = entries.GetRange(splitPoint + 1, entries.Count - splitPoint - 1);

            var newParentId = CreateNode(isLeaf: false, transactionId);

            WriteInternalNode(parentPageId, p0, leftEntries, transactionId);
            WriteInternalNode(newParentId, rightP0, rightEntries, transactionId);

            InsertIntoParent(parentPageId, promoteKey, newParentId, path, transactionId);
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(pageBuffer);
        }
    }

    /// <summary>
    /// Grows the tree by one level after the root was split, keeping the root page id fixed:
    /// the root's current content (the left half of the split) moves to a freshly allocated
    /// page and the root page is rewritten in place as an internal node pointing at both halves.
    /// </summary>
    private void CreateNewRoot(uint leftChildId, IndexKey key, uint rightChildId, ulong transactionId)
    {
        if (leftChildId != _rootPageId)
            throw new InvalidOperationException("CreateNewRoot must be called with the root page as left child.");

        var leftCopyId = _storage.AllocateIndexPage();
        var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            ReadPage(_rootPageId, transactionId, pageBuffer);
            var page = pageBuffer.AsSpan(0, _storage.PageSize);
            var nodeHeader = RelocateNode(page, leftCopyId);
            WritePage(leftCopyId, transactionId, page);

            // The old root was a leaf: its right sibling (the other half) points back at it.
            if (nodeHeader.IsLeaf && nodeHeader.NextLeafPageId != 0)
                UpdatePrevPointer(nodeHeader.NextLeafPageId, leftCopyId, transactionId);
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(pageBuffer);
        }

        var entries = new List<InternalEntry> { new InternalEntry(key, rightChildId) };
        WriteInternalNode(_rootPageId, leftCopyId, entries, transactionId);
    }

    /// <summary>
    /// Shrinks the tree by one level when the root is left with a single child, keeping the
    /// root page id fixed: the child's content is copied into the root page and the child freed.
    /// </summary>
    private void CollapseRootInto(uint soleChildId, ulong transactionId)
    {
        var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            ReadPage(soleChildId, transactionId, pageBuffer);
            var page = pageBuffer.AsSpan(0, _storage.PageSize);
            RelocateNode(page, _rootPageId);
            WritePage(_rootPageId, transactionId, page);
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(pageBuffer);
        }

        // The sole child is intentionally left orphaned rather than freed: FreePage acts on disk
        // immediately, while the copy of this page written by MergeNodes still sits in the
        // transaction cache and the committed root still points at it until commit. Freeing it
        // here would let a reallocation be overwritten by the stale image on commit and would
        // leave the committed tree pointing at a free page if the transaction rolls back.
    }

    /// <summary>Rewrites the page and node headers of a node image so it lives at <paramref name="newPageId"/>.</summary>
    private static BTreeNodeHeader RelocateNode(Span<byte> page, uint newPageId)
    {
        var pageHeader = PageHeader.ReadFrom(page);
        pageHeader.PageId = newPageId;
        pageHeader.WriteTo(page);

        var nodeHeader = BTreeNodeHeader.ReadFrom(page.Slice(32));
        nodeHeader.PageId = newPageId;
        nodeHeader.WriteTo(page.Slice(32, 20));
        return nodeHeader;
    }

    private uint CreateNode(bool isLeaf, ulong transactionId)
    {
        var pageId = _storage.AllocateIndexPage();
        var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            Array.Clear(pageBuffer, 0, _storage.PageSize);

            // Write page header
            var pageHeader = new PageHeader
            {
                PageId = pageId,
                PageType = PageType.Index,
                FreeBytes = (ushort)(_storage.PageSize - 32 - 20),
                NextPageId = 0,
                TransactionId = 0,
                Checksum = 0
            };
            pageHeader.WriteTo(pageBuffer);

            // Write B+Tree node header
            var nodeHeader = new BTreeNodeHeader
            {
                PageId = pageId,
                IsLeaf = isLeaf,
                EntryCount = 0,
                ParentPageId = 0,
                NextLeafPageId = 0
            };
            nodeHeader.WriteTo(pageBuffer.AsSpan(32, 20));

            // Write page
            WritePage(pageId, transactionId, pageBuffer.AsSpan(0, _storage.PageSize));
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(pageBuffer);
        }

        return pageId;
    }

    private List<IndexEntry> ReadLeafEntries(Span<byte> pageBuffer, int count)
    {
        var entries = new List<IndexEntry>(count);
        var dataOffset = 32 + 20;

        for (int i = 0; i < count; i++)
        {
            var key = ReadIndexKey(pageBuffer, dataOffset);
            var locationOffset = dataOffset + 4 + key.Data.Length;
            var location = DocumentLocation.ReadFrom(pageBuffer.Slice(locationOffset, DocumentLocation.SerializedSize));
            entries.Add(new IndexEntry(key, location));
            dataOffset = locationOffset + DocumentLocation.SerializedSize;
        }
        return entries;
    }

    private (uint P0, List<InternalEntry> Entries) ReadInternalEntries(Span<byte> pageBuffer, int count)
    {
        var entries = new List<InternalEntry>(count);
        var dataOffset = 32 + 20;

        var p0 = BitConverter.ToUInt32(pageBuffer.Slice(dataOffset, 4));
        dataOffset += 4;

        for (int i = 0; i < count; i++)
        {
            var key = ReadIndexKey(pageBuffer, dataOffset);
            var ptrOffset = dataOffset + 4 + key.Data.Length;
            var pageId = BitConverter.ToUInt32(pageBuffer.Slice(ptrOffset, 4));
            entries.Add(new InternalEntry(key, pageId));
            dataOffset = ptrOffset + 4;
        }
        return (p0, entries);
    }

    private void WriteLeafNode(uint pageId, List<IndexEntry> entries, uint nextLeafId, uint prevLeafId, ulong? transactionId = null)
    {
        var txnId = transactionId ?? 0;
        var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            Array.Clear(pageBuffer, 0, _storage.PageSize);

            // Re-write headers
            var pageHeader = new PageHeader
            {
                PageId = pageId,
                PageType = PageType.Index,
                FreeBytes = 0, // Simplified
                NextPageId = 0,
                TransactionId = 0,
                Checksum = 0
            };
            pageHeader.WriteTo(pageBuffer);

            var nodeHeader = new BTreeNodeHeader
            {
                PageId = pageId,
                IsLeaf = true,
                EntryCount = (ushort)entries.Count,
                ParentPageId = 0, // Parent pointer is not persisted; traversal is always top-down.
                NextLeafPageId = nextLeafId,
                PrevLeafPageId = prevLeafId
            };
            nodeHeader.WriteTo(pageBuffer.AsSpan(32, 20));

            // Write entries with DocumentLocation (6 bytes instead of ObjectId 12 bytes)
            var dataOffset = 32 + 20;
            foreach (var entry in entries)
            {
                BitConverter.TryWriteBytes(pageBuffer.AsSpan(dataOffset, 4), entry.Key.Data.Length);
                entry.Key.Data.CopyTo(pageBuffer.AsSpan(dataOffset + 4));
                entry.Location.WriteTo(pageBuffer.AsSpan(dataOffset + 4 + entry.Key.Data.Length));
                dataOffset += 4 + entry.Key.Data.Length + DocumentLocation.SerializedSize;
            }

            // Write page
            WritePage(pageId, txnId, pageBuffer.AsSpan(0, _storage.PageSize));
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(pageBuffer);
        }
    }

    private void WriteInternalNode(uint pageId, uint p0, List<InternalEntry> entries, ulong transactionId)
    {
        var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            Array.Clear(pageBuffer, 0, _storage.PageSize);

            // Re-write headers
            var pageHeader = new PageHeader
            {
                PageId = pageId,
                PageType = PageType.Index,
                FreeBytes = 0,
                NextPageId = 0,
                TransactionId = 0,
                Checksum = 0
            };
            pageHeader.WriteTo(pageBuffer);

            var nodeHeader = new BTreeNodeHeader
            {
                PageId = pageId,
                IsLeaf = false,
                EntryCount = (ushort)entries.Count,
                ParentPageId = 0,
                NextLeafPageId = 0
            };
            nodeHeader.WriteTo(pageBuffer.AsSpan(32, 20));

            // Write P0
            var dataOffset = 32 + 20;
            BitConverter.TryWriteBytes(pageBuffer.AsSpan(dataOffset, 4), p0);
            dataOffset += 4;

            // Write entries
            foreach (var entry in entries)
            {
                BitConverter.TryWriteBytes(pageBuffer.AsSpan(dataOffset, 4), entry.Key.Data.Length);
                entry.Key.Data.CopyTo(pageBuffer.AsSpan(dataOffset + 4));
                BitConverter.TryWriteBytes(pageBuffer.AsSpan(dataOffset + 4 + entry.Key.Data.Length, 4), entry.PageId);
                dataOffset += 4 + entry.Key.Data.Length + 4;
            }

            // Write page
            WritePage(pageId, transactionId, pageBuffer.AsSpan(0, _storage.PageSize));
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(pageBuffer);
        }
    }

    /// <summary>Fills <paramref name="offsets"/> with the offset of each leaf entry, then the end of the data.</summary>
    private static void BuildLeafOffsets(ReadOnlySpan<byte> pageBuffer, Span<int> offsets)
    {
        int off = NodeDataStart;
        for (int i = 0; i < offsets.Length - 1; i++)
        {
            offsets[i] = off;
            off += LeafEntrySize(BitConverter.ToInt32(pageBuffer.Slice(off, 4)));
        }
        offsets[offsets.Length - 1] = off;
    }

    /// <summary>Index of the first leaf entry whose key is not below <paramref name="keyData"/> (the entry count if none).</summary>
    private static int LeafLowerBound(ReadOnlySpan<byte> pageBuffer, ReadOnlySpan<int> offsets, ReadOnlySpan<byte> keyData)
    {
        int lo = 0, hi = offsets.Length - 2;
        int result = offsets.Length - 1;
        while (lo <= hi)
        {
            int mid = (lo + hi) >> 1;
            int midOff = offsets[mid];
            int midKeyLen = BitConverter.ToInt32(pageBuffer.Slice(midOff, 4));
            if (IndexKey.CompareRaw(keyData, pageBuffer.Slice(midOff + 4, midKeyLen)) <= 0)
            { result = mid; hi = mid - 1; }
            else { lo = mid + 1; }
        }
        return result;
    }

    /// <summary>
    /// Looks <paramref name="key"/> up in the leaf in <paramref name="pageBuffer"/>, returning
    /// <c>false</c> when every key in the leaf is smaller and the lookup must go on in the next
    /// leaf; otherwise <paramref name="found"/> says whether the first key not below it matches.
    /// </summary>
    private static bool SearchLeafLowerBound(ReadOnlySpan<byte> pageBuffer, int entryCount, IndexKey key,
                                             out bool found, out DocumentLocation location)
    {
        found = false;
        location = default;
        Span<int> offsets = stackalloc int[entryCount + 1];
        BuildLeafOffsets(pageBuffer, offsets);
        int idx = LeafLowerBound(pageBuffer, offsets, key.Data);
        if (idx == entryCount) return false;

        int off = offsets[idx];
        int keyLen = BitConverter.ToInt32(pageBuffer.Slice(off, 4));
        if (IndexKey.CompareRaw(key.Data, pageBuffer.Slice(off + 4, keyLen)) == 0)
        {
            found = true;
            location = DocumentLocation.ReadFrom(pageBuffer.Slice(off + 4 + keyLen, DocumentLocation.SerializedSize));
        }
        return true;
    }

    /// <summary>Index of the leaf entry (<paramref name="key"/>, <paramref name="location"/>), or -1.</summary>
    private static int FindEntryInLeaf(ReadOnlySpan<byte> pageBuffer, ReadOnlySpan<int> offsets, IndexKey key, DocumentLocation location)
    {
        var keyData = key.Data;
        for (int i = LeafLowerBound(pageBuffer, offsets, keyData); i < offsets.Length - 1; i++)
        {
            int off = offsets[i];
            int keyLen = BitConverter.ToInt32(pageBuffer.Slice(off, 4));
            if (IndexKey.CompareRaw(keyData, pageBuffer.Slice(off + 4, keyLen)) != 0) break;
            var loc = DocumentLocation.ReadFrom(pageBuffer.Slice(off + 4 + keyLen, DocumentLocation.SerializedSize));
            if (loc.PageId == location.PageId && loc.SlotIndex == location.SlotIndex) return i;
        }
        return -1;
    }

    /// <summary>
    /// Finds the leaf that holds the entry (<paramref name="key"/>, <paramref name="location"/>)
    /// in the subtree at <paramref name="pageId"/>, adding its internal ancestors to
    /// <paramref name="path"/> and leaving the leaf page in <paramref name="pageBuffer"/>.
    /// In a non-unique index the copies of a key can span several children of a node, from the
    /// leftmost child that can hold the key to the rightmost one, so those subtrees are searched
    /// in order (#213); usually that is a single child. Returns 0, with <paramref name="path"/>
    /// unchanged, when the entry is not in the subtree.
    /// </summary>
    private uint FindLeafHoldingEntry(uint pageId, IndexKey key, DocumentLocation location, List<uint> path,
                                      ulong transactionId, byte[] pageBuffer)
    {
        ReadPage(pageId, transactionId, pageBuffer);
        var header = BTreeNodeHeader.ReadFrom(pageBuffer.AsSpan(32));
        int count = header.EntryCount;

        if (header.IsLeaf)
        {
            Span<int> leafOffsets = stackalloc int[count + 1];
            BuildLeafOffsets(pageBuffer, leafOffsets);
            return FindEntryInLeaf(pageBuffer, leafOffsets, key, location) >= 0 ? pageId : 0;
        }

        Span<int> offsets = stackalloc int[count];
        BuildInternalOffsets(pageBuffer, offsets);
        int first = FindChildIndex(pageBuffer, offsets, key, leftmost: true);
        int last = FindChildIndex(pageBuffer, offsets, key, leftmost: false);

        // The buffer is reused by the recursive calls: take the child pointers out first.
        Span<uint> children = stackalloc uint[last - first + 1];
        for (int i = 0; i < children.Length; i++)
            children[i] = ChildPointer(pageBuffer, offsets, first + i);

        path.Add(pageId);
        foreach (var child in children)
        {
            var leafPageId = FindLeafHoldingEntry(child, key, location, path, transactionId, pageBuffer);
            if (leafPageId != 0) return leafPageId;
        }
        path.RemoveAt(path.Count - 1);
        return 0;
    }


    /// <summary>
    /// Binary search a leaf page buffer for an exact key match.
    /// Zero-allocation: no IndexKey objects or byte[] copies are created.
    /// </summary>
    private static (bool Found, DocumentLocation Location) BinarySearchLeaf(
        ReadOnlySpan<byte> pageBuffer, int entryCount, IndexKey key)
    {
        if (entryCount == 0) return (false, default);

        Span<int> offsets = stackalloc int[entryCount];
        int off = 32 + 20;
        for (int i = 0; i < entryCount; i++)
        {
            offsets[i] = off;
            int keyLen = BitConverter.ToInt32(pageBuffer.Slice(off, 4));
            off += 4 + keyLen + DocumentLocation.SerializedSize;
        }

        var keyData = key.Data;
        int lo = 0, hi = entryCount - 1;
        while (lo <= hi)
        {
            int mid = (lo + hi) >> 1;
            int midOff = offsets[mid];
            int midKeyLen = BitConverter.ToInt32(pageBuffer.Slice(midOff, 4));
            int cmp = IndexKey.CompareRaw(keyData, pageBuffer.Slice(midOff + 4, midKeyLen));
            if (cmp == 0)
            {
                return (true, DocumentLocation.ReadFrom(pageBuffer.Slice(midOff + 4 + midKeyLen, DocumentLocation.SerializedSize)));
            }
            if (cmp < 0) hi = mid - 1;
            else lo = mid + 1;
        }

        return (false, default);
    }

    /// <summary>
    /// Array overload — callable from async methods without exposing Span locals.
    /// </summary>
    private static bool SearchLeafLowerBoundArray(
        byte[] pageBuffer, int entryCount, IndexKey key, out bool found, out DocumentLocation location)
        => SearchLeafLowerBound(pageBuffer, entryCount, key, out found, out location);

    /// <summary>
    /// Returns the byte offset just past the last leaf entry.
    /// </summary>
    private static int LeafDataEnd(byte[] buffer, int entryCount)
    {
        int off = 32 + 20;
        for (int i = 0; i < entryCount; i++)
        {
            int keyLen = BitConverter.ToInt32(buffer.AsSpan(off, 4));
            off += 4 + keyLen + DocumentLocation.SerializedSize;
        }
        return off;
    }

    private IndexKey ReadIndexKey(Span<byte> buffer, int offset)
    {
        var keyLength = BitConverter.ToInt32(buffer.Slice(offset, 4));
        var keyData = buffer.Slice(offset + 4, keyLength);
        return IndexKey.FromOwnedArray(keyData.ToArray());
    }

    private IndexKey ReadIndexKey(byte[] buffer, int offset)
    {
        var keyLength = BitConverter.ToInt32(buffer.AsSpan(offset, 4));
        var keyData = buffer.AsSpan(offset + 4, keyLength);
        return IndexKey.FromOwnedArray(keyData.ToArray());
    }

    /// <summary>
    /// Deletes a key-location pair from the index
    /// </summary>
    public bool Delete(IndexKey key, DocumentLocation location, ulong? transactionId = null)
    {
        var txnId = transactionId ?? 0;
        var path = new List<uint>();

        var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            // Copies of the key may sit on either side of an equal separator (#213): find the
            // leaf that holds this exact entry, which leaves it in pageBuffer.
            var leafPageId = FindLeafHoldingEntry(_rootPageId, key, location, path, txnId, pageBuffer);
            if (leafPageId == 0)
                return false;

            var header = BTreeNodeHeader.ReadFrom(pageBuffer.AsSpan(32));
            int count = header.EntryCount;

            // Build offset table
            Span<int> offsets = stackalloc int[count + 1];
            BuildLeafOffsets(pageBuffer, offsets);

            int deleteIdx = FindEntryInLeaf(pageBuffer, offsets, key, location);
            if (deleteIdx == -1)
                return false;

            // Shift left to close gap (forward copy — safe since destination < source)
            int entryStart = offsets[deleteIdx];
            int entryEnd = offsets[deleteIdx + 1];
            int entrySize = entryEnd - entryStart;
            int dataEnd = offsets[count];
            int tailBytes = dataEnd - entryEnd;
            if (tailBytes > 0)
            {
                pageBuffer.AsSpan(entryEnd, tailBytes).CopyTo(pageBuffer.AsSpan(entryStart, tailBytes));
            }
            // Clear freed space
            pageBuffer.AsSpan(dataEnd - entrySize, entrySize).Clear();

            int newCount = count - 1;
            header.EntryCount = (ushort)newCount;
            header.WriteTo(pageBuffer.AsSpan(32, 20));
            WritePage(leafPageId, txnId, pageBuffer.AsSpan(0, _storage.PageSize));

            // Check for underflow
            int minEntries = MaxEntriesPerNode / 2;
            if (newCount < minEntries && _rootPageId != leafPageId)
            {
                HandleUnderflow(leafPageId, path, txnId);
            }

            return true;
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(pageBuffer);
        }
    }

    private void HandleUnderflow(uint nodeId, List<uint> path, ulong transactionId)
    {
        if (path.Count == 0)
        {
            // Node is root
            if (nodeId == _rootPageId)
            {
                // Special case: Collapse root if it has only 1 child (and is not a leaf)
                // For now, simpliest implementation: do nothing for root underflow unless it's empty
                // If it's a leaf root, it can be empty.
                return;
            }
        }

        var parentPageId = path[^1]; // Parent is last in path (before current node removed? No, path contains ancestors)
                                     // Wait, FindLeafNodeWithPath adds ancestors. So path.Last() is not current node, it's parent.
                                     // Let's verify FindLeafNodeWithPath:
                                     // path.Add(currentPageId); currentPageId = FindChildNode(...);
                                     // It adds PARENTS. It does NOT add the leaf itself.

        // Correct.
        // So path.Last() is the parent.

        var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            ReadPage(parentPageId, transactionId, pageBuffer);
            var parentHeader = BTreeNodeHeader.ReadFrom(pageBuffer.AsSpan(32));
            var (p0, parentEntries) = ReadInternalEntries(pageBuffer, parentHeader.EntryCount);

            // Find index of current node in parent
            int childIndex = -1;
            if (p0 == nodeId) childIndex = -1; // -1 indicates P0
            else
            {
                childIndex = parentEntries.FindIndex(e => e.PageId == nodeId);
            }

            // Try to borrow from siblings
            if (BorrowFromSibling(nodeId, parentPageId, childIndex, parentEntries, p0, transactionId))
            {
                return; // Rebalanced
            }

            // Borrow failed, valid siblings are too small -> MERGE
            MergeWithSibling(nodeId, parentPageId, childIndex, parentEntries, p0, path, transactionId);
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(pageBuffer);
        }
    }

    /// <summary>
    /// Moves one entry from a sibling with more than the minimum into the underfull node. Besides
    /// the entry count, the moved entry must fit in the node and the new separator in the parent:
    /// with variable-length keys either can be larger than what it replaces (#217). A borrow that
    /// would not fit is skipped.
    /// </summary>
    private bool BorrowFromSibling(uint nodeId, uint parentId, int childIndex, List<InternalEntry> parentEntries, uint p0, ulong transactionId)
    {
        int minEntries = MaxEntriesPerNode / 2;
        int parentBytes = InternalEntriesSize(parentEntries);
        var nodeBuffer    = System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        var siblingBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            ReadPage(nodeId, transactionId, nodeBuffer);
            var nodeHeader = BTreeNodeHeader.ReadFrom(nodeBuffer.AsSpan(32));

            // ── Try borrow from RIGHT sibling ─────────────────────────────────────
            // childIndex == -1  → current is P0;   right sibling = entries[0]
            // childIndex >= 0   → current is entries[childIndex]; right = entries[childIndex+1]
            uint rightSiblingId = 0;
            int  rightSepIdx    = -1;

            if (childIndex == -1 && parentEntries.Count > 0)
            {
                rightSiblingId = parentEntries[0].PageId;
                rightSepIdx    = 0;
            }
            else if (childIndex >= 0 && childIndex < parentEntries.Count - 1)
            {
                rightSiblingId = parentEntries[childIndex + 1].PageId;
                rightSepIdx    = childIndex + 1;
            }

            if (rightSiblingId != 0)
            {
                ReadPage(rightSiblingId, transactionId, siblingBuffer);
                var rightHeader = BTreeNodeHeader.ReadFrom(siblingBuffer.AsSpan(32));
                int oldSepLen = parentEntries[rightSepIdx].Key.Data.Length;

                if (rightHeader.EntryCount > minEntries)
                {
                    if (nodeHeader.IsLeaf)
                    {
                        // ── In-place borrow-right for leaf nodes ──
                        // 1. Find data-end of current node
                        int nodeDataEnd = LeafDataEnd(nodeBuffer, nodeHeader.EntryCount);

                        // 2. Read first entry of right sibling (offset + size)
                        int rightFirstOff = 32 + 20;
                        int rightFirstKeyLen = BitConverter.ToInt32(siblingBuffer.AsSpan(rightFirstOff, 4));
                        int rightFirstSize = 4 + rightFirstKeyLen + DocumentLocation.SerializedSize;

                        // The second entry of the right sibling becomes the separator.
                        int newSepLen = BitConverter.ToInt32(siblingBuffer.AsSpan(rightFirstOff + rightFirstSize, 4));
                        if (nodeDataEnd + rightFirstSize <= _storage.PageSize
                            && parentBytes - oldSepLen + newSepLen <= InternalEntrySpace)
                        {
                            // 3. Copy that entry to end of current node
                            new Span<byte>(siblingBuffer, rightFirstOff, rightFirstSize)
                                .CopyTo(new Span<byte>(nodeBuffer, nodeDataEnd, rightFirstSize));

                            // 4. Shift right sibling data left to close gap
                            int rightDataEnd = LeafDataEnd(siblingBuffer, rightHeader.EntryCount);
                            int rightTailBytes = rightDataEnd - (rightFirstOff + rightFirstSize);
                            if (rightTailBytes > 0)
                            {
                                siblingBuffer.AsSpan(rightFirstOff + rightFirstSize, rightTailBytes)
                                    .CopyTo(siblingBuffer.AsSpan(rightFirstOff, rightTailBytes));
                            }
                            siblingBuffer.AsSpan(rightDataEnd - rightFirstSize, rightFirstSize).Clear();

                            // 5. Update headers
                            nodeHeader.EntryCount++;
                            nodeHeader.WriteTo(nodeBuffer.AsSpan(32, 20));
                            rightHeader.EntryCount--;
                            rightHeader.WriteTo(siblingBuffer.AsSpan(32, 20));

                            // 6. Parent separator = new first key of right sibling
                            int newRightFirstKeyLen = BitConverter.ToInt32(siblingBuffer.AsSpan(rightFirstOff, 4));
                            var newSepKey = IndexKey.FromOwnedArray(siblingBuffer.AsSpan(rightFirstOff + 4, newRightFirstKeyLen).ToArray());
                            parentEntries[rightSepIdx] = new InternalEntry(newSepKey, rightSiblingId);

                            WritePage(nodeId, transactionId, new ReadOnlySpan<byte>(nodeBuffer, 0, _storage.PageSize));
                            WritePage(rightSiblingId, transactionId, new ReadOnlySpan<byte>(siblingBuffer, 0, _storage.PageSize));
                            WriteInternalNode(parentId, p0, parentEntries, transactionId);
                            return true;
                        }
                    }
                    else
                    {
                        var (nodeP0,  nodeEntries)  = ReadInternalEntries(nodeBuffer,  nodeHeader.EntryCount);
                        var (rightP0, rightEntries) = ReadInternalEntries(siblingBuffer, rightHeader.EntryCount);

                        // Rotate left for internal node:
                        // Pull separator key down from parent; right's P0 becomes new last child of current.
                        var sep = parentEntries[rightSepIdx].Key;
                        if (InternalEntriesSize(nodeEntries) + InternalEntrySize(sep.Data.Length) <= InternalEntrySpace
                            && parentBytes - oldSepLen + rightEntries[0].Key.Data.Length <= InternalEntrySpace)
                        {
                            nodeEntries.Add(new InternalEntry(sep, rightP0));

                            // Right sibling's new P0 = its former first entry's PageId
                            var newRightP0 = rightEntries[0].PageId;
                            parentEntries[rightSepIdx] = new InternalEntry(rightEntries[0].Key, rightSiblingId);
                            rightEntries.RemoveAt(0);

                            WriteInternalNode(nodeId,         nodeP0,    nodeEntries,  transactionId);
                            WriteInternalNode(rightSiblingId, newRightP0, rightEntries, transactionId);
                            WriteInternalNode(parentId, p0, parentEntries, transactionId);
                            return true;
                        }
                    }
                }
            }

            // ── Try borrow from LEFT sibling ──────────────────────────────────────
            // childIndex == 0   → current is entries[0]; left sibling = P0
            // childIndex >  0   → current is entries[childIndex]; left = entries[childIndex-1]
            // (childIndex == -1 → current IS P0, no left sibling)
            uint leftSiblingId = 0;
            int  leftSepIdx    = -1;

            if (childIndex == 0)
            {
                leftSiblingId = p0;
                leftSepIdx    = 0;
            }
            else if (childIndex > 0)
            {
                leftSiblingId = parentEntries[childIndex - 1].PageId;
                leftSepIdx    = childIndex;
            }

            if (leftSiblingId != 0)
            {
                ReadPage(leftSiblingId, transactionId, siblingBuffer);
                var leftHeader = BTreeNodeHeader.ReadFrom(siblingBuffer.AsSpan(32));
                int oldSepLen = parentEntries[leftSepIdx].Key.Data.Length;

                if (leftHeader.EntryCount > minEntries)
                {
                    if (nodeHeader.IsLeaf)
                    {
                        // ── In-place borrow-left for leaf nodes ──
                        // 1. Find last entry of left sibling
                        int leftDataEnd = LeafDataEnd(siblingBuffer, leftHeader.EntryCount);
                        // Walk to last entry offset
                        int leftLastOff = 32 + 20;
                        {
                            int cur = leftLastOff;
                            for (int i = 0; i < leftHeader.EntryCount - 1; i++)
                            {
                                int kl = BitConverter.ToInt32(siblingBuffer.AsSpan(cur, 4));
                                cur += 4 + kl + DocumentLocation.SerializedSize;
                            }
                            leftLastOff = cur;
                        }
                        int lastEntryKeyLen = BitConverter.ToInt32(siblingBuffer.AsSpan(leftLastOff, 4));
                        int lastEntrySize = 4 + lastEntryKeyLen + DocumentLocation.SerializedSize;

                        int nodeDataEnd = LeafDataEnd(nodeBuffer, nodeHeader.EntryCount);
                        if (nodeDataEnd + lastEntrySize <= _storage.PageSize
                            && parentBytes - oldSepLen + lastEntryKeyLen <= InternalEntrySpace)
                        {
                            // 2. Shift current node data right by lastEntrySize
                            int nodeDataStart = 32 + 20;
                            int nodeDataLen = nodeDataEnd - nodeDataStart;
                            if (nodeDataLen > 0)
                            {
                                // Reverse copy for overlapping shift-right
                                for (int b = nodeDataLen - 1; b >= 0; b--)
                                    nodeBuffer[nodeDataStart + lastEntrySize + b] = nodeBuffer[nodeDataStart + b];
                            }

                            // 3. Copy left's last entry to start of current node
                            new Span<byte>(siblingBuffer, leftLastOff, lastEntrySize)
                                .CopyTo(new Span<byte>(nodeBuffer, nodeDataStart, lastEntrySize));

                            // 4. Truncate left sibling (clear removed entry)
                            siblingBuffer.AsSpan(leftLastOff, lastEntrySize).Clear();

                            // 5. Update headers
                            leftHeader.EntryCount--;
                            leftHeader.WriteTo(siblingBuffer.AsSpan(32, 20));
                            nodeHeader.EntryCount++;
                            nodeHeader.WriteTo(nodeBuffer.AsSpan(32, 20));

                            // 6. Parent separator = the borrowed key (now first key of current)
                            var borrowedKey = IndexKey.FromOwnedArray(nodeBuffer.AsSpan(nodeDataStart + 4, lastEntryKeyLen).ToArray());
                            parentEntries[leftSepIdx] = new InternalEntry(borrowedKey, nodeId);

                            WritePage(leftSiblingId, transactionId, new ReadOnlySpan<byte>(siblingBuffer, 0, _storage.PageSize));
                            WritePage(nodeId, transactionId, new ReadOnlySpan<byte>(nodeBuffer, 0, _storage.PageSize));
                            WriteInternalNode(parentId, p0, parentEntries, transactionId);
                            return true;
                        }
                    }
                    else
                    {
                        var (nodeP0, nodeEntries) = ReadInternalEntries(nodeBuffer,   nodeHeader.EntryCount);
                        var (leftP0, leftEntries) = ReadInternalEntries(siblingBuffer, leftHeader.EntryCount);

                        // Rotate right for internal node:
                        // Pull separator key down from parent; old nodeP0 becomes regular first entry of current.
                        var sep = parentEntries[leftSepIdx].Key;
                        var lastLeft = leftEntries[leftEntries.Count - 1];
                        if (InternalEntriesSize(nodeEntries) + InternalEntrySize(sep.Data.Length) <= InternalEntrySpace
                            && parentBytes - oldSepLen + lastLeft.Key.Data.Length <= InternalEntrySpace)
                        {
                            nodeEntries.Insert(0, new InternalEntry(sep, nodeP0));

                            // Left's last entry key becomes new parent separator; its PageId is new nodeP0
                            parentEntries[leftSepIdx] = new InternalEntry(lastLeft.Key, nodeId);
                            var newNodeP0 = lastLeft.PageId;
                            leftEntries.RemoveAt(leftEntries.Count - 1);

                            WriteInternalNode(leftSiblingId, leftP0,    leftEntries, transactionId);
                            WriteInternalNode(nodeId,        newNodeP0, nodeEntries, transactionId);
                            WriteInternalNode(parentId, p0, parentEntries, transactionId);
                            return true;
                        }
                    }
                }
            }

            return false;
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(nodeBuffer);
            System.Buffers.ArrayPool<byte>.Shared.Return(siblingBuffer);
        }
    }

    /// <summary>Bytes taken by the entries of an internal node, P0 excluded.</summary>
    private static int InternalEntriesSize(List<InternalEntry> entries)
    {
        int size = 0;
        foreach (var entry in entries)
            size += InternalEntrySize(entry.Key.Data.Length);
        return size;
    }

    private void MergeWithSibling(uint nodeId, uint parentId, int childIndex, List<InternalEntry> parentEntries, uint p0, List<uint> path, ulong transactionId)
    {
        // Identify sibling to merge with.
        // If P0 (childIndex -1), merge with right sibling (Entry 0).
        // If last child, merge with left sibling.
        // Otherwise, pick left or right.

        // An internal node left with P0 alone (its children could not be merged, see below)
        // has no sibling to merge with under this parent.
        if (parentEntries.Count == 0)
            return;

        IndexKey separatorKey;
        uint leftNodeId, rightNodeId;

        if (childIndex == -1) // Current is P0 (Leftmost)
        {
            // Merge with Entry 0 (Right sibling)
            rightNodeId = parentEntries[0].PageId;
            leftNodeId = nodeId;
            separatorKey = parentEntries[0].Key; // Key separating P0 and P1

            // Remove Entry 0 from parent (demote key)
            // But wait, the key moves DOWN into the merged node?
            // For leaf nodes: separator key is just a copy in parent.
            // For internal nodes: separator key moves down.
        }
        else
        {
            // Merge with left sibling
            if (childIndex == 0) leftNodeId = p0;
            else leftNodeId = parentEntries[childIndex - 1].PageId;

            rightNodeId = nodeId;
            separatorKey = parentEntries[childIndex].Key; // Key separating Left and Right
        }

        // Perform Merge: Move all items from Right Node to Left Node. With variable-length keys
        // the two nodes may not fit in one page (#217): the node then stays underfull, which
        // costs space but keeps the tree valid.
        if (!MergeNodes(leftNodeId, rightNodeId, separatorKey, transactionId))
            return;

        // Remove separator key and right pointer from Parent
        if (childIndex == -1)
        {
            parentEntries.RemoveAt(0); // Removing Entry 0 (Key 0, P1) - P1 was Right Node
            // P0 remains P0 (which was Left Node)
        }
        else
        {
            parentEntries.RemoveAt(childIndex); // Remove entry pointing to Right Node
        }

        // Write updated Parent
        WriteInternalNode(parentId, p0, parentEntries, transactionId);

        // Free the empty Right Node
        _storage.FreePage(rightNodeId); // Not transactional: the page is released even if this
                                         // transaction rolls back. Might need TransactionalFreePage.

        // Recursive Underflow Check on Parent
        int minInternal = MaxEntriesPerNode / 2;
        if (parentEntries.Count < minInternal && parentId != _rootPageId)
        {
            var parentPath = new List<uint>(path.Take(path.Count - 1)); // Path to grandparent
            HandleUnderflow(parentId, parentPath, transactionId);
        }
        else if (parentId == _rootPageId && parentEntries.Count == 0)
        {
            // Root collapse: P0 is the sole remaining child; its content moves into the root page.
            CollapseRootInto(p0, transactionId);
        }
    }

    #region Async API

    // -------------------------------------------------------------------------
    // Step 2.1 — FindLeafNodeWithPathAsync
    // -------------------------------------------------------------------------

    /// <summary>
    /// Traverses the B+Tree from root to the appropriate leaf for <paramref name="key"/>.
    /// Each level performs a true async page read via <see cref="StorageEngine.ReadPageAsync"/>.
    /// Comparisons and pointer lookups are done synchronously on the in-memory buffer.
    /// See <see cref="FindLeafNode"/> for <paramref name="leftmost"/>.
    /// </summary>
    private async ValueTask<uint> FindLeafNodeWithPathAsync(
        IndexKey key, List<uint>? path, ulong transactionId, bool leftmost, CancellationToken ct)
    {
        var currentPageId = _rootPageId;
        var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            while (true)
            {
                ct.ThrowIfCancellationRequested();
                await _storage.ReadPageAsync(currentPageId, transactionId, pageBuffer.AsMemory(0, _storage.PageSize), ct).ConfigureAwait(false);
                var header = BTreeNodeHeader.ReadFrom(pageBuffer.AsSpan(32));

                if (header.IsLeaf)
                    return currentPageId;

                path?.Add(currentPageId);
                currentPageId = FindChildNode(pageBuffer.AsSpan(0, _storage.PageSize), header, key, leftmost);
            }
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(pageBuffer);
        }
    }

    // -------------------------------------------------------------------------
    // Step 2.2 — TryFindAsync
    // -------------------------------------------------------------------------

    /// <summary>
    /// Async version of <see cref="TryFind"/>.
    /// Returns a tuple instead of an <c>out</c> parameter (incompatible with async).
    /// </summary>
    public async ValueTask<(bool Found, DocumentLocation Location)> TryFindAsync(
        IndexKey key, ulong? transactionId = null, CancellationToken ct = default)
    {
        var txnId = transactionId ?? 0;
        // Leftmost leaf that can hold the key, then on along the chain (see TryFind, #213).
        var leafPageId = await FindLeafNodeWithPathAsync(key, null, txnId, leftmost: true, ct).ConfigureAwait(false);

        var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            while (leafPageId != 0)
            {
                await _storage.ReadPageAsync(leafPageId, txnId, pageBuffer.AsMemory(0, _storage.PageSize), ct).ConfigureAwait(false);
                var header = BTreeNodeHeader.ReadFrom(pageBuffer.AsSpan(32));
                if (SearchLeafLowerBoundArray(pageBuffer, header.EntryCount, key, out var found, out var location))
                    return (found, location);
                leafPageId = header.NextLeafPageId;
            }
            return (false, default);
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(pageBuffer);
        }
    }

    // -------------------------------------------------------------------------
    // Step 2.3 — RangeAsync
    // -------------------------------------------------------------------------

    /// <summary>
    /// Async range scan — yields all entries with keys in [<paramref name="minKey"/>, <paramref name="maxKey"/>].
    /// Each page read is a true async I/O call; entry parsing is synchronous on the in-memory buffer.
    /// The ArrayPool buffer is rented and returned per page so that <c>yield return</c> never
    /// holds a buffer across an async boundary.
    /// </summary>
    public async IAsyncEnumerable<IndexEntry> RangeAsync(
        IndexKey minKey,
        IndexKey maxKey,
        IndexDirection direction = IndexDirection.Forward,
        ulong? transactionId = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var txnId = transactionId ?? 0;

        if (direction == IndexDirection.Forward)
        {
            var leafPageId = await FindLeafNodeWithPathAsync(minKey, null, txnId, leftmost: true, ct).ConfigureAwait(false);

            while (leafPageId != 0)
            {
                ct.ThrowIfCancellationRequested();

                // Rent, read, parse — return buffer BEFORE yielding
                List<IndexEntry> pageEntries;
                uint nextLeafId;
                bool exceeded;

                var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);
                try
                {
                    await _storage.ReadPageAsync(leafPageId, txnId, pageBuffer.AsMemory(0, _storage.PageSize), ct).ConfigureAwait(false);
                    var header = BTreeNodeHeader.ReadFrom(pageBuffer.AsSpan(32));
                    nextLeafId = header.NextLeafPageId;
                    pageEntries = new List<IndexEntry>(header.EntryCount);
                    exceeded = false;

                    var dataOffset = 32 + 20;
                    for (int i = 0; i < header.EntryCount; i++)
                    {
                        var entryKey = ReadIndexKey(pageBuffer, dataOffset);
                        if (entryKey >= minKey && entryKey <= maxKey)
                        {
                            var locOffset = dataOffset + 4 + entryKey.Data.Length;
                            var location = DocumentLocation.ReadFrom(pageBuffer.AsSpan(locOffset, DocumentLocation.SerializedSize));
                            pageEntries.Add(new IndexEntry(entryKey, location));
                        }
                        else if (entryKey > maxKey)
                        {
                            exceeded = true;
                            break;
                        }
                        dataOffset += 4 + entryKey.Data.Length + DocumentLocation.SerializedSize;
                    }
                }
                finally
                {
                    System.Buffers.ArrayPool<byte>.Shared.Return(pageBuffer);
                }

                // Buffer returned — safe to yield
                foreach (var entry in pageEntries)
                    yield return entry;

                if (exceeded) yield break;
                leafPageId = nextLeafId;
            }
        }
        else // Backward
        {
            var leafPageId = await FindLeafNodeWithPathAsync(maxKey, null, txnId, leftmost: false, ct).ConfigureAwait(false);

            while (leafPageId != 0)
            {
                ct.ThrowIfCancellationRequested();

                List<IndexEntry> pageEntries;
                uint prevLeafId;

                var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);
                try
                {
                    await _storage.ReadPageAsync(leafPageId, txnId, pageBuffer.AsMemory(0, _storage.PageSize), ct).ConfigureAwait(false);
                    var header = BTreeNodeHeader.ReadFrom(pageBuffer.AsSpan(32));
                    prevLeafId = header.PrevLeafPageId;
                    pageEntries = ReadLeafEntries(pageBuffer.AsSpan(0, _storage.PageSize), header.EntryCount);
                }
                finally
                {
                    System.Buffers.ArrayPool<byte>.Shared.Return(pageBuffer);
                }

                bool belowMin = false;
                for (int i = pageEntries.Count - 1; i >= 0; i--)
                {
                    var entry = pageEntries[i];
                    if (entry.Key <= maxKey && entry.Key >= minKey)
                        yield return entry;
                    else if (entry.Key < minKey)
                    {
                        belowMin = true;
                        break;
                    }
                }

                if (belowMin) yield break;
                leafPageId = prevLeafId;
            }
        }
    }

    // -------------------------------------------------------------------------
    // Step 2.4 — Query primitive async (delegate to RangeAsync)
    // -------------------------------------------------------------------------

    /// <summary>Async exact-match lookup. Delegates to <see cref="RangeAsync"/>.</summary>
    public IAsyncEnumerable<IndexEntry> EqualAsync(
        IndexKey key, ulong transactionId, CancellationToken ct = default)
        => RangeAsync(key, key, IndexDirection.Forward, transactionId, ct);

    /// <summary>Async greater-than (or equal) scan.</summary>
    public async IAsyncEnumerable<IndexEntry> GreaterThanAsync(
        IndexKey key, bool orEqual, ulong transactionId,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var effectiveMin = orEqual ? key : key; // start key; filter below handles strict
        await foreach (var entry in RangeAsync(effectiveMin, IndexKey.MaxKey, IndexDirection.Forward, transactionId, ct).ConfigureAwait(false))
        {
            if (!orEqual && entry.Key.Equals(key)) continue;
            yield return entry;
        }
    }

    /// <summary>Async less-than (or equal) scan (descending).</summary>
    public async IAsyncEnumerable<IndexEntry> LessThanAsync(
        IndexKey key, bool orEqual, ulong transactionId,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var entry in RangeAsync(IndexKey.MinKey, key, IndexDirection.Backward, transactionId, ct).ConfigureAwait(false))
        {
            if (!orEqual && entry.Key.Equals(key)) continue;
            yield return entry;
        }
    }

    /// <summary>Async range with configurable inclusivity at both ends.</summary>
    public async IAsyncEnumerable<IndexEntry> BetweenAsync(
        IndexKey start, IndexKey end, bool startInclusive, bool endInclusive,
        ulong transactionId, [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var entry in RangeAsync(start, end, IndexDirection.Forward, transactionId, ct).ConfigureAwait(false))
        {
            if (!startInclusive && entry.Key.Equals(start)) continue;
            if (!endInclusive && entry.Key.Equals(end)) continue;
            yield return entry;
        }
    }

    /// <summary>Async prefix scan for string keys.</summary>
    public async IAsyncEnumerable<IndexEntry> StartsWithAsync(
        string prefix, ulong transactionId,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var startKey = IndexKey.Create(prefix);
        await foreach (var entry in RangeAsync(startKey, IndexKey.MaxKey, IndexDirection.Forward, transactionId, ct).ConfigureAwait(false))
        {
            string val = string.Empty;
            try { val = entry.Key.As<string>(); } catch { yield break; }
            if (!val.StartsWith(prefix, StringComparison.Ordinal)) yield break;
            yield return entry;
        }
    }

    /// <summary>Async multi-key lookup (sorted to exploit leaf chaining).</summary>
    public async IAsyncEnumerable<IndexEntry> InAsync(
        IEnumerable<IndexKey> keys, ulong transactionId,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var key in keys.OrderBy(k => k))
        {
            var (found, location) = await TryFindAsync(key, transactionId, ct).ConfigureAwait(false);
            if (found)
                yield return new IndexEntry(key, location);
        }
    }

    /// <summary>Async SQL LIKE pattern scan for string keys.</summary>
    public async IAsyncEnumerable<IndexEntry> LikeAsync(
        string pattern, ulong transactionId,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var regexPattern = "^" + System.Text.RegularExpressions.Regex.Escape(pattern)
            .Replace("%", ".*").Replace("_", ".") + "$";
        var regex = new System.Text.RegularExpressions.Regex(
            regexPattern, System.Text.RegularExpressions.RegexOptions.Compiled);

        string prefix = "";
        foreach (char c in pattern)
        {
            if (c == '%' || c == '_') break;
            prefix += c;
        }

        var startKey = string.IsNullOrEmpty(prefix) ? IndexKey.MinKey : IndexKey.Create(prefix);

        await foreach (var entry in RangeAsync(startKey, IndexKey.MaxKey, IndexDirection.Forward, transactionId, ct).ConfigureAwait(false))
        {
            string val = string.Empty;
            try { val = entry.Key.As<string>(); } catch { yield break; }

            if (!string.IsNullOrEmpty(prefix) && !val.StartsWith(prefix, StringComparison.Ordinal))
                yield break;

            if (regex.IsMatch(val))
                yield return entry;
        }
    }

    #endregion

    /// <summary>
    /// Returns all physical page IDs owned by this B+Tree.
    /// Used when dropping an index to free storage space.
    /// </summary>
    public IReadOnlyList<uint> CollectAllPages() => CollectAllPages(0);

    /// <summary>
    /// Like <see cref="CollectAllPages()"/>, but walks the tree as <paramref name="transactionId"/>
    /// sees it, including nodes that transaction has written but not committed.
    /// </summary>
    internal IReadOnlyList<uint> CollectAllPages(ulong transactionId)
    {
        var result = new List<uint>();
        var root = RootPageId;
        if (root != 0)
            CollectPagesRecursive(root, result, transactionId);
        return result;
    }

    private void CollectPagesRecursive(uint pageId, List<uint> result, ulong transactionId)
    {
        if (pageId == 0) return;
        result.Add(pageId);
        var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            ReadPage(pageId, transactionId, pageBuffer);
            var header = BTreeNodeHeader.ReadFrom(pageBuffer.AsSpan(32));
            if (!header.IsLeaf)
            {
                var (p0, entries) = ReadInternalEntries(pageBuffer, header.EntryCount);
                CollectPagesRecursive(p0, result, transactionId);
                foreach (var entry in entries)
                    CollectPagesRecursive(entry.PageId, result, transactionId);
            }
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(pageBuffer);
        }
    }

    /// <summary>
    /// Moves every entry of the right node into the left one. Returns <c>false</c>, writing
    /// nothing, when the result would not fit in one node by count or by bytes (#217).
    /// </summary>
    private bool MergeNodes(uint leftNodeId, uint rightNodeId, IndexKey separatorKey, ulong transactionId)
    {
        var buffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            // Read both nodes
            // Note: Simplification - assuming both are Leaves or both Internal. 
            // In standard B-Tree they must be same height.

            ReadPage(leftNodeId, transactionId, buffer);
            var leftHeader = BTreeNodeHeader.ReadFrom(buffer.AsSpan(32));
            // Read entries... (need specific method based on type)

            if (leftHeader.IsLeaf)
            {
                // ── In-place leaf merge: copy right's data block into left ──
                var rightBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);
                try
                {
                    ReadPage(rightNodeId, transactionId, rightBuffer);
                    var rightHeader = BTreeNodeHeader.ReadFrom(rightBuffer.AsSpan(32));

                    // Compute byte regions
                    int leftDataEnd = LeafDataEnd(buffer, leftHeader.EntryCount);
                    int rightDataStart = 32 + 20;
                    int rightDataEnd = LeafDataEnd(rightBuffer, rightHeader.EntryCount);
                    int rightDataLen = rightDataEnd - rightDataStart;

                    if (leftHeader.EntryCount + rightHeader.EntryCount > MaxEntriesPerNode
                        || leftDataEnd + rightDataLen > _storage.PageSize)
                        return false;

                    // Bulk copy right's entries to end of left
                    if (rightDataLen > 0)
                    {
                        new Span<byte>(rightBuffer, rightDataStart, rightDataLen)
                            .CopyTo(new Span<byte>(buffer, leftDataEnd, rightDataLen));
                    }

                    // Update left header: count, next pointer
                    leftHeader.EntryCount += rightHeader.EntryCount;
                    leftHeader.NextLeafPageId = rightHeader.NextLeafPageId;
                    leftHeader.WriteTo(buffer.AsSpan(32, 20));

                    WritePage(leftNodeId, transactionId, new ReadOnlySpan<byte>(buffer, 0, _storage.PageSize));

                    // Fix Right.Next's Prev pointer
                    if (rightHeader.NextLeafPageId != 0)
                    {
                        UpdatePrevPointer(rightHeader.NextLeafPageId, leftNodeId, transactionId);
                    }
                }
                finally
                {
                    System.Buffers.ArrayPool<byte>.Shared.Return(rightBuffer);
                }
            }
            else
            {
                // Internal Node Merge
                ReadPage(leftNodeId, transactionId, buffer);
                // leftHeader is already read and valid
                var (leftP0, leftEntries) = ReadInternalEntries(buffer, leftHeader.EntryCount);

                ReadPage(rightNodeId, transactionId, buffer);
                var rightHeader = BTreeNodeHeader.ReadFrom(buffer.AsSpan(32));
                var (rightP0, rightEntries) = ReadInternalEntries(buffer, rightHeader.EntryCount);

                if (leftEntries.Count + 1 + rightEntries.Count > MaxEntriesPerNode
                    || InternalEntriesSize(leftEntries) + InternalEntrySize(separatorKey.Data.Length)
                       + InternalEntriesSize(rightEntries) > InternalEntrySpace)
                    return false;

                // Add Separator Key (from parent) pointing to Right's P0
                leftEntries.Add(new InternalEntry(separatorKey, rightP0));

                // Add all Right entries
                leftEntries.AddRange(rightEntries);

                // UpdateAsync Left Node
                WriteInternalNode(leftNodeId, leftP0, leftEntries, transactionId);
            }
            return true;
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}