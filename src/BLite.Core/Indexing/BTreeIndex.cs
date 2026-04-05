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
    private uint _rootPageId;
    private readonly Action<uint>? _onRootChanged;
    internal const int MaxEntriesPerNode = 100; // Low value to test splitting

    public BTreeIndex(StorageEngine storage,
                      IndexOptions options, 
                      uint rootPageId = 0,
                      Action<uint>? onRootChanged = null)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _options = options;
        _rootPageId = rootPageId;
        _onRootChanged = onRootChanged;

        if (_rootPageId == 0)
        {
            // Allocate new root page (cannot use page 0 which is file header)
            _rootPageId = _storage.AllocateIndexPage();

            // Initialize as empty leaf
            var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);
            try
            {
                // Clear buffer
                pageBuffer.AsSpan().Clear();

                // Write headers
                var pageHeader = new PageHeader
                {
                    PageId = _rootPageId,
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

                _storage.WritePageImmediate(_rootPageId, pageBuffer);
            }
            finally
            {
                System.Buffers.ArrayPool<byte>.Shared.Return(pageBuffer);
            }
        }
    }

    public uint RootPageId => _rootPageId;

    /// <summary>
    /// Invokes the optional callback to notify the owner that the root page has changed
    /// and must be re-persisted in collection metadata.
    /// </summary>
    private void NotifyRootChanged() => _onRootChanged?.Invoke(_rootPageId);

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

        if (_options.Unique && TryFind(key, out var existingLocation, txnId))
        {
            if (!existingLocation.Equals(location))
                throw new InvalidOperationException("Duplicate key violation for unique index");

            return;
        }

        var entry = new IndexEntry(key, location);
        var path = new List<uint>();

        // Find the leaf node for insertion
        var leafPageId = FindLeafNodeWithPath(key, path, txnId);

        var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            ReadPage(leafPageId, txnId, pageBuffer);
            var header = BTreeNodeHeader.ReadFrom(pageBuffer.AsSpan(32));

            // Check if we need to split
            if (header.EntryCount >= MaxEntriesPerNode)
            {
                SplitNode(leafPageId, path, txnId);

                // Re-find leaf after split to ensure we have correct node
                path.Clear();
                leafPageId = FindLeafNodeWithPath(key, path, txnId);
                ReadPage(leafPageId, txnId, pageBuffer);
            }

            // Insert entry into leaf
            InsertIntoLeaf(leafPageId, entry, pageBuffer, txnId);
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

        var leafPageId = FindLeafNode(key, txnId);

        Span<byte> pageBuffer = stackalloc byte[_storage.PageSize];
        ReadPage(leafPageId, txnId, pageBuffer);

        var header = BTreeNodeHeader.ReadFrom(pageBuffer[32..]);
        var dataOffset = 32 + 20; // Page header + BTree node header

        // Linear search in leaf (could be optimized with binary search)
        for (int i = 0; i < header.EntryCount; i++)
        {
            var entryKey = ReadIndexKey(pageBuffer, dataOffset);

            if (entryKey.Equals(key))
            {
                // Found - read DocumentLocation (6 bytes: 4 for PageId + 2 for SlotIndex)
                var locationOffset = dataOffset + entryKey.Data.Length + 4; // +4 for key length prefix
                location = DocumentLocation.ReadFrom(pageBuffer.Slice(locationOffset, DocumentLocation.SerializedSize));
                return true;
            }

            // Move to next entry: length(4) + key + location(6)
            dataOffset += 4 + entryKey.Data.Length + DocumentLocation.SerializedSize;
        }

        return false;
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
                var leafPageId = FindLeafNode(minKey, txnId);

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
                // Start from the end of the range (maxKey)
                var leafPageId = FindLeafNode(maxKey, txnId);

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
        var txnId = transactionId ?? 0;
        int count = 0;
        var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            var leafPageId = FindLeafNode(minKey, txnId);
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
                    if (IndexKey.CompareRaw(entryKeySpan, maxKey.Data) <= 0)
                        count++;
                    else
                    {
                        stopScan = true; // past maxKey — no more matches in any leaf
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
        return count;
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
            var leafPageId = FindLeafNode(minKey, txnId);

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

    internal uint FindLeafNode(IndexKey key, ulong transactionId)
    {
        var path = new List<uint>();
        return FindLeafNodeWithPath(key, path, transactionId);
    }

    private uint FindLeafNodeWithPath(IndexKey key, List<uint> path, ulong transactionId)
    {
        var currentPageId = _rootPageId;
        var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);

        try
        {
            while (true)
            {
                ReadPage(currentPageId, transactionId, pageBuffer);
                var header = BTreeNodeHeader.ReadFrom(pageBuffer.AsSpan(32));

                if (header.IsLeaf)
                {
                    return currentPageId;
                }

                path.Add(currentPageId);
                currentPageId = FindChildNode(pageBuffer, header, key);
            }
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(pageBuffer);
        }
    }

    private uint FindChildNode(Span<byte> nodeBuffer, BTreeNodeHeader header, IndexKey key)
    {
        // Internal Node Format:
        // [Header]
        // [P0 (4 bytes)] - Pointer to child with keys < Key1
        // [Entry 1: Key1, P1]
        // [Entry 2: Key2, P2]
        // ...

        var dataOffset = 32 + 20;
        var p0 = BitConverter.ToUInt32(nodeBuffer.Slice(dataOffset, 4));
        dataOffset += 4;

        uint childPageId = p0;

        // Linear search for now (optimize to binary search later)
        for (int i = 0; i < header.EntryCount; i++)
        {
            // Read key length and span — no IndexKey allocation needed for traversal.
            int keyLength = BitConverter.ToInt32(nodeBuffer.Slice(dataOffset, 4));
            var entryKeySpan = nodeBuffer.Slice(dataOffset + 4, keyLength);
            var pointerOffset = dataOffset + 4 + keyLength;
            var nextPointer = BitConverter.ToUInt32(nodeBuffer.Slice(pointerOffset, 4));

            // key < entryKey → descend into current (left) child
            if (IndexKey.CompareRaw(key.Data, entryKeySpan) < 0)
            {
                return childPageId;
            }

            childPageId = nextPointer;
            dataOffset = pointerOffset + 4; // advance past key + pointer
        }

        return childPageId; // Return last pointer (>= last key)
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

    private void InsertIntoLeaf(uint leafPageId, IndexEntry entry, Span<byte> pageBuffer, ulong transactionId)
    {
        var header = BTreeNodeHeader.ReadFrom(pageBuffer[32..]);
        var dataOffset = 32 + 20;

        // Find sorted insertion point: first existing entry >= new key
        int insertOffset = -1;
        for (int i = 0; i < header.EntryCount; i++)
        {
            var keyLen = BitConverter.ToInt32(pageBuffer.Slice(dataOffset, 4));
            var existingKey = IndexKey.FromOwnedArray(pageBuffer.Slice(dataOffset + 4, keyLen).ToArray());
            if (insertOffset == -1 && entry.Key.CompareTo(existingKey) <= 0)
                insertOffset = dataOffset;
            dataOffset += 4 + keyLen + DocumentLocation.SerializedSize;
        }

        // Append at end if no earlier insertion point found
        if (insertOffset == -1)
            insertOffset = dataOffset;

        int entrySize = 4 + entry.Key.Data.Length + DocumentLocation.SerializedSize;
        int currentEnd = dataOffset;

        // Shift existing entries right to make room for the new entry
        if (insertOffset < currentEnd)
        {
            int bytesToShift = currentEnd - insertOffset;
            var temp = new byte[bytesToShift];
            pageBuffer.Slice(insertOffset, bytesToShift).CopyTo(temp);
            temp.AsSpan().CopyTo(pageBuffer.Slice(insertOffset + entrySize, bytesToShift));
        }

        // Write new entry at sorted insertion point
        int writePos = insertOffset;
        BitConverter.TryWriteBytes(pageBuffer.Slice(writePos, 4), entry.Key.Data.Length);
        writePos += 4;
        entry.Key.Data.CopyTo(pageBuffer.Slice(writePos, entry.Key.Data.Length));
        writePos += entry.Key.Data.Length;
        entry.Location.WriteTo(pageBuffer.Slice(writePos, DocumentLocation.SerializedSize));

        header.EntryCount++;
        header.WriteTo(pageBuffer.Slice(32, 20));
        WritePage(leafPageId, transactionId, pageBuffer);
    }

    private void SplitNode(uint nodePageId, List<uint> path, ulong transactionId)
    {
        var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            ReadPage(nodePageId, transactionId, pageBuffer);
            var header = BTreeNodeHeader.ReadFrom(pageBuffer.AsSpan(32));

            if (header.IsLeaf)
            {
                SplitLeafNode(nodePageId, header, pageBuffer, path, transactionId);
            }
            else
            {
                SplitInternalNode(nodePageId, header, pageBuffer, path, transactionId);
            }
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(pageBuffer);
        }
    }

    private void SplitLeafNode(uint nodePageId, BTreeNodeHeader header, Span<byte> pageBuffer, List<uint> path, ulong transactionId)
    {
        var entries = ReadLeafEntries(pageBuffer, header.EntryCount);

        var splitPoint = entries.Count / 2;
        var leftEntries = entries.Take(splitPoint).ToList();
        var rightEntries = entries.Skip(splitPoint).ToList();

        // Create new node for right half
        var newNodeId = CreateNode(isLeaf: true, transactionId);

        // UpdateAsync original node (left)
        // Next -> RightNode
        // Prev -> Original Prev (remains same)
        WriteLeafNode(nodePageId, leftEntries, newNodeId, header.PrevLeafPageId, transactionId);

        // UpdateAsync new node (right) 
        // Next -> Original Next
        // Prev -> LeftNode
        WriteLeafNode(newNodeId, rightEntries, header.NextLeafPageId, nodePageId, transactionId);

        // UpdateAsync Original Next Node's Prev pointer to point to New Node
        if (header.NextLeafPageId != 0)
        {
            UpdatePrevPointer(header.NextLeafPageId, newNodeId, transactionId);
        }

        // Promote key to parent (first key of right node)
        var promoteKey = rightEntries[0].Key;
        InsertIntoParent(nodePageId, promoteKey, newNodeId, path, transactionId);
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

    private void SplitInternalNode(uint nodePageId, BTreeNodeHeader header, Span<byte> pageBuffer, List<uint> path, ulong transactionId)
    {
        var (p0, entries) = ReadInternalEntries(pageBuffer, header.EntryCount);
        var splitPoint = entries.Count / 2;

        // For internal nodes, the median key moves UP to parent and is excluded from children
        var promoteKey = entries[splitPoint].Key;

        var leftEntries = entries.Take(splitPoint).ToList();
        var rightEntries = entries.Skip(splitPoint + 1).ToList();
        var rightP0 = entries[splitPoint].PageId; // Attempting to use the pointer associated with promoted key as P0 for right node

        // Create new internal node
        var newNodeId = CreateNode(isLeaf: false, transactionId);

        // UpdateAsync left node
        WriteInternalNode(nodePageId, p0, leftEntries, transactionId);

        // UpdateAsync right node
        WriteInternalNode(newNodeId, rightP0, rightEntries, transactionId);

        // Insert promoted key into parent
        InsertIntoParent(nodePageId, promoteKey, newNodeId, path, transactionId);
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

            if (header.EntryCount >= MaxEntriesPerNode)
            {
                // Parent full, split parent
                // Ideally should Insert then Split, or Split then Insert.
                // Simplified: Split parent first, then insert into appropriate half.
                // But wait, to Split we need the median.
                // Better approach: Read all, add new entry, then split the collection and write back.

                var (p0, entries) = ReadInternalEntries(pageBuffer.AsSpan(0, _storage.PageSize), header.EntryCount);

                // Insert new key/pointer in sorted order
                var newEntry = new InternalEntry(key, rightChildPageId);
                int insertIndex = entries.FindIndex(e => e.Key > key);
                if (insertIndex == -1) entries.Add(newEntry);
                else entries.Insert(insertIndex, newEntry);

                // Now split these extended entries
                var splitPoint = entries.Count / 2;
                var promoteKey = entries[splitPoint].Key;
                var rightP0 = entries[splitPoint].PageId;

                var leftEntries = entries.Take(splitPoint).ToList();
                var rightEntries = entries.Skip(splitPoint + 1).ToList();

                var newParentId = CreateNode(isLeaf: false, transactionId);

                WriteInternalNode(parentPageId, p0, leftEntries, transactionId);
                WriteInternalNode(newParentId, rightP0, rightEntries, transactionId);

                InsertIntoParent(parentPageId, promoteKey, newParentId, path, transactionId);
            }
            else
            {
                // Insert directly
                InsertIntoInternal(parentPageId, header, pageBuffer.AsSpan(0, _storage.PageSize), key, rightChildPageId, transactionId);
            }
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(pageBuffer);
        }
    }

    private void CreateNewRoot(uint leftChildId, IndexKey key, uint rightChildId, ulong transactionId)
    {
        var newRootId = CreateNode(isLeaf: false, transactionId);
        var entries = new List<InternalEntry> { new InternalEntry(key, rightChildId) };
        WriteInternalNode(newRootId, leftChildId, entries, transactionId);
        _rootPageId = newRootId;
        NotifyRootChanged(); // Caller must re-persist the new root page ID in collection metadata
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

    private void InsertIntoInternal(uint pageId, BTreeNodeHeader header, Span<byte> pageBuffer, IndexKey key, uint rightChildId, ulong transactionId)
    {
        // Read, insert, write back. In production do in-place shift.
        var (p0, entries) = ReadInternalEntries(pageBuffer, header.EntryCount);

        var newEntry = new InternalEntry(key, rightChildId);
        int insertIndex = entries.FindIndex(e => e.Key > key);
        if (insertIndex == -1) entries.Add(newEntry);
        else entries.Insert(insertIndex, newEntry);

        WriteInternalNode(pageId, p0, entries, transactionId);
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
        var leafPageId = FindLeafNodeWithPath(key, path, txnId);

        var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            ReadPage(leafPageId, txnId, pageBuffer);
            var header = BTreeNodeHeader.ReadFrom(pageBuffer.AsSpan(32));

            // Check if key exists in leaf
            var entries = ReadLeafEntries(pageBuffer, header.EntryCount);
            var entryIndex = entries.FindIndex(e => e.Key.Equals(key) && 
                e.Location.PageId == location.PageId && 
                e.Location.SlotIndex == location.SlotIndex);

            if (entryIndex == -1)
            {
                return false; // Not found
            }

            // Remove entry
            entries.RemoveAt(entryIndex);

            // UpdateAsync leaf
            WriteLeafNode(leafPageId, entries, header.NextLeafPageId, header.PrevLeafPageId, txnId);

            // Check for underflow (min 50% fill)
            // Simplified: min 1 entry for now, or MaxEntries/2
            int minEntries = MaxEntriesPerNode / 2;
            if (entries.Count < minEntries && _rootPageId != leafPageId)
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

    private bool BorrowFromSibling(uint nodeId, uint parentId, int childIndex, List<InternalEntry> parentEntries, uint p0, ulong transactionId)
    {
        int minEntries = MaxEntriesPerNode / 2;
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

                if (rightHeader.EntryCount > minEntries)
                {
                    if (nodeHeader.IsLeaf)
                    {
                        var nodeEntries  = ReadLeafEntries(nodeBuffer,  nodeHeader.EntryCount);
                        var rightEntries = ReadLeafEntries(siblingBuffer, rightHeader.EntryCount);

                        // Rotate left: move right's first entry to end of current
                        nodeEntries.Add(rightEntries[0]);
                        rightEntries.RemoveAt(0);

                        // Parent separator = new first key of right sibling
                        parentEntries[rightSepIdx] = new InternalEntry(rightEntries[0].Key, rightSiblingId);

                        WriteLeafNode(nodeId,          nodeEntries,  nodeHeader.NextLeafPageId,  nodeHeader.PrevLeafPageId,  transactionId);
                        WriteLeafNode(rightSiblingId,  rightEntries, rightHeader.NextLeafPageId, rightHeader.PrevLeafPageId, transactionId);
                    }
                    else
                    {
                        var (nodeP0,  nodeEntries)  = ReadInternalEntries(nodeBuffer,  nodeHeader.EntryCount);
                        var (rightP0, rightEntries) = ReadInternalEntries(siblingBuffer, rightHeader.EntryCount);

                        // Rotate left for internal node:
                        // Pull separator key down from parent; right's P0 becomes new last child of current.
                        var sep = parentEntries[rightSepIdx].Key;
                        nodeEntries.Add(new InternalEntry(sep, rightP0));

                        // Right sibling's new P0 = its former first entry's PageId
                        var newRightP0 = rightEntries[0].PageId;
                        parentEntries[rightSepIdx] = new InternalEntry(rightEntries[0].Key, rightSiblingId);
                        rightEntries.RemoveAt(0);

                        WriteInternalNode(nodeId,         nodeP0,    nodeEntries,  transactionId);
                        WriteInternalNode(rightSiblingId, newRightP0, rightEntries, transactionId);
                    }

                    WriteInternalNode(parentId, p0, parentEntries, transactionId);
                    return true;
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

                if (leftHeader.EntryCount > minEntries)
                {
                    if (nodeHeader.IsLeaf)
                    {
                        var nodeEntries = ReadLeafEntries(nodeBuffer,   nodeHeader.EntryCount);
                        var leftEntries = ReadLeafEntries(siblingBuffer, leftHeader.EntryCount);

                        // Rotate right: move left's last entry to front of current
                        var borrowed = leftEntries[leftEntries.Count - 1];
                        leftEntries.RemoveAt(leftEntries.Count - 1);
                        nodeEntries.Insert(0, borrowed);

                        // Parent separator = the borrowed entry (now first key of current)
                        parentEntries[leftSepIdx] = new InternalEntry(borrowed.Key, nodeId);

                        WriteLeafNode(leftSiblingId, leftEntries, leftHeader.NextLeafPageId, leftHeader.PrevLeafPageId, transactionId);
                        WriteLeafNode(nodeId,        nodeEntries, nodeHeader.NextLeafPageId, nodeHeader.PrevLeafPageId, transactionId);
                    }
                    else
                    {
                        var (nodeP0, nodeEntries) = ReadInternalEntries(nodeBuffer,   nodeHeader.EntryCount);
                        var (leftP0, leftEntries) = ReadInternalEntries(siblingBuffer, leftHeader.EntryCount);

                        // Rotate right for internal node:
                        // Pull separator key down from parent; old nodeP0 becomes regular first entry of current.
                        var sep = parentEntries[leftSepIdx].Key;
                        nodeEntries.Insert(0, new InternalEntry(sep, nodeP0));

                        // Left's last entry key becomes new parent separator; its PageId is new nodeP0
                        var lastLeft = leftEntries[leftEntries.Count - 1];
                        parentEntries[leftSepIdx] = new InternalEntry(lastLeft.Key, nodeId);
                        var newNodeP0 = lastLeft.PageId;
                        leftEntries.RemoveAt(leftEntries.Count - 1);

                        WriteInternalNode(leftSiblingId, leftP0,    leftEntries, transactionId);
                        WriteInternalNode(nodeId,        newNodeP0, nodeEntries, transactionId);
                    }

                    WriteInternalNode(parentId, p0, parentEntries, transactionId);
                    return true;
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

    private void MergeWithSibling(uint nodeId, uint parentId, int childIndex, List<InternalEntry> parentEntries, uint p0, List<uint> path, ulong transactionId)
    {
        // Identify sibling to merge with.
        // If P0 (childIndex -1), merge with right sibling (Entry 0).
        // If last child, merge with left sibling.
        // Otherwise, pick left or right.

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

        // Perform Merge: Move all items from Right Node to Left Node
        MergeNodes(leftNodeId, rightNodeId, separatorKey, transactionId);

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
        _storage.FreePage(rightNodeId); // Need to verify this works safely with Txn logic?
                                         // Actually, FreePage is immediate in current impl. Might need TransactionalFreePage.
                                         // Or just leave it allocated but unused for now.

        // Recursive Underflow Check on Parent
        int minInternal = MaxEntriesPerNode / 2;
        if (parentEntries.Count < minInternal && parentId != _rootPageId)
        {
            var parentPath = new List<uint>(path.Take(path.Count - 1)); // Path to grandparent
            HandleUnderflow(parentId, parentPath, transactionId);
        }
        else if (parentId == _rootPageId && parentEntries.Count == 0)
        {
            // Root collapse: P0 is the sole remaining child and becomes the new root.
            _rootPageId = p0;
            NotifyRootChanged(); // Caller must re-persist the new root page ID in collection metadata
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
    /// </summary>
    private async ValueTask<uint> FindLeafNodeWithPathAsync(
        IndexKey key, List<uint>? path, ulong transactionId, CancellationToken ct)
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
                currentPageId = FindChildNode(pageBuffer.AsSpan(0, _storage.PageSize), header, key);
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
        var leafPageId = await FindLeafNodeWithPathAsync(key, null, txnId, ct).ConfigureAwait(false);

        var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            await _storage.ReadPageAsync(leafPageId, txnId, pageBuffer.AsMemory(0, _storage.PageSize), ct).ConfigureAwait(false);
            var header = BTreeNodeHeader.ReadFrom(pageBuffer.AsSpan(32));
            var dataOffset = 32 + 20;

            for (int i = 0; i < header.EntryCount; i++)
            {
                var entryKey = ReadIndexKey(pageBuffer, dataOffset);
                if (entryKey.Equals(key))
                {
                    var locationOffset = dataOffset + 4 + entryKey.Data.Length;
                    var location = DocumentLocation.ReadFrom(pageBuffer.AsSpan(locationOffset, DocumentLocation.SerializedSize));
                    return (true, location);
                }
                dataOffset += 4 + entryKey.Data.Length + DocumentLocation.SerializedSize;
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
            var leafPageId = await FindLeafNodeWithPathAsync(minKey, null, txnId, ct).ConfigureAwait(false);

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
            var leafPageId = await FindLeafNodeWithPathAsync(maxKey, null, txnId, ct).ConfigureAwait(false);

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
    public IReadOnlyList<uint> CollectAllPages()
    {
        var result = new List<uint>();
        if (_rootPageId != 0)
            CollectPagesRecursive(_rootPageId, result);
        return result;
    }

    private void CollectPagesRecursive(uint pageId, List<uint> result)
    {
        if (pageId == 0) return;
        result.Add(pageId);
        var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);
        try
        {
            ReadPage(pageId, 0, pageBuffer);
            var header = BTreeNodeHeader.ReadFrom(pageBuffer.AsSpan(32));
            if (!header.IsLeaf)
            {
                var (p0, entries) = ReadInternalEntries(pageBuffer, header.EntryCount);
                CollectPagesRecursive(p0, result);
                foreach (var entry in entries)
                    CollectPagesRecursive(entry.PageId, result);
            }
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(pageBuffer);
        }
    }

    private void MergeNodes(uint leftNodeId, uint rightNodeId, IndexKey separatorKey, ulong transactionId)
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
                var leftEntries = ReadLeafEntries(buffer, leftHeader.EntryCount);

                ReadPage(rightNodeId, transactionId, buffer);
                var rightEntries = ReadLeafEntries(buffer.AsSpan(0, _storage.PageSize), ((BTreeNodeHeader.ReadFrom(buffer.AsSpan(32))).EntryCount)); // Dirty read reuse buffer? No, bad hygiene.
                // Re-read right clean
                var rightHeader = BTreeNodeHeader.ReadFrom(buffer.AsSpan(32));
                rightEntries = ReadLeafEntries(buffer, rightHeader.EntryCount);

                // Merge: Append Right to Left
                leftEntries.AddRange(rightEntries);

                // UpdateAsync Left
                // Next -> Right.Next
                // Prev -> Left.Prev (unchanged)
                WriteLeafNode(leftNodeId, leftEntries, rightHeader.NextLeafPageId, leftHeader.PrevLeafPageId, transactionId);

                // UpdateAsync Right.Next's Prev pointer to point to Left (since Right is gone)
                if (rightHeader.NextLeafPageId != 0)
                {
                    UpdatePrevPointer(rightHeader.NextLeafPageId, leftNodeId, transactionId);
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

                // Add Separator Key (from parent) pointing to Right's P0
                leftEntries.Add(new InternalEntry(separatorKey, rightP0));

                // Add all Right entries
                leftEntries.AddRange(rightEntries);

                // UpdateAsync Left Node
                WriteInternalNode(leftNodeId, leftP0, leftEntries, transactionId);
            }
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}