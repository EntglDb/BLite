using DocumentDb.Bson;
using DocumentDb.Core.Storage;

namespace DocumentDb.Core.Indexing;

/// <summary>
/// B+Tree index implementation for ordered index operations.
/// </summary>
public sealed class BTreeIndex
{
    private readonly PageFile _pageFile;
    private readonly IndexOptions _options;
    private uint _rootPageId;
    internal const int MaxEntriesPerNode = 4; // Low value to test splitting

    public BTreeIndex(PageFile pageFile, IndexOptions options, uint rootPageId = 0)
    {
        _pageFile = pageFile ?? throw new ArgumentNullException(nameof(pageFile));
        _options = options;
        _rootPageId = rootPageId;

        if (_rootPageId == 0)
        {
            // Allocate new root page (cannot use page 0 which is file header)
            _rootPageId = _pageFile.AllocatePage();
            
            // Initialize as empty leaf
            var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_pageFile.PageSize);
            try
            {
                // Clear buffer
                pageBuffer.AsSpan().Clear();
                
                // Write headers
                var pageHeader = new PageHeader
                {
                    PageId = _rootPageId,
                    PageType = PageType.Index,
                    FreeBytes = (ushort)(_pageFile.PageSize - 32),
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
                
                _pageFile.WritePage(_rootPageId, pageBuffer);
            }
            finally
            {
                System.Buffers.ArrayPool<byte>.Shared.Return(pageBuffer);
            }
        }
    }

    public uint RootPageId => _rootPageId;

    /// <summary>
    /// Inserts a key-value pair into the index
    /// </summary>
    public void Insert(IndexKey key, ObjectId documentId)
    {
        var entry = new IndexEntry(key, documentId);
        var path = new List<uint>();
        
        // Find the leaf node for insertion
        var leafPageId = FindLeafNodeWithPath(key, path);
        
        var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_pageFile.PageSize);
        try
        {
            _pageFile.ReadPage(leafPageId, pageBuffer);
            var header = BTreeNodeHeader.ReadFrom(pageBuffer.AsSpan(32));
            
            // Check if we need to split
            if (header.EntryCount >= MaxEntriesPerNode)
            {
                SplitNode(leafPageId, path);
                
                // Re-find leaf after split to ensure we have correct node
                path.Clear();
                leafPageId = FindLeafNodeWithPath(key, path);
                _pageFile.ReadPage(leafPageId, pageBuffer);
            }

            // Insert entry into leaf
            InsertIntoLeaf(leafPageId, entry, pageBuffer);
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(pageBuffer);
        }
    }

    /// <summary>
    /// Finds a document ID by exact key match
    /// </summary>
    public bool TryFind(IndexKey key, out ObjectId documentId)
    {
        documentId = default;
        
        var leafPageId = FindLeafNode(key);
        
        Span<byte> pageBuffer = stackalloc byte[_pageFile.PageSize];
        _pageFile.ReadPage(leafPageId, pageBuffer);
        
        var header = BTreeNodeHeader.ReadFrom(pageBuffer[32..]);
        var dataOffset = 32 + 16; // Page header + BTree node header
        
        // Linear search in leaf (could be optimized with binary search)
        for (int i = 0; i < header.EntryCount; i++)
        {
            var entryKey = ReadIndexKey(pageBuffer, dataOffset);
            
            if (entryKey.Equals(key))
            {
                // Found - read ObjectId
                var oidOffset = dataOffset + entryKey.Data.Length + 4; // +4 for key length prefix
                documentId = new ObjectId(pageBuffer.Slice(oidOffset, 12));
                return true;
            }
            
            // Move to next entry
            dataOffset += 4 + entryKey.Data.Length + 12; // length + key + oid
        }
        
        return false;
    }

    /// <summary>
    /// Range scan: finds all entries between minKey and maxKey (inclusive)
    /// </summary>
    public IEnumerable<IndexEntry> Range(IndexKey minKey, IndexKey maxKey)
    {
        var leafPageId = FindLeafNode(minKey);
        var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_pageFile.PageSize);

        try
        {
            while (leafPageId != 0)
            {
                _pageFile.ReadPage(leafPageId, pageBuffer);

                var header = BTreeNodeHeader.ReadFrom(pageBuffer.AsSpan(32));
                var dataOffset = 32 + 16;

                for (int i = 0; i < header.EntryCount; i++)
                {
                    var entryKey = ReadIndexKey(pageBuffer, dataOffset);

                    if (entryKey >= minKey && entryKey <= maxKey)
                    {
                        var oidOffset = dataOffset + entryKey.Data.Length + 4;
                        var oid = new ObjectId(pageBuffer.AsSpan(oidOffset, 12));
                        yield return new IndexEntry(entryKey, oid);
                    }
                    else if (entryKey > maxKey)
                    {
                        yield break; // Exceeded range
                    }

                    dataOffset += 4 + entryKey.Data.Length + 12;
                }

                // Move to next leaf
                leafPageId = header.NextLeafPageId;
            }
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(pageBuffer);
        }
    }

    private uint FindLeafNode(IndexKey key)
    {
        var path = new List<uint>();
        return FindLeafNodeWithPath(key, path);
    }

    private uint FindLeafNodeWithPath(IndexKey key, List<uint> path)
    {
        var currentPageId = _rootPageId;
        var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_pageFile.PageSize);

        try
        {
            while (true)
            {
                _pageFile.ReadPage(currentPageId, pageBuffer);
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

        var dataOffset = 32 + 16;
        var p0 = BitConverter.ToUInt32(nodeBuffer.Slice(dataOffset, 4));
        dataOffset += 4;

        uint childPageId = p0;

        // Linear search for now (optimize to binary search later)
        for (int i = 0; i < header.EntryCount; i++)
        {
            var entryKey = ReadIndexKey(nodeBuffer, dataOffset);
            var keyLen = 4 + entryKey.Data.Length;
            var pointerOffset = dataOffset + keyLen;
            var nextPointer = BitConverter.ToUInt32(nodeBuffer.Slice(pointerOffset, 4));

            if (key < entryKey)
            {
                return childPageId;
            }

            childPageId = nextPointer;
            dataOffset += keyLen + 4; // Key + Pointer
        }

        return childPageId; // Return last pointer (>= last key)
    }

    private void InsertIntoLeaf(uint leafPageId, IndexEntry entry, Span<byte> pageBuffer)
    {
        // Read current entries to determine offset
        var header = BTreeNodeHeader.ReadFrom(pageBuffer[32..]);
        var dataOffset = 32 + 16;

        // Skip existing entries to find free space
        for (int i = 0; i < header.EntryCount; i++)
        {
            var keyLen = BitConverter.ToInt32(pageBuffer.Slice(dataOffset, 4));
            dataOffset += 4 + keyLen + 12; // Length + Key + ObjectId
        }

        // Write key length
        BitConverter.TryWriteBytes(pageBuffer.Slice(dataOffset, 4), entry.Key.Data.Length);
        dataOffset += 4;

        // Write key data
        entry.Key.Data.CopyTo(pageBuffer.Slice(dataOffset, entry.Key.Data.Length));
        dataOffset += entry.Key.Data.Length;

        // Write ObjectId
        entry.DocumentId.WriteTo(pageBuffer.Slice(dataOffset, 12));

        // Update header
        header.EntryCount++;
        header.WriteTo(pageBuffer.Slice(32, 16));
        
        // Write page back
        _pageFile.WritePage(leafPageId, pageBuffer);
    }

    private void SplitNode(uint nodePageId, List<uint> path)
    {
        var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_pageFile.PageSize);
        try
        {
            _pageFile.ReadPage(nodePageId, pageBuffer);
            var header = BTreeNodeHeader.ReadFrom(pageBuffer.AsSpan(32));

            if (header.IsLeaf)
            {
                SplitLeafNode(nodePageId, header, pageBuffer, path);
            }
            else
            {
                SplitInternalNode(nodePageId, header, pageBuffer, path);
            }
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(pageBuffer);
        }
    }

    private void SplitLeafNode(uint nodePageId, BTreeNodeHeader header, Span<byte> pageBuffer, List<uint> path)
    {
        var entries = ReadLeafEntries(pageBuffer, header.EntryCount);
        var splitPoint = entries.Count / 2;
        var leftEntries = entries.Take(splitPoint).ToList();
        var rightEntries = entries.Skip(splitPoint).ToList();

        // Create new node for right half
        var newNodeId = CreateNode(isLeaf: true);

        // Update original node (left)
        WriteLeafNode(nodePageId, leftEntries, newNodeId); // Point to new node

        // Update new node (right) - preserve original next pointer
        WriteLeafNode(newNodeId, rightEntries, header.NextLeafPageId);

        // Promote key to parent (first key of right node)
        var promoteKey = rightEntries[0].Key;
        InsertIntoParent(nodePageId, promoteKey, newNodeId, path);
    }

    private void SplitInternalNode(uint nodePageId, BTreeNodeHeader header, Span<byte> pageBuffer, List<uint> path)
    {
        var (p0, entries) = ReadInternalEntries(pageBuffer, header.EntryCount);
        var splitPoint = entries.Count / 2;

        // For internal nodes, the median key moves UP to parent and is excluded from children
        var promoteKey = entries[splitPoint].Key;

        var leftEntries = entries.Take(splitPoint).ToList();
        var rightEntries = entries.Skip(splitPoint + 1).ToList();
        var rightP0 = entries[splitPoint].PageId; // Attempting to use the pointer associated with promoted key as P0 for right node

        // Create new internal node
        var newNodeId = CreateNode(isLeaf: false);

        // Update left node
        WriteInternalNode(nodePageId, p0, leftEntries);

        // Update right node
        WriteInternalNode(newNodeId, rightP0, rightEntries);

        // Insert promoted key into parent
        InsertIntoParent(nodePageId, promoteKey, newNodeId, path);
    }

    private void InsertIntoParent(uint leftChildPageId, IndexKey key, uint rightChildPageId, List<uint> path)
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
                CreateNewRoot(leftChildPageId, key, rightChildPageId);
                return;
            }
        }

        var parentPageId = path.Last();
        path.RemoveAt(path.Count - 1); // Pop parent for recursive calls

        var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_pageFile.PageSize);
        try
        {
            _pageFile.ReadPage(parentPageId, pageBuffer);
            var header = BTreeNodeHeader.ReadFrom(pageBuffer.AsSpan(32));

            if (header.EntryCount >= MaxEntriesPerNode)
            {
                // Parent full, split parent
                // Ideally should Insert then Split, or Split then Insert.
                // Simplified: Split parent first, then insert into appropriate half.
                // But wait, to Split we need the median.
                // Better approach: Read all, add new entry, then split the collection and write back.

                var (p0, entries) = ReadInternalEntries(pageBuffer.AsSpan(0, _pageFile.PageSize), header.EntryCount);

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

                var newParentId = CreateNode(isLeaf: false);

                WriteInternalNode(parentPageId, p0, leftEntries);
                WriteInternalNode(newParentId, rightP0, rightEntries);

                InsertIntoParent(parentPageId, promoteKey, newParentId, path);
            }
            else
            {
                // Insert directly
                InsertIntoInternal(parentPageId, header, pageBuffer.AsSpan(0, _pageFile.PageSize), key, rightChildPageId);
            }
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(pageBuffer);
        }
    }

    private void CreateNewRoot(uint leftChildId, IndexKey key, uint rightChildId)
    {
        var newRootId = CreateNode(isLeaf: false);
        var entries = new List<InternalEntry> { new InternalEntry(key, rightChildId) };
        WriteInternalNode(newRootId, leftChildId, entries);
        _rootPageId = newRootId; // Update in-memory root

        // TODO: Update root in file header/metadata block so it persists? 
        // For now user passes rootPageId to ctor. BTreeIndex doesn't manage master root pointer persistence yet.
    }

    private uint CreateNode(bool isLeaf)
    {
        var pageId = _pageFile.AllocatePage();
        var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_pageFile.PageSize);
        try
        {
            Array.Clear(pageBuffer, 0, _pageFile.PageSize);

            // Write page header
            var pageHeader = new PageHeader
            {
                PageId = pageId,
                PageType = PageType.Index,
                FreeBytes = (ushort)(_pageFile.PageSize - 32 - 16),
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
            nodeHeader.WriteTo(pageBuffer.AsSpan(32, 16));

            _pageFile.WritePage(pageId, pageBuffer.AsSpan(0, _pageFile.PageSize));
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
        var dataOffset = 32 + 16;

        for (int i = 0; i < count; i++)
        {
            var key = ReadIndexKey(pageBuffer, dataOffset);
            var oidOffset = dataOffset + 4 + key.Data.Length;
            var oid = new ObjectId(pageBuffer.Slice(oidOffset, 12));
            entries.Add(new IndexEntry(key, oid));
            dataOffset = oidOffset + 12;
        }
        return entries;
    }

    private (uint P0, List<InternalEntry> Entries) ReadInternalEntries(Span<byte> pageBuffer, int count)
    {
        var entries = new List<InternalEntry>(count);
        var dataOffset = 32 + 16;

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

    private void WriteLeafNode(uint pageId, List<IndexEntry> entries, uint nextLeafId)
    {
        var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_pageFile.PageSize);
        try
        {
            Array.Clear(pageBuffer, 0, _pageFile.PageSize);

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
                ParentPageId = 0, // TODO: Maintain parent pointers if needed for upstream traversal
                NextLeafPageId = nextLeafId
            };
            nodeHeader.WriteTo(pageBuffer.AsSpan(32, 16));

            // Write entries
            var dataOffset = 32 + 16;
            foreach (var entry in entries)
            {
                BitConverter.TryWriteBytes(pageBuffer.AsSpan(dataOffset, 4), entry.Key.Data.Length);
                entry.Key.Data.CopyTo(pageBuffer.AsSpan(dataOffset + 4));
                entry.DocumentId.WriteTo(pageBuffer.AsSpan(dataOffset + 4 + entry.Key.Data.Length));
                dataOffset += 4 + entry.Key.Data.Length + 12;
            }

            _pageFile.WritePage(pageId, pageBuffer.AsSpan(0, _pageFile.PageSize));
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(pageBuffer);
        }
    }

    private void WriteInternalNode(uint pageId, uint p0, List<InternalEntry> entries)
    {
        var pageBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(_pageFile.PageSize);
        try
        {
            Array.Clear(pageBuffer, 0, _pageFile.PageSize);

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
            nodeHeader.WriteTo(pageBuffer.AsSpan(32, 16));

            // Write P0
            var dataOffset = 32 + 16;
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

            _pageFile.WritePage(pageId, pageBuffer.AsSpan(0, _pageFile.PageSize));
        }
        finally
        {
            System.Buffers.ArrayPool<byte>.Shared.Return(pageBuffer);
        }
    }

    private void InsertIntoInternal(uint pageId, BTreeNodeHeader header, Span<byte> pageBuffer, IndexKey key, uint rightChildId)
    {
        // Read, insert, write back. In production do in-place shift.
        var (p0, entries) = ReadInternalEntries(pageBuffer, header.EntryCount);

        var newEntry = new InternalEntry(key, rightChildId);
        int insertIndex = entries.FindIndex(e => e.Key > key);
        if (insertIndex == -1) entries.Add(newEntry);
        else entries.Insert(insertIndex, newEntry);

        WriteInternalNode(pageId, p0, entries);
    }


    private IndexKey ReadIndexKey(Span<byte> buffer, int offset)
    {
        var keyLength = BitConverter.ToInt32(buffer.Slice(offset, 4));
        var keyData = buffer.Slice(offset + 4, keyLength);
        return new IndexKey(keyData);
    }

    private IndexKey ReadIndexKey(byte[] buffer, int offset)
    {
        var keyLength = BitConverter.ToInt32(buffer.AsSpan(offset, 4));
        var keyData = buffer.AsSpan(offset + 4, keyLength);
        return new IndexKey(keyData);
    }
}
