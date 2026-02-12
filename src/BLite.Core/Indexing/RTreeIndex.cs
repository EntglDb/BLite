using BLite.Core.Storage;
using BLite.Core.Transactions;
using BLite.Core.Collections;
using BLite.Core.Indexing.Internal;
using System.Buffers;

namespace BLite.Core.Indexing;

/// <summary>
/// R-Tree Index implementation for Geospatial Indexing.
/// Uses Quadratic Split algorithm for simplicity and efficiency in paged storage.
/// </summary>
internal class RTreeIndex : IDisposable
{
    private readonly StorageEngine _storage;
    private readonly IndexOptions _options;
    private uint _rootPageId;
    private readonly object _lock = new();
    private readonly int _pageSize;

    public RTreeIndex(StorageEngine storage, IndexOptions options, uint rootPageId)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _options = options;
        _rootPageId = rootPageId;
        _pageSize = _storage.PageSize;

        if (_rootPageId == 0)
        {
            InitializeNewIndex();
        }
    }

    public uint RootPageId => _rootPageId;

    private void InitializeNewIndex()
    {
        var buffer = RentPageBuffer();
        try
        {
            _rootPageId = _storage.AllocatePage();
            SpatialPage.Initialize(buffer, _rootPageId, true, 0);
            _storage.WritePageImmediate(_rootPageId, buffer);
        }
        finally { ReturnPageBuffer(buffer); }
    }

    public IEnumerable<DocumentLocation> Search(GeoBox area, ITransaction? transaction = null)
    {
        if (_rootPageId == 0) yield break;

        var stack = new Stack<uint>();
        stack.Push(_rootPageId);

        var buffer = RentPageBuffer();
        try
        {
            while (stack.Count > 0)
            {
                uint pageId = stack.Pop();
                _storage.ReadPage(pageId, transaction?.TransactionId, buffer);

                bool isLeaf = SpatialPage.GetIsLeaf(buffer);
                ushort count = SpatialPage.GetEntryCount(buffer);

                for (int i = 0; i < count; i++)
                {
                    SpatialPage.ReadEntry(buffer, i, out var mbr, out var pointer);

                    if (area.Intersects(mbr))
                    {
                        if (isLeaf)
                        {
                            yield return pointer;
                        }
                        else
                        {
                            stack.Push(pointer.PageId);
                        }
                    }
                }
            }
        }
        finally { ReturnPageBuffer(buffer); }
    }

    public void Insert(GeoBox mbr, DocumentLocation loc, ITransaction? transaction = null)
    {
        lock (_lock)
        {
            var leafPageId = ChooseLeaf(_rootPageId, mbr, transaction);
            InsertIntoNode(leafPageId, mbr, loc, transaction);
        }
    }

    private uint ChooseLeaf(uint rootId, GeoBox mbr, ITransaction? transaction)
    {
        uint currentId = rootId;
        var buffer = RentPageBuffer();
        try
        {
            while (true)
            {
                _storage.ReadPage(currentId, transaction?.TransactionId, buffer);
                if (SpatialPage.GetIsLeaf(buffer)) return currentId;

                ushort count = SpatialPage.GetEntryCount(buffer);
                uint bestChild = 0;
                double minEnlargement = double.MaxValue;
                double minArea = double.MaxValue;

                for (int i = 0; i < count; i++)
                {
                    SpatialPage.ReadEntry(buffer, i, out var childMbr, out var pointer);
                    
                    var expanded = childMbr.ExpandTo(mbr);
                    double enlargement = expanded.Area - childMbr.Area;

                    if (enlargement < minEnlargement)
                    {
                        minEnlargement = enlargement;
                        minArea = childMbr.Area;
                        bestChild = pointer.PageId;
                    }
                    else if (enlargement == minEnlargement)
                    {
                        if (childMbr.Area < minArea)
                        {
                            minArea = childMbr.Area;
                            bestChild = pointer.PageId;
                        }
                    }
                }

                currentId = bestChild;
            }
        }
        finally { ReturnPageBuffer(buffer); }
    }

    private void InsertIntoNode(uint pageId, GeoBox mbr, DocumentLocation pointer, ITransaction? transaction)
    {
        var buffer = RentPageBuffer();
        try
        {
            _storage.ReadPage(pageId, transaction?.TransactionId, buffer);
            ushort count = SpatialPage.GetEntryCount(buffer);
            int maxEntries = SpatialPage.GetMaxEntries(_pageSize);

            if (count < maxEntries)
            {
                SpatialPage.WriteEntry(buffer, count, mbr, pointer);
                SpatialPage.SetEntryCount(buffer, (ushort)(count + 1));
                
                if (transaction != null)
                    _storage.WritePage(pageId, transaction.TransactionId, buffer);
                else
                    _storage.WritePageImmediate(pageId, buffer);

                // Propagate MBR update upwards
                UpdateMBRUpwards(pageId, transaction);
            }
            else
            {
                SplitNode(pageId, mbr, pointer, transaction);
            }
        }
        finally { ReturnPageBuffer(buffer); }
    }

    private void UpdateMBRUpwards(uint pageId, ITransaction? transaction)
    {
        var buffer = RentPageBuffer();
        var parentBuffer = RentPageBuffer();
        try
        {
            uint currentId = pageId;
            while (currentId != _rootPageId)
            {
                _storage.ReadPage(currentId, transaction?.TransactionId, buffer);
                var currentMbr = SpatialPage.CalculateMBR(buffer);
                uint parentId = SpatialPage.GetParentPageId(buffer);

                if (parentId == 0) break;

                _storage.ReadPage(parentId, transaction?.TransactionId, parentBuffer);
                ushort count = SpatialPage.GetEntryCount(parentBuffer);
                bool changed = false;

                for (int i = 0; i < count; i++)
                {
                    SpatialPage.ReadEntry(parentBuffer, i, out var mbr, out var pointer);
                    if (pointer.PageId == currentId)
                    {
                        if (mbr != currentMbr)
                        {
                            SpatialPage.WriteEntry(parentBuffer, i, currentMbr, pointer);
                            changed = true;
                        }
                        break;
                    }
                }

                if (!changed) break;

                if (transaction != null)
                    _storage.WritePage(parentId, transaction.TransactionId, parentBuffer);
                else
                    _storage.WritePageImmediate(parentId, parentBuffer);

                currentId = parentId;
            }
        }
        finally 
        { 
            ReturnPageBuffer(buffer); 
            ReturnPageBuffer(parentBuffer);
        }
    }

    private void SplitNode(uint pageId, GeoBox newMbr, DocumentLocation newPointer, ITransaction? transaction)
    {
        var buffer = RentPageBuffer();
        var newBuffer = RentPageBuffer();
        try
        {
            _storage.ReadPage(pageId, transaction?.TransactionId, buffer);
            bool isLeaf = SpatialPage.GetIsLeaf(buffer);
            byte level = SpatialPage.GetLevel(buffer);
            ushort count = SpatialPage.GetEntryCount(buffer);
            uint parentId = SpatialPage.GetParentPageId(buffer);

            // Collect all entries including the new one
            var entries = new List<(GeoBox Mbr, DocumentLocation Pointer)>();
            for (int i = 0; i < count; i++)
            {
                SpatialPage.ReadEntry(buffer, i, out var m, out var p);
                entries.Add((m, p));
            }
            entries.Add((newMbr, newPointer));

            // Pick Seeds
            PickSeeds(entries, out var seed1, out var seed2);
            entries.Remove(seed1);
            entries.Remove(seed2);

            // Initialize two nodes
            uint newPageId = _storage.AllocatePage();
            SpatialPage.Initialize(buffer, pageId, isLeaf, level);
            SpatialPage.Initialize(newBuffer, newPageId, isLeaf, level);
            SpatialPage.SetParentPageId(buffer, parentId);
            SpatialPage.SetParentPageId(newBuffer, parentId);

            SpatialPage.WriteEntry(buffer, 0, seed1.Mbr, seed1.Pointer);
            SpatialPage.SetEntryCount(buffer, 1);
            SpatialPage.WriteEntry(newBuffer, 0, seed2.Mbr, seed2.Pointer);
            SpatialPage.SetEntryCount(newBuffer, 1);

            GeoBox mbr1 = seed1.Mbr;
            GeoBox mbr2 = seed2.Mbr;

            // Distribute remaining entries
            while (entries.Count > 0)
            {
                var entry = entries[0];
                entries.RemoveAt(0);

                var exp1 = mbr1.ExpandTo(entry.Mbr);
                var exp2 = mbr2.ExpandTo(entry.Mbr);
                double d1 = exp1.Area - mbr1.Area;
                double d2 = exp2.Area - mbr2.Area;

                if (d1 < d2)
                {
                    int idx = SpatialPage.GetEntryCount(buffer);
                    SpatialPage.WriteEntry(buffer, idx, entry.Mbr, entry.Pointer);
                    SpatialPage.SetEntryCount(buffer, (ushort)(idx + 1));
                    mbr1 = exp1;
                }
                else
                {
                    int idx = SpatialPage.GetEntryCount(newBuffer);
                    SpatialPage.WriteEntry(newBuffer, idx, entry.Mbr, entry.Pointer);
                    SpatialPage.SetEntryCount(newBuffer, (ushort)(idx + 1));
                    mbr2 = exp2;
                }
            }

            // Write pages
            if (transaction != null)
            {
                _storage.WritePage(pageId, transaction.TransactionId, buffer);
                _storage.WritePage(newPageId, transaction.TransactionId, newBuffer);
            }
            else
            {
                _storage.WritePageImmediate(pageId, buffer);
                _storage.WritePageImmediate(newPageId, newBuffer);
            }

            // Propagate split upwards
            if (pageId == _rootPageId)
            {
                // New Root
                uint newRootId = _storage.AllocatePage();
                SpatialPage.Initialize(buffer, newRootId, false, (byte)(level + 1));
                SpatialPage.WriteEntry(buffer, 0, mbr1, new DocumentLocation(pageId, 0));
                SpatialPage.WriteEntry(buffer, 1, mbr2, new DocumentLocation(newPageId, 0));
                SpatialPage.SetEntryCount(buffer, 2);
                
                if (transaction != null)
                    _storage.WritePage(newRootId, transaction.TransactionId, buffer);
                else
                    _storage.WritePageImmediate(newRootId, buffer);

                _rootPageId = newRootId;
                
                // Update parent pointers
                UpdateParentPointer(pageId, newRootId, transaction);
                UpdateParentPointer(newPageId, newRootId, transaction);
            }
            else
            {
                // Insert second node into parent
                InsertIntoNode(parentId, mbr2, new DocumentLocation(newPageId, 0), transaction);
                UpdateMBRUpwards(pageId, transaction);
            }
        }
        finally 
        { 
            ReturnPageBuffer(buffer); 
            ReturnPageBuffer(newBuffer);
        }
    }

    private void UpdateParentPointer(uint pageId, uint parentId, ITransaction? transaction)
    {
        var buffer = RentPageBuffer();
        try
        {
            _storage.ReadPage(pageId, transaction?.TransactionId, buffer);
            SpatialPage.SetParentPageId(buffer, parentId);
            if (transaction != null)
                _storage.WritePage(pageId, transaction.TransactionId, buffer);
            else
                _storage.WritePageImmediate(pageId, buffer);
        }
        finally { ReturnPageBuffer(buffer); }
    }

    private void PickSeeds(List<(GeoBox Mbr, DocumentLocation Pointer)> entries, out (GeoBox Mbr, DocumentLocation Pointer) s1, out (GeoBox Mbr, DocumentLocation Pointer) s2)
    {
        double maxWaste = double.MinValue;
        s1 = entries[0];
        s2 = entries[1];

        for (int i = 0; i < entries.Count; i++)
        {
            for (int j = i + 1; j < entries.Count; j++)
            {
                var combined = entries[i].Mbr.ExpandTo(entries[j].Mbr);
                double waste = combined.Area - entries[i].Mbr.Area - entries[j].Mbr.Area;
                if (waste > maxWaste)
                {
                    maxWaste = waste;
                    s1 = entries[i];
                    s2 = entries[j];
                }
            }
        }
    }

    private byte[] RentPageBuffer()
    {
        return ArrayPool<byte>.Shared.Rent(_pageSize);
    }

    private void ReturnPageBuffer(byte[] buffer)
    {
        ArrayPool<byte>.Shared.Return(buffer);
    }

    public void Dispose()
    {
    }
}
