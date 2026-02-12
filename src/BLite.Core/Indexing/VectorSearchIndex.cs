using BLite.Core.Storage;
using BLite.Core.Transactions;
using System.Collections.Generic;

namespace BLite.Core.Indexing;

/// <summary>
/// HNSW (Hierarchical Navigable Small World) index implementation.
/// Handles multi-vector indexing and similarity searches.
/// </summary>
public sealed class VectorSearchIndex
{
    private struct NodeReference
    {
        public uint PageId;
        public int NodeIndex;
        public int MaxLevel;
    }

    private readonly StorageEngine _storage;
    private readonly IndexOptions _options;
    private uint _rootPageId;
    private readonly Random _random = new(42);

    public VectorSearchIndex(StorageEngine storage, IndexOptions options, uint rootPageId = 0)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _options = options;
        _rootPageId = rootPageId;
    }

    public uint RootPageId => _rootPageId;

    public void Insert(float[] vector, DocumentLocation docLocation, ITransaction? transaction = null)
    {
        if (vector.Length != _options.Dimensions)
            throw new ArgumentException($"Vector dimension mismatch. Expected {_options.Dimensions}, got {vector.Length}");

        // 1. Determine level for new node
        int targetLevel = GetRandomLevel();

        // 2. If index is empty, create first page and first node
        if (_rootPageId == 0)
        {
            _rootPageId = CreateNewPage(transaction);
            var pageBuffer = RentPageBuffer();
            try
            {
                _storage.ReadPage(_rootPageId, transaction?.TransactionId, pageBuffer);
                VectorPage.WriteNode(pageBuffer, 0, docLocation, targetLevel, vector, _options.Dimensions);
                VectorPage.IncrementNodeCount(pageBuffer); // Helper needs to be added or handled
                
                if (transaction != null)
                    _storage.WritePage(_rootPageId, transaction.TransactionId, pageBuffer);
                else
                    _storage.WritePageImmediate(_rootPageId, pageBuffer);
            }
            finally { ReturnPageBuffer(pageBuffer); }
            return;
        }

        // HNSW Core logic
        // 3. Find current entry point
        var entryPoint = GetEntryPoint();
        var currentPoint = entryPoint;

        // 4. Greedy search down to targetLevel+1
        for (int l = entryPoint.MaxLevel; l > targetLevel; l--)
        {
            currentPoint = GreedySearch(currentPoint, vector, l, transaction);
        }

        // 5. Create the new node
        var newNode = AllocateNode(vector, docLocation, targetLevel, transaction);

        // 6. For each layer from targetLevel down to 0
        for (int l = Math.Min(targetLevel, entryPoint.MaxLevel); l >= 0; l--)
        {
            var neighbors = SearchLayer(currentPoint, vector, _options.EfConstruction, l, transaction);
            var selectedNeighbors = SelectNeighbors(neighbors, vector, _options.M, l, transaction);
            
            foreach (var neighbor in selectedNeighbors)
            {
                AddBidirectionalLink(newNode, neighbor, l, transaction);
            }
            
            // Move currentPoint down for next level if available
            currentPoint = GreedySearch(currentPoint, vector, l, transaction);
        }

        // 7. Update entry point if new node is higher
        if (targetLevel > entryPoint.MaxLevel)
        {
            UpdateEntryPoint(newNode, transaction);
        }
    }

    private IEnumerable<NodeReference> SelectNeighbors(IEnumerable<NodeReference> candidates, float[] query, int m, int level, ITransaction? transaction)
    {
        // Simple heuristic: just take top M nearest.
        // HNSW Paper suggests more complex heuristic to maintain connectivity diversity.
        return candidates.Take(m);
    }

    private void AddBidirectionalLink(NodeReference node1, NodeReference node2, int level, ITransaction? transaction)
    {
        Link(node1, node2, level, transaction);
        Link(node2, node1, level, transaction);
    }

    private void Link(NodeReference from, NodeReference to, int level, ITransaction? transaction)
    {
        var buffer = RentPageBuffer();
        try
        {
            _storage.ReadPage(from.PageId, transaction?.TransactionId, buffer);
            var links = VectorPage.GetLinksSpan(buffer, from.NodeIndex, level, _options.Dimensions, _options.M);
            
            // Find first empty slot (PageId == 0)
            for (int i = 0; i < links.Length; i += 6)
            {
                var existing = DocumentLocation.ReadFrom(links.Slice(i, 6));
                if (existing.PageId == 0)
                {
                    new DocumentLocation(to.PageId, (ushort)to.NodeIndex).WriteTo(links.Slice(i, 6));
                    
                    if (transaction != null)
                        _storage.WritePage(from.PageId, transaction.TransactionId, buffer);
                    else
                        _storage.WritePageImmediate(from.PageId, buffer);
                    return;
                }
            }
            // If full, we should technically prune or redistribute links as per HNSW paper.
            // For now, we assume M is large enough or we skip (limited connectivity).
        }
        finally { ReturnPageBuffer(buffer); }
    }

    private NodeReference AllocateNode(float[] vector, DocumentLocation docLoc, int level, ITransaction? transaction)
    {
        // Find a page with space or create new
        // For simplicity, we search for a page with available slots or append to a new one.
        // Implementation omitted for brevity but required for full persistence.
        uint pageId = _rootPageId; // Placeholder: need allocation strategy
        int index = 0; 
        
        var buffer = RentPageBuffer();
        try
        {
             _storage.ReadPage(pageId, transaction?.TransactionId, buffer);
             index = VectorPage.GetNodeCount(buffer);
             VectorPage.WriteNode(buffer, index, docLoc, level, vector, _options.Dimensions);
             VectorPage.IncrementNodeCount(buffer);
             
             if (transaction != null)
                _storage.WritePage(pageId, transaction.TransactionId, buffer);
             else
                _storage.WritePageImmediate(pageId, buffer);
        }
        finally { ReturnPageBuffer(buffer); }
        
        return new NodeReference { PageId = pageId, NodeIndex = index, MaxLevel = level };
    }

    private void UpdateEntryPoint(NodeReference newEntry, ITransaction? transaction)
    {
        // Store in index header or collection metadata
        // For now, it's a simplification.
    }

    private NodeReference GreedySearch(NodeReference entryPoint, float[] query, int level, ITransaction? transaction)
    {
        bool changed = true;
        var current = entryPoint;
        float currentDist = VectorMath.Distance(query, LoadVector(current, transaction), _options.Metric);

        while (changed)
        {
            changed = false;
            foreach (var neighbor in GetNeighbors(current, level, transaction))
            {
                float dist = VectorMath.Distance(query, LoadVector(neighbor, transaction), _options.Metric);
                if (dist < currentDist)
                {
                    currentDist = dist;
                    current = neighbor;
                    changed = true;
                }
            }
        }
        return current;
    }

    private IEnumerable<NodeReference> SearchLayer(NodeReference entryPoint, float[] query, int ef, int level, ITransaction? transaction)
    {
        var visited = new HashSet<NodeReference>();
        var candidates = new PriorityQueue<NodeReference, float>();
        var result = new PriorityQueue<NodeReference, float>();

        float dist = VectorMath.Distance(query, LoadVector(entryPoint, transaction), _options.Metric);
        candidates.Enqueue(entryPoint, dist);
        result.Enqueue(entryPoint, -dist); // Max-heap for results
        visited.Add(entryPoint);

        while (candidates.Count > 0)
        {
            float d_c = 0;
            candidates.TryPeek(out var c, out d_c);
            result.TryPeek(out var f, out var d_f);
            
            if (d_c > -d_f) break;

            candidates.Dequeue();

            foreach (var e in GetNeighbors(c, level, transaction))
            {
                if (!visited.Contains(e))
                {
                    visited.Add(e);
                    result.TryPeek(out _, out d_f);
                    float d_e = VectorMath.Distance(query, LoadVector(e, transaction), _options.Metric);

                    if (d_e < -d_f || result.Count < ef)
                    {
                        candidates.Enqueue(e, d_e);
                        result.Enqueue(e, -d_e);
                        if (result.Count > ef) result.Dequeue();
                    }
                }
            }
        }

        // Convert result to list (ordered by distance)
        var list = new List<NodeReference>();
        while (result.Count > 0) list.Add(result.Dequeue());
        list.Reverse();
        return list;
    }

    private NodeReference GetEntryPoint()
    {
        // For now, assume a fixed location or track it in page 0 of index
        // TODO: Real implementation
        return new NodeReference { PageId = _rootPageId, NodeIndex = 0, MaxLevel = 0 };
    }

    private float[] LoadVector(NodeReference node, ITransaction? transaction)
    {
        var buffer = RentPageBuffer();
        try
        {
            _storage.ReadPage(node.PageId, transaction?.TransactionId, buffer);
            float[] vector = new float[_options.Dimensions];
            VectorPage.ReadNodeData(buffer, node.NodeIndex, out _, out _, vector);
            return vector;
        }
        finally { ReturnPageBuffer(buffer); }
    }

    public IEnumerable<VectorSearchResult> Search(float[] query, int k, int efSearch = 100, ITransaction? transaction = null)
    {
        if (_rootPageId == 0) yield break;

        var entryPoint = GetEntryPoint();
        var currentPoint = entryPoint;

        // 1. Greedy search through higher layers to find entry point for level 0
        for (int l = entryPoint.MaxLevel; l > 0; l--)
        {
            currentPoint = GreedySearch(currentPoint, query, l, transaction);
        }

        // 2. Comprehensive search on level 0
        var nearest = SearchLayer(currentPoint, query, Math.Max(efSearch, k), 0, transaction);
        
        // 3. Return top-k results
        int count = 0;
        foreach (var node in nearest)
        {
            if (count++ >= k) break;
            
            float dist = VectorMath.Distance(query, LoadVector(node, transaction), _options.Metric);
            var loc = LoadDocumentLocation(node, transaction);
            yield return new VectorSearchResult(loc, dist);
        }
    }

    private DocumentLocation LoadDocumentLocation(NodeReference node, ITransaction? transaction)
    {
        var buffer = RentPageBuffer();
        try
        {
            _storage.ReadPage(node.PageId, transaction?.TransactionId, buffer);
            VectorPage.ReadNodeData(buffer, node.NodeIndex, out var loc, out _, new float[0]); // Vector not needed here
            return loc;
        }
        finally { ReturnPageBuffer(buffer); }
    }

    private IEnumerable<NodeReference> GetNeighbors(NodeReference node, int level, ITransaction? transaction)
    {
        var buffer = RentPageBuffer();
        var results = new List<NodeReference>();
        try
        {
            _storage.ReadPage(node.PageId, transaction?.TransactionId, buffer);
            var links = VectorPage.GetLinksSpan(buffer, node.NodeIndex, level, _options.Dimensions, _options.M);
            
            for (int i = 0; i < links.Length; i += 6)
            {
                var loc = DocumentLocation.ReadFrom(links.Slice(i, 6));
                if (loc.PageId == 0) break; // End of links
                
                results.Add(new NodeReference { PageId = loc.PageId, NodeIndex = loc.SlotIndex });
            }
        }
        finally { ReturnPageBuffer(buffer); }
        return results;
    }

    private int GetRandomLevel()
    {
        // Probability p = 1/M for each level
        double p = 1.0 / _options.M;
        int level = 0;
        while (_random.NextDouble() < p && level < 15)
        {
            level++;
        }
        return level;
    }

    private uint CreateNewPage(ITransaction? transaction)
    {
        uint pageId = _storage.AllocatePage();
        var buffer = RentPageBuffer();
        try
        {
            VectorPage.Initialize(buffer, pageId, _options.Dimensions, _options.M);
            _storage.WritePageImmediate(pageId, buffer);
            return pageId;
        }
        finally { ReturnPageBuffer(buffer); }
    }

    private byte[] RentPageBuffer() => System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);
    private void ReturnPageBuffer(byte[] buffer) => System.Buffers.ArrayPool<byte>.Shared.Return(buffer);
}

public record struct VectorSearchResult(DocumentLocation Location, float Distance);
