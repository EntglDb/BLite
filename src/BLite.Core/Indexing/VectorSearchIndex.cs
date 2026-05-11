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
    private struct NodeReference : IEquatable<NodeReference>
    {
        public uint PageId;
        public int NodeIndex;
        public int MaxLevel;

        // Equality considers only the physical identity (page + slot).
        // MaxLevel is metadata and must not affect HashSet lookups.
        public bool Equals(NodeReference other) => PageId == other.PageId && NodeIndex == other.NodeIndex;
        public override bool Equals(object? obj) => obj is NodeReference n && Equals(n);
        public override int GetHashCode() => HashCode.Combine(PageId, NodeIndex);
    }

    private readonly StorageEngine _storage;
    private readonly IndexOptions _options;
    private uint _rootPageId;
    private readonly Action<uint>? _onRootChanged;
    // Thread-safe random source compatible with netstandard2.1.
    // Random.Shared was introduced in .NET 6; use ThreadLocal<Random> as a
    // back-compat equivalent that is equally thread-safe and unbiased.
#if NET6_0_OR_GREATER
    private static readonly Random _random = Random.Shared;
#else
    private static readonly ThreadLocal<Random> _randomLocal = new(() => new Random());
    private static Random _random => _randomLocal.Value!;
#endif

    // Cached HNSW entry point (highest-level node in the graph)
    private uint _entryPageId;
    private int _entryNodeIndex;
    private int _entryMaxLevel;

    public VectorSearchIndex(StorageEngine storage, IndexOptions options, uint rootPageId = 0, Action<uint>? onRootChanged = null)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _options = options;
        _rootPageId = rootPageId;
        _onRootChanged = onRootChanged;

        if (_rootPageId != 0)
        {
            // Restore entry point from root page header
            var buffer = RentPageBuffer();
            try
            {
                _storage.ReadPage(_rootPageId, null, buffer);
                (_entryPageId, _entryNodeIndex, _entryMaxLevel) = VectorPage.GetEntryPoint(buffer);
            }
            finally { ReturnPageBuffer(buffer); }
        }
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
            _onRootChanged?.Invoke(_rootPageId);
            var pageBuffer = RentPageBuffer();
            try
            {
                _storage.ReadPage(_rootPageId, transaction?.TransactionId, pageBuffer);
                VectorPage.WriteNode(pageBuffer, 0, docLocation, targetLevel, vector, _options.Dimensions);
                VectorPage.IncrementNodeCount(pageBuffer);
                // Persist entry point: first node is always the initial entry point
                VectorPage.SetEntryPoint(pageBuffer, _rootPageId, 0, targetLevel);
                _entryPageId = _rootPageId;
                _entryNodeIndex = 0;
                _entryMaxLevel = targetLevel;

                if (transaction != null)
                    _storage.WritePage(_rootPageId, transaction.TransactionId, pageBuffer);
                else
                    _storage.WritePageImmediate(_rootPageId, pageBuffer);
            }
            finally { ReturnPageBuffer(pageBuffer); }
            return;
        }

        // HNSW Core logic (Malkov & Yashunin 2018, Algorithm 1)
        // 3. Find current entry point
        var entryPoint = GetEntryPoint();
        var currentPoint = entryPoint;

        // 4. Greedy search through layers above targetLevel (ef=1)
        for (int l = entryPoint.MaxLevel; l > targetLevel; l--)
        {
            currentPoint = GreedySearch(currentPoint, vector, l, transaction);
        }

        // 5. Create the new node
        var newNode = AllocateNode(vector, docLocation, targetLevel, transaction);

        // 6. For each layer from min(targetLevel, entryPoint.MaxLevel) down to 0:
        //    - Search with efConstruction → candidate set W
        //    - Select up to Mmax neighbours (Mmax = 2*M at layer 0, M elsewhere)
        //    - Bidirectionally link, then shrink neighbour's connections if oversized
        //    - Use W as entry-point set for the next (lower) layer
        for (int l = Math.Min(targetLevel, entryPoint.MaxLevel); l >= 0; l--)
        {
            int mMax = (l == 0) ? (2 * _options.M) : _options.M;

            // SearchLayer returns candidates ordered by ascending distance to vector.
            var wEnum = SearchLayer(currentPoint, vector, _options.EfConstruction, l, transaction);
            var w = wEnum as IList<NodeReference> ?? new List<NodeReference>(wEnum);

            var selectedNeighbors = SelectNeighbors(w, vector, _options.M, transaction);

            foreach (var neighbor in selectedNeighbors)
            {
                Link(newNode, neighbor, l, transaction);
                // Reverse link with shrinking on neighbour if over capacity.
                LinkWithShrinking(neighbor, newNode, l, mMax, transaction);
            }

            // Standard HNSW: use the W candidate set as entry for the next lower layer.
            // Pick the nearest candidate as the single entry point (current SearchLayer
            // signature accepts a single entry point).
            if (w.Count > 0)
                currentPoint = w[0];
        }

        // 7. Update entry point if new node reached a higher layer than any existing one.
        if (targetLevel > entryPoint.MaxLevel)
        {
            UpdateEntryPoint(newNode, transaction);
        }
    }

    // HNSW Algorithm 4 (Malkov & Yashunin, 2018) with keepPrunedConnections=true.
    // Keeps the closer-to-query-than-to-existing-selected heuristic but tops the result
    // up to `m` using the pruned (dominated) candidates in ascending distance order,
    // so the out-degree never collapses below what the candidate pool can supply.
    // `candidates` MUST be ordered by ascending distance to `query`.
    private List<NodeReference> SelectNeighbors(IList<NodeReference> candidates, float[] query, int m, ITransaction? transaction)
    {
        var result = new List<NodeReference>(m);
        var pruned = new List<NodeReference>();

        foreach (var e in candidates)
        {
            if (result.Count >= m) break;

            float distEQ = VectorMath.Distance(query, LoadVector(e, transaction), _options.Metric);
            bool dominated = false;
            foreach (var r in result)
            {
                float distER = VectorMath.Distance(LoadVector(e, transaction), LoadVector(r, transaction), _options.Metric);
                if (distER < distEQ) { dominated = true; break; }
            }
            if (dominated) pruned.Add(e);
            else           result.Add(e);
        }

        // keepPrunedConnections: top up with pruned candidates (already distance-ordered)
        // so we always reach `m` neighbours when the pool allows it.
        for (int i = 0; i < pruned.Count && result.Count < m; i++)
            result.Add(pruned[i]);

        return result;
    }

    /// <summary>
    /// Writes a one-way link from `from` to `to` at the given level.
    /// Idempotent: if `to` is already in the link list, no-op.
    /// If there is no empty slot, this function does NOT evict — call
    /// <see cref="LinkWithShrinking"/> on the side that owns the connection
    /// budget (typically the reverse direction) to enforce Mmax.
    /// </summary>
    private void Link(NodeReference from, NodeReference to, int level, ITransaction? transaction)
    {
        var buffer = RentPageBuffer();
        try
        {
            _storage.ReadPage(from.PageId, transaction?.TransactionId, buffer);
            var links = VectorPage.GetLinksSpan(buffer, from.NodeIndex, level, _options.Dimensions, _options.M);

            for (int i = 0; i < links.Length; i += 6)
            {
                var existing = DocumentLocation.ReadFrom(links.Slice(i, 6));
                if (existing.PageId == 0)
                {
                    new DocumentLocation(to.PageId, (ushort)to.NodeIndex).WriteTo(links.Slice(i, 6));
                    WritePage(from.PageId, transaction, buffer);
                    return;
                }
                if (existing.PageId == to.PageId && existing.SlotIndex == (ushort)to.NodeIndex)
                    return; // already linked, idempotent
            }
            // No empty slot: caller must invoke LinkWithShrinking instead.
        }
        finally { ReturnPageBuffer(buffer); }
    }

    /// <summary>
    /// Adds `to` to `from`'s neighbour list at `level`. If the list (plus the new
    /// candidate) exceeds <paramref name="mMax"/>, re-selects the best mMax using
    /// the canonical HNSW heuristic (Algorithm 4) on the full union, then rewrites
    /// the link slots. This is the "shrink connections" step from Algorithm 1.
    /// </summary>
    private void LinkWithShrinking(NodeReference from, NodeReference to, int level, int mMax, ITransaction? transaction)
    {
        var buffer = RentPageBuffer();
        try
        {
            _storage.ReadPage(from.PageId, transaction?.TransactionId, buffer);
            var links = VectorPage.GetLinksSpan(buffer, from.NodeIndex, level, _options.Dimensions, _options.M);
            int slotCount = links.Length / 6;

            // Collect current neighbours and check for duplicate / empty slot fast path.
            var current = new List<NodeReference>(slotCount);
            int firstEmpty = -1;
            for (int i = 0; i < links.Length; i += 6)
            {
                var existing = DocumentLocation.ReadFrom(links.Slice(i, 6));
                if (existing.PageId == 0)
                {
                    if (firstEmpty < 0) firstEmpty = i;
                    continue;
                }
                if (existing.PageId == to.PageId && existing.SlotIndex == (ushort)to.NodeIndex)
                    return; // already present
                current.Add(new NodeReference { PageId = existing.PageId, NodeIndex = existing.SlotIndex });
            }

            // Fast path: empty slot available — just write it in place.
            if (firstEmpty >= 0)
            {
                new DocumentLocation(to.PageId, (ushort)to.NodeIndex).WriteTo(links.Slice(firstEmpty, 6));
                WritePage(from.PageId, transaction, buffer);
                return;
            }

            // No empty slot AND list does not yet include `to`: run neighbour shrinking.
            // Build the candidate set (existing + new) sorted by distance to `from`.
            var fromVec = LoadVector(from, transaction);
            var union = new List<(NodeReference Node, float Dist)>(current.Count + 1);
            foreach (var c in current)
                union.Add((c, VectorMath.Distance(fromVec, LoadVector(c, transaction), _options.Metric)));
            union.Add((to, VectorMath.Distance(fromVec, LoadVector(to, transaction), _options.Metric)));
            union.Sort((a, b) => a.Dist.CompareTo(b.Dist));

            var ordered = new List<NodeReference>(union.Count);
            foreach (var u in union) ordered.Add(u.Node);

            var selected = SelectNeighbors(ordered, fromVec, mMax, transaction);

            // Rewrite the slot area: clear then write selected neighbours.
            links.Clear();
            for (int i = 0; i < selected.Count && i < slotCount; i++)
            {
                new DocumentLocation(selected[i].PageId, (ushort)selected[i].NodeIndex)
                    .WriteTo(links.Slice(i * 6, 6));
            }
            WritePage(from.PageId, transaction, buffer);
        }
        finally { ReturnPageBuffer(buffer); }
    }

    // Helper: choose the right write API based on whether a transaction is active.
    private void WritePage(uint pageId, ITransaction? transaction, byte[] buffer)
    {
        if (transaction != null)
            _storage.WritePage(pageId, transaction.TransactionId, buffer);
        else
            _storage.WritePageImmediate(pageId, buffer);
    }

    private NodeReference AllocateNode(float[] vector, DocumentLocation docLoc, int level, ITransaction? transaction)
    {
        // Walk the _rootPageId → NextPageId chain looking for a page that still has
        // capacity.  If none exists, allocate a new page and link it to the tail.
        var buffer = RentPageBuffer();
        try
        {
            uint pageId = _rootPageId;
            uint prevPageId = 0;

            while (true)
            {
                _storage.ReadPage(pageId, transaction?.TransactionId, buffer);
                int nodeCount = VectorPage.GetNodeCount(buffer);
                int maxNodes  = VectorPage.GetMaxNodes(buffer);

                if (nodeCount < maxNodes)
                {
                    // This page has room — write the node here.
                    VectorPage.WriteNode(buffer, nodeCount, docLoc, level, vector, _options.Dimensions);
                    VectorPage.IncrementNodeCount(buffer);
                    WritePage(pageId, transaction, buffer);
                    return new NodeReference { PageId = pageId, NodeIndex = nodeCount, MaxLevel = level };
                }

                // Follow the chain.
                var header = PageHeader.ReadFrom(buffer);
                if (header.NextPageId != 0)
                {
                    prevPageId = pageId;
                    pageId = header.NextPageId;
                }
                else
                {
                    // No more pages in chain — allocate a new one and link it.
                    uint newPageId = CreateNewPage(transaction);
                    LinkPageChain(pageId, newPageId, transaction);

                    // Write node into fresh page.
                    _storage.ReadPage(newPageId, transaction?.TransactionId, buffer);
                    VectorPage.WriteNode(buffer, 0, docLoc, level, vector, _options.Dimensions);
                    VectorPage.IncrementNodeCount(buffer);
                    WritePage(newPageId, transaction, buffer);
                    return new NodeReference { PageId = newPageId, NodeIndex = 0, MaxLevel = level };
                }
            }
        }
        finally { ReturnPageBuffer(buffer); }
    }

    // Sets header.NextPageId on `fromPageId` so that CollectAllPages() can follow
    // the full page chain and subsequent AllocateNode() calls find all pages.
    private void LinkPageChain(uint fromPageId, uint toPageId, ITransaction? transaction)
    {
        var buffer = RentPageBuffer();
        try
        {
            _storage.ReadPage(fromPageId, transaction?.TransactionId, buffer);
            var header = PageHeader.ReadFrom(buffer);
            header.NextPageId = toPageId;
            header.WriteTo(buffer);
            WritePage(fromPageId, transaction, buffer);
        }
        finally { ReturnPageBuffer(buffer); }
    }

    private void UpdateEntryPoint(NodeReference newEntry, ITransaction? transaction)
    {
        _entryPageId = newEntry.PageId;
        _entryNodeIndex = newEntry.NodeIndex;
        _entryMaxLevel = newEntry.MaxLevel;

        // Persist entry point into root page header so it survives restart
        var buffer = RentPageBuffer();
        try
        {
            _storage.ReadPage(_rootPageId, transaction?.TransactionId, buffer);
            VectorPage.SetEntryPoint(buffer, newEntry.PageId, newEntry.NodeIndex, newEntry.MaxLevel);
            if (transaction != null)
                _storage.WritePage(_rootPageId, transaction.TransactionId, buffer);
            else
                _storage.WritePageImmediate(_rootPageId, buffer);
        }
        finally { ReturnPageBuffer(buffer); }
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
        if (_entryPageId != 0)
            return new NodeReference { PageId = _entryPageId, NodeIndex = _entryNodeIndex, MaxLevel = _entryMaxLevel };
        // Fallback for an index opened before entry-point tracking was available
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
        
        // 3. Return top-k results.
        // SearchLayer already computed distances; we retrieve location + vector together
        // in a single page read to avoid the double-LoadVector that the old code did.
        int count = 0;
        foreach (var node in nearest)
        {
            if (count++ >= k) break;

            var buffer = RentPageBuffer();
            try
            {
                _storage.ReadPage(node.PageId, transaction?.TransactionId, buffer);
                float[] vec = new float[_options.Dimensions];
                VectorPage.ReadNodeData(buffer, node.NodeIndex, out var loc, out _, vec);
                float dist = VectorMath.Distance(query, vec, _options.Metric);
                yield return new VectorSearchResult(loc, dist);
            }
            finally { ReturnPageBuffer(buffer); }
        }
    }

    private DocumentLocation LoadDocumentLocation(NodeReference node, ITransaction? transaction)
    {
        var buffer = RentPageBuffer();
        try
        {
            _storage.ReadPage(node.PageId, transaction?.TransactionId, buffer);
            // Use the zero-allocation overload: skip the vector bytes entirely.
            return VectorPage.ReadLocation(buffer, node.NodeIndex);
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
        // HNSW paper (Malkov & Yashunin 2018): recommended multiplier is 1/ln(M).
        // For M=16 this gives mL≈0.36, producing a better-connected multi-layer graph
        // than the original 1/M (≈0.063) which was too steep.
        double mL = 1.0 / Math.Log(_options.M);
        return Math.Min((int)Math.Floor(-Math.Log(_random.NextDouble()) * mL), 15);
    }

    private uint CreateNewPage(ITransaction? transaction)
    {
        uint pageId = _storage.AllocateIndexPage();
        var buffer = RentPageBuffer();
        try
        {
            VectorPage.Initialize(buffer, pageId, _options.Dimensions, _options.M);
            // Use the transaction's WAL cache when available so that a rollback
            // doesn't leave orphaned pages on disk.
            WritePage(pageId, transaction, buffer);
            return pageId;
        }
        finally { ReturnPageBuffer(buffer); }
    }

    private byte[] RentPageBuffer() => System.Buffers.ArrayPool<byte>.Shared.Rent(_storage.PageSize);
    private void ReturnPageBuffer(byte[] buffer) => System.Buffers.ArrayPool<byte>.Shared.Return(buffer);

    /// <summary>
    /// Returns all physical page IDs owned by this vector index.
    /// Used when dropping an index to free storage.
    /// </summary>
    public IReadOnlyList<uint> CollectAllPages()
    {
        if (_rootPageId == 0) return Array.Empty<uint>();
        // Multi-page vector graphs store all pages starting from _rootPageId;
        // follow NextPageId chain to collect the full set.
        var pages = new List<uint>();
        var buffer = RentPageBuffer();
        try
        {
            var current = _rootPageId;
            while (current != 0)
            {
                pages.Add(current);
                _storage.ReadPage(current, null, buffer);
                var header = PageHeader.ReadFrom(buffer);
                current = header.NextPageId;
            }
        }
        finally { ReturnPageBuffer(buffer); }
        return pages;
    }
}

public record struct VectorSearchResult(DocumentLocation Location, float Distance);
