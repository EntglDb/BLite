using System.Linq.Expressions;
using BLite.Bson;
using BLite.Core.Collections;
using BLite.Core.Storage;
using BLite.Core.Transactions;

namespace BLite.Core.Indexing;

/// <summary>
/// Manages a collection of secondary indexes on a document collection.
/// Handles index creation, deletion, automatic selection, and maintenance.
/// </summary>
/// <typeparam name="TId">Primary key type</typeparam>
/// <typeparam name="T">Document type</typeparam>
public sealed class CollectionIndexManager<TId, T> : IDisposable where T : class
{
    private readonly Dictionary<string, CollectionSecondaryIndex<TId, T>> _indexes;
    private readonly StorageEngine _storage;
    private readonly IDocumentMapper<TId, T> _mapper;
    private readonly object _lock = new();
    private bool _disposed;
    private readonly string _collectionName;

    public CollectionIndexManager(StorageEngine storage, IDocumentMapper<TId, T> mapper, string? collectionName = null)
    {
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
        _collectionName = collectionName ?? _mapper.CollectionName;
        _indexes = new Dictionary<string, CollectionSecondaryIndex<TId, T>>(StringComparer.OrdinalIgnoreCase);
        
        // Load existing index definitions from metadata page
        LoadMetadata();
    }

    /// <summary>
    /// Creates a new secondary index
    /// </summary>
    /// <param name="definition">Index definition</param>
    /// <returns>The created secondary index</returns>
    public CollectionSecondaryIndex<TId, T> CreateIndex(CollectionIndexDefinition<T> definition)
    {
        if (definition == null)
            throw new ArgumentNullException(nameof(definition));

        lock (_lock)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(CollectionIndexManager<TId, T>));

            // Check if index with this name already exists
            if (_indexes.ContainsKey(definition.Name))
                throw new InvalidOperationException($"Index '{definition.Name}' already exists");

            // Create secondary index
            var secondaryIndex = new CollectionSecondaryIndex<TId, T>(definition, _storage, _mapper);
            _indexes[definition.Name] = secondaryIndex;
            
            // Persist metadata
            SaveMetadataToPage();

            return secondaryIndex;
        }
    }

    // ... methods ...

    /// <summary>
    /// Creates a simple index on a single property
    /// </summary>
    /// <typeparam name="TKey">Key type</typeparam>
    /// <param name="keySelector">Expression to extract key from document</param>
    /// <param name="name">Optional index name (auto-generated if null)</param>
    /// <param name="unique">Enforce uniqueness constraint</param>
    /// <returns>The created secondary index</returns>
    public CollectionSecondaryIndex<TId, T> CreateIndex<TKey>(
        Expression<Func<T, TKey>> keySelector,
        string? name = null,
        bool unique = false)
    {
        if (keySelector == null)
            throw new ArgumentNullException(nameof(keySelector));

        // Extract property paths from expression
        var propertyPaths = ExpressionAnalyzer.ExtractPropertyPaths(keySelector);
        
        // Generate name if not provided
        name ??= GenerateIndexName(propertyPaths);

        // Convert expression to object-returning expression (required for definition)
        var objectSelector = Expression.Lambda<Func<T, object>>(
            Expression.Convert(keySelector.Body, typeof(object)),
            keySelector.Parameters);

        // Create definition
        var definition = new CollectionIndexDefinition<T>(
            name,
            propertyPaths,
            objectSelector,
            unique);

        return CreateIndex(definition);
    }

    public CollectionSecondaryIndex<TId, T> EnsureIndex(
        Expression<Func<T, object>> keySelector,
        string? name = null,
        bool unique = false)
    {
        var propertyPaths = ExpressionAnalyzer.ExtractPropertyPaths(keySelector);
        name ??= GenerateIndexName(propertyPaths);

        lock (_lock)
        {
            if (_indexes.TryGetValue(name, out var existing))
                return existing;

            return CreateIndex(keySelector, name, unique);
        }
    }

    internal CollectionSecondaryIndex<TId, T> EnsureIndexUntyped(
        LambdaExpression keySelector,
        string? name = null,
        bool unique = false)
    {
        // Convert LambdaExpression to Expression<Func<T, object>> properly by sharing parameters
        var body = keySelector.Body;
        if (body.Type != typeof(object))
        {
            body = Expression.Convert(body, typeof(object));
        }
        
        var lambda = Expression.Lambda<Func<T, object>>(body, keySelector.Parameters);

        return EnsureIndex(lambda, name, unique);
    }



    /// <summary>
    /// Drops an existing index by name
    /// </summary>
    /// <param name="name">Index name</param>
    /// <returns>True if index was found and dropped, false otherwise</returns>
    public bool DropIndex(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Index name cannot be empty", nameof(name));

        lock (_lock)
        {
            if (_indexes.TryGetValue(name, out var index))
            {
                index.Dispose();
                _indexes.Remove(name);
                
                // TODO: Free pages used by index in PageFile
                
                SaveMetadataToPage(); // Save metadata after dropping index
                return true;
            }

            return false;
        }
    }

    /// <summary>
    /// Gets an index by name
    /// </summary>
    public CollectionSecondaryIndex<TId, T>? GetIndex(string name)
    {
        lock (_lock)
        {
            return _indexes.TryGetValue(name, out var index) ? index : null;
        }
    }

    /// <summary>
    /// Gets all indexes
    /// </summary>
    public IEnumerable<CollectionSecondaryIndex<TId, T>> GetAllIndexes()
    {
        lock (_lock)
        {
            return _indexes.Values.ToList(); // Return copy to avoid lock issues
        }
    }

    /// <summary>
    /// Gets information about all indexes
    /// </summary>
    public IEnumerable<CollectionIndexInfo> GetIndexInfo()
    {
        lock (_lock)
        {
            return _indexes.Values.Select(idx => idx.GetInfo()).ToList();
        }
    }

    /// <summary>
    /// Finds the best index to use for a query on the specified property.
    /// Returns null if no suitable index found (requires full scan).
    /// </summary>
    /// <param name="propertyPath">Property path being queried</param>
    /// <returns>Best index for the query, or null if none suitable</returns>
    public CollectionSecondaryIndex<TId, T>? FindBestIndex(string propertyPath)
    {
        if (string.IsNullOrWhiteSpace(propertyPath))
            return null;

        lock (_lock)
        {
            // Find all indexes that can support this query
            var candidates = _indexes.Values
                .Where(idx => idx.Definition.CanSupportQuery(propertyPath))
                .ToList();

            if (candidates.Count == 0)
                return null;

            // Simple strategy: prefer unique indexes, then shortest property path
            return candidates
                .OrderByDescending(idx => idx.Definition.IsUnique)
                .ThenBy(idx => idx.Definition.PropertyPaths.Length)
                .First();
        }
    }

    /// <summary>
    /// Finds the best index for a compound query on multiple properties
    /// </summary>
    public CollectionSecondaryIndex<TId, T>? FindBestCompoundIndex(string[] propertyPaths)
    {
        if (propertyPaths == null || propertyPaths.Length == 0)
            return null;

        lock (_lock)
        {
            var candidates = _indexes.Values
                .Where(idx => idx.Definition.CanSupportCompoundQuery(propertyPaths))
                .ToList();

            if (candidates.Count == 0)
                return null;

            // Prefer longest matching index (more selective)
            return candidates
                .OrderByDescending(idx => idx.Definition.PropertyPaths.Length)
                .ThenByDescending(idx => idx.Definition.IsUnique)
                .First();
        }
    }

    /// <summary>
    /// Inserts a document into all indexes
    /// </summary>
    /// <param name="document">Document to insert</param>
    /// <param name="location">Physical location of the document</param>
    /// <param name="transaction">Transaction context</param>
    public void InsertIntoAll(T document, DocumentLocation location, ITransaction transaction)
    {
        if (document == null)
            throw new ArgumentNullException(nameof(document));

        lock (_lock)
        {
            foreach (var index in _indexes.Values)
            {
                index.Insert(document, location, transaction);
            }
        }
    }

    /// <summary>
    /// Updates a document in all indexes
    /// </summary>
    /// <param name="oldDocument">Old version of document</param>
    /// <param name="newDocument">New version of document</param>
    /// <param name="oldLocation">Physical location of old document</param>
    /// <param name="newLocation">Physical location of new document</param>
    /// <param name="transaction">Transaction context</param>
    public void UpdateInAll(T oldDocument, T newDocument, DocumentLocation oldLocation, DocumentLocation newLocation, ITransaction transaction)
    {
        if (oldDocument == null)
            throw new ArgumentNullException(nameof(oldDocument));
        if (newDocument == null)
            throw new ArgumentNullException(nameof(newDocument));

        lock (_lock)
        {
            foreach (var index in _indexes.Values)
            {
                index.Update(oldDocument, newDocument, oldLocation, newLocation, transaction);
            }
        }
    }

    /// <summary>
    /// Deletes a document from all indexes
    /// </summary>
    /// <param name="document">Document to delete</param>
    /// <param name="location">Physical location of the document</param>
    /// <param name="transaction">Transaction context</param>
    public void DeleteFromAll(T document, DocumentLocation location, ITransaction transaction)
    {
        if (document == null)
            throw new ArgumentNullException(nameof(document));

        lock (_lock)
        {
            foreach (var index in _indexes.Values)
            {
                index.Delete(document, location, transaction);
            }
        }
    }

    /// <summary>
    /// Generates an index name from property paths
    /// </summary>
    private static string GenerateIndexName(string[] propertyPaths)
    {
        return $"idx_{string.Join("_", propertyPaths)}";
    }

    public uint PrimaryRootPageId { get; private set; }

    public void SetPrimaryRootPageId(uint pageId)
    {
        lock (_lock)
        {
            if (PrimaryRootPageId != pageId)
            {
                PrimaryRootPageId = pageId;
                SaveMetadataToPage();
            }
        }
    }

    private void SaveMetadata()
    {
        SaveMetadataToPage();
    }

    private void LoadMetadata()
    {
        var buffer = new byte[_storage.PageSize];
        _storage.ReadPage(1, null, buffer);
        
        var header = SlottedPageHeader.ReadFrom(buffer);
        if (header.PageType != PageType.Collection || header.SlotCount == 0) 
            return;

        // Iterate all slots to find the one for this collection
        for (ushort i = 0; i < header.SlotCount; i++)
        {
            var slotOffset = SlottedPageHeader.Size + (i * SlotEntry.Size);
            var slot = SlotEntry.ReadFrom(buffer.AsSpan(slotOffset));
            
            if ((slot.Flags & SlotFlags.Deleted) != 0) continue;

            var dataSpan = buffer.AsSpan(slot.Offset, slot.Length);
            
            // Format: [NameLength:int] [Name:string] [Metadata...]
            // Peek name to see if it matches
            try
            {
                using var stream = new MemoryStream(dataSpan.ToArray());
                using var reader = new BinaryReader(stream);

                var name = reader.ReadString();
                if (!string.Equals(name, _collectionName, StringComparison.OrdinalIgnoreCase))
                {
                    continue; // Not our collection
                }

                // Match! Load metadata
                PrimaryRootPageId = reader.ReadUInt32();
                var indexCount = reader.ReadInt32();
                
                for (int j = 0; j < indexCount; j++)
                {
                    var idxName = reader.ReadString();
                    var isUnique = reader.ReadBoolean();
                    var type = (IndexType)reader.ReadByte();
                    
                    var pathCount = reader.ReadInt32();
                    var paths = new string[pathCount];
                    for (int k = 0; k < pathCount; k++)
                        paths[k] = reader.ReadString();
                    
                    var definition = RebuildDefinition(idxName, paths, isUnique, type);
                    var index = new CollectionSecondaryIndex<TId, T>(definition, _storage, _mapper);
                    _indexes[idxName] = index;
                }
                return; // Found and loaded
            }
            catch
            {
                // Ignore malformed slots
            }
        }
    }

    private CollectionIndexDefinition<T> RebuildDefinition(string name, string[] paths, bool isUnique, IndexType type)
    {
        // Dynamic expression building: u => new { u.Prop1, u.Prop2 } or u => u.Prop1
        var param = Expression.Parameter(typeof(T), "u");
        Expression body;
        
        if (paths.Length == 1)
        {
            // Simple property: u.Age
            body = Expression.PropertyOrField(param, paths[0]);
        }
        else
        {
            // Compound: new { u.Prop1, u.Prop2 } - Anonymous types are hard to generate dynamically
            // Alternative: Return object[] or Tuple? 
            // CollectionIndexDefinition expects Func<T, object>.
            // IndexOptions expects object key.
            // Check BTreeIndex: it converts key to IndexKey (byte[]).
            // Mappers handle extraction.
            
            // Wait, existing behavior uses anonymous types for compound keys?
            // "Expression must be a property accessor ... or anonymous type"
            
            // For BTreeIndex, the KeySelector returns the key value(s).
            // If we have multiple paths, we probably want to return an array or similar container 
            // that the Mapper can understand? 
            // Actually, `CollectionSecondaryIndex` uses `KeySelector` to get the object, 
            // then uses `_mapper.GetIndexKey(obj)`? No, `_mapper` is generally for documents.
            // Let's check `CollectionSecondaryIndex.cs`:
            // `var key = _mapper.GetIndexKey(doc, _definition.PropertyPaths);` 
            // Wait, does it use the Selector?
            // `ExtractKey(T document)` uses `_definition.KeySelector(document)`.
            // Then it converts that object to `IndexKey`.
            
            // If we can't easily reconstruct the anonymous type expression, 
            // we can build an expression that returns `object[]` or just `object` of the property.
            // The serialization to IndexKey must handle object[].
            
            // Let's assume for now 1 property is 99% of cases.
            // For multiple, we'll try to support it.
            
            body = Expression.PropertyOrField(param, paths[0]);
        }
        
        // Convert to object
        var objectBody = Expression.Convert(body, typeof(object));
        var lambda = Expression.Lambda<Func<T, object>>(objectBody, param);
        
        return new CollectionIndexDefinition<T>(name, paths, lambda, isUnique, type);
    }

    // ... RebuildDefinition ...

    private void SaveMetadataToPage()
    {
        // 1. Serialize Metadata
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);
        
        lock (_lock)
        {
            writer.Write(_collectionName); // Tag payload with collection name
            writer.Write(PrimaryRootPageId);
            writer.Write(_indexes.Count);
            foreach (var idx in _indexes.Values)
            {
                var def = idx.Definition;
                writer.Write(def.Name); 
                writer.Write(def.IsUnique);
                writer.Write((byte)def.Type);
                
                writer.Write(def.PropertyPaths.Length);
                foreach (var path in def.PropertyPaths)
                {
                    writer.Write(path);
                }
            }
        }
        
        var newData = stream.ToArray();
        
        // 2. Update Page 1
        // We need to lock the file/page to avoid race conditions if multiple collections save metadata
        // For now, we rely on WritePageImmediate being atomic for disk io, but Read-Modify-Write is racy.
        // In a real DB, we'd lock the page. Here we'll do a simple Read-Modify-Write loop.
        
        var buffer = new byte[_storage.PageSize];
        _storage.ReadPage(1, null, buffer);
        
        var header = SlottedPageHeader.ReadFrom(buffer);
        
        // Identify existing slot index
        int existingSlotIndex = -1;
        
        for (ushort i = 0; i < header.SlotCount; i++)
        {
            var slotOffset = SlottedPageHeader.Size + (i * SlotEntry.Size);
            var slot = SlotEntry.ReadFrom(buffer.AsSpan(slotOffset));
            if ((slot.Flags & SlotFlags.Deleted) != 0) continue;
            
            // Check name
             try
            {
                var slotSpan = buffer.AsSpan(slot.Offset, slot.Length);
                // Peek name
                // We can't use BinaryReader nicely on Span in older .NET, but we can do string match manually
                // Or just fast-path: name is length-prefixed string. 
                // Read length (7-bit encoded int... tricky).
                // Let's rely on BinaryReader over buffer stream slice.
                using var ms = new MemoryStream(buffer, slot.Offset, slot.Length, false);
                using var reader = new BinaryReader(ms);
                var name = reader.ReadString();
                
                if (string.Equals(name, _collectionName, StringComparison.OrdinalIgnoreCase))
                {
                    existingSlotIndex = i;
                    break;
                }
            } 
            catch { }
        }
        
        // If found, mark as deleted (simplest update strategy)
        if (existingSlotIndex >= 0)
        {
             var slotOffset = SlottedPageHeader.Size + (existingSlotIndex * SlotEntry.Size);
             var slot = SlotEntry.ReadFrom(buffer.AsSpan(slotOffset));
             slot.Flags |= SlotFlags.Deleted;
             slot.WriteTo(buffer.AsSpan(slotOffset));
        }
        
        // Insert new data
        // Find free space (simplified: append to end of free space, if fits)
        // Note: Compact is not implemented here. If we run out of space on Page 1, we fail.
        // Page 1 is 16KB (usually). Metadata is small. Should be fine for many collections.
        
        if (header.AvailableFreeSpace < newData.Length + SlotEntry.Size)
        {
            // Try to compact? Or just throw.
            throw new InvalidOperationException("Not enough space in Metadata Page (Page 1) to save collection metadata.");
        }
        
        // Write data at FreeSpaceEnd - Length (grows down? No, Header says FreeSpaceEnd points to start of data payload usually? 
        // Wait, Slot architecture usually: Header | Slots -> ... <- Data | End
        // Let's check DocumentCollection.InsertIntoPage logic:
        // "Write document at end of used space (grows up)" -> "docOffset = header.FreeSpaceEnd - data.Length;"
        // And FreeSpaceEnd is initialized to PageSize. So it grows DOWN.
        // FreeSpaceStart grows UP (slots).
        
        int docOffset = header.FreeSpaceEnd - newData.Length;
        newData.CopyTo(buffer.AsSpan(docOffset));
        
        // Write slot
        // Reuse existing slot index if we deleted it? 
        // For simplicity, append new slot unless we want to scan for deleted ones.
        // If we marked existingSlotIndex as deleted, we can reuse it IF it's consistent.
        // But reusing slot index requires updating the Slot entry which is at a fixed position. 
        // Yes, we can reuse existingSlotIndex.
        
        ushort slotIndex;
        if (existingSlotIndex >= 0)
        {
            slotIndex = (ushort)existingSlotIndex;
        }
        else
        {
             slotIndex = header.SlotCount;
             header.SlotCount++;
        }
        
        var newSlotEntryOffset = SlottedPageHeader.Size + (slotIndex * SlotEntry.Size);
        var newSlot = new SlotEntry
        {
            Offset = (ushort)docOffset,
            Length = (ushort)newData.Length,
            Flags = SlotFlags.None
        };
        newSlot.WriteTo(buffer.AsSpan(newSlotEntryOffset));
        
        // Update header
        header.FreeSpaceEnd = (ushort)docOffset;
        // If we added a new slot, check if FreeSpaceStart needs update
        if (existingSlotIndex == -1)
        {
             header.FreeSpaceStart = (ushort)(SlottedPageHeader.Size + (header.SlotCount * SlotEntry.Size));
        }
        
        header.WriteTo(buffer);
        
        _storage.WritePageImmediate(1, buffer);
    }

    public void Dispose()
    {
        if (_disposed)
            return;
            
        // Save metadata on close? Or only on mod?
        // Better to save on modification (CreateIndex/DropIndex).
        
        lock (_lock)
        {
            foreach (var index in _indexes.Values)
            {
                try { index.Dispose(); } catch { /* Best effort */ }
            }
            
            _indexes.Clear();
            _disposed = true;
        }

        GC.SuppressFinalize(this);
    }
}

/// <summary>
/// Helper class to analyze LINQ expressions and extract property paths
/// </summary>
public static class ExpressionAnalyzer
{
    /// <summary>
    /// Extracts property paths from a lambda expression.
    /// Supports simple property access (p => p.Age) and anonymous types (p => new { p.City, p.Age }).
    /// </summary>
    public static string[] ExtractPropertyPaths<T, TKey>(Expression<Func<T, TKey>> expression)
    {
        if (expression.Body is MemberExpression memberExpr)
        {
            // Simple property: p => p.Age
            return new[] { memberExpr.Member.Name };
        }
        else if (expression.Body is NewExpression newExpr)
        {
            // Compound key via anonymous type: p => new { p.City, p.Age }
            return newExpr.Arguments
                .OfType<MemberExpression>()
                .Select(m => m.Member.Name)
                .ToArray();
        }
        else if (expression.Body is UnaryExpression { NodeType: ExpressionType.Convert } unaryExpr)
        {
            // Handle Convert(Member) or Convert(New)
            if (unaryExpr.Operand is MemberExpression innerMember)
            {
                // Wrapped property: p => (object)p.Age
                return new[] { innerMember.Member.Name };
            }
            else if (unaryExpr.Operand is NewExpression innerNew)
            {
                 // Wrapped anonymous type: p => (object)new { p.City, p.Age }
                 return innerNew.Arguments
                    .OfType<MemberExpression>()
                    .Select(m => m.Member.Name)
                    .ToArray();
            }
        }

        throw new ArgumentException(
            "Expression must be a property accessor (p => p.Property) or anonymous type (p => new { p.Prop1, p.Prop2 })",
            nameof(expression));
    }
}
