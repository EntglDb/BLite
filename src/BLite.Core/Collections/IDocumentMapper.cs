using BLite.Bson;
using BLite.Core.Indexing;
using System;
using System.Buffers;

namespace BLite.Core.Collections;

/// <summary>
/// Interface for mapping between entities and BSON using zero-allocation serialization.
/// Handles bidirectional mapping between TId and IndexKey.
/// </summary>
public interface IDocumentMapper<TId, T> where T : class
{
    string CollectionName { get; }
    int Serialize(T entity, Span<byte> buffer);
    T Deserialize(ReadOnlySpan<byte> buffer);
    
    TId GetId(T entity);
    void SetId(T entity, TId id);
    
    IndexKey ToIndexKey(TId id);
    TId FromIndexKey(IndexKey key);
    
    /// <summary>
    /// Gets a list of all BSON keys used by this mapper.
    /// </summary>
    IEnumerable<string> UsedKeys { get; }

    /// <summary>
    /// Returns a BSON schema describing the mapped entity.
    /// </summary>
    BsonSchema GetSchema();
}

/// <summary>
/// Legacy interface for compatibility with existing ObjectId-based collections.
/// </summary>
public interface IDocumentMapper<T> : IDocumentMapper<ObjectId, T> where T : class
{
}
