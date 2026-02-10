using System;
using System.Buffers;
using DocumentDb.Bson;
using DocumentDb.Core.Indexing;

namespace DocumentDb.Core.Collections;

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
}

/// <summary>
/// Legacy interface for compatibility with existing ObjectId-based collections.
/// </summary>
public interface IDocumentMapper<T> : IDocumentMapper<ObjectId, T> where T : class
{
}
