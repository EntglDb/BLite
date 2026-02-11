using BLite.Bson;
using BLite.Core.Indexing;
using System;
using System.Buffers;

namespace BLite.Core.Collections;

/// <summary>
/// Non-generic interface for common mapper operations.
/// </summary>
public interface IDocumentMapper
{
    string CollectionName { get; }
    IEnumerable<string> UsedKeys { get; }
    BsonSchema GetSchema();
}

/// <summary>
/// Interface for mapping between entities and BSON using zero-allocation serialization.
/// Handles bidirectional mapping between TId and IndexKey.
/// </summary>
public interface IDocumentMapper<TId, T> : IDocumentMapper where T : class
{
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
