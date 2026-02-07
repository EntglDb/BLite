using System;
using System.Buffers;
using DocumentDb.Bson;

namespace DocumentDb.Core.Collections;

/// <summary>
/// Interface for mapping between entities and BSON using zero-allocation serialization.
/// Supports both Span-based (fixed buffer) and IBufferWriter-based (streaming) serialization.
/// </summary>
/// <typeparam name="T">Entity type</typeparam>
public interface IDocumentMapper<T> where T : class
{
    /// <summary>
    /// Collection name in database (e.g., "users", "products")
    /// </summary>
    string CollectionName { get; }
    
    /// <summary>
    /// Serialize entity to BSON format into fixed-size buffer.
    /// Use for small documents or when buffer size is known.
    /// </summary>
    int Serialize(T entity, Span<byte> buffer);
    
    /// <summary>
    /// Serialize entity to BSON using buffer writer (streaming, unlimited size).
    /// Preferred for large documents or when size is unknown.
    /// </summary>
    void Serialize(T entity, IBufferWriter<byte> writer);
    
    /// <summary>
    /// Deserialize entity from BSON format
    /// </summary>
    T Deserialize(ReadOnlySpan<byte> buffer);
    
    /// <summary>
    /// Get ObjectId from entity
    /// </summary>
    ObjectId GetId(T entity);
    
    /// <summary>
    /// Set ObjectId in entity
    /// </summary>
    void SetId(T entity, ObjectId id);
}
