using System;
using BLite.Bson;
using BLite.Core.Transactions;

namespace BLite.Core.CDC;

/// <summary>
/// A generic, immutable struct representing a data change in a collection.
/// </summary>
public readonly struct ChangeStreamEvent<TId, T> where T : class
{
    public long Timestamp { get; init; }
    public ulong TransactionId { get; init; }
    public string CollectionName { get; init; }
    public OperationType Type { get; init; }
    public TId DocumentId { get; init; }
    
    /// <summary>
    /// The deserialized entity. Null if capturePayload was false during Watch().
    /// </summary>
    public T? Entity { get; init; }
}

/// <summary>
/// Low-level event structure used internally to transport changes before deserialization.
/// </summary>
internal readonly struct InternalChangeEvent
{
    public long Timestamp { get; init; }
    public ulong TransactionId { get; init; }
    public string CollectionName { get; init; }
    public OperationType Type { get; init; }

    /// <summary>Original BsonId type — required to reconstruct the ID from <see cref="IdBytes"/>.</summary>
    public BsonIdType IdType { get; init; }

    /// <summary>
    /// Raw bytes of the Document ID, serialized via <see cref="BsonId.ToBytes()"/>.
    /// </summary>
    public ReadOnlyMemory<byte> IdBytes { get; init; }

    /// <summary>
    /// Raw BSON of the Entity. Null if payload not captured.
    /// </summary>
    public ReadOnlyMemory<byte>? PayloadBytes { get; init; }
}
