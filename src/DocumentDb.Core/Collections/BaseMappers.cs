using System;
using System.Buffers;
using DocumentDb.Bson;
using DocumentDb.Core.Indexing;

namespace DocumentDb.Core.Collections;

/// <summary>
/// Base class for custom mappers that provides bidirectional IndexKey mapping for standard types.
/// </summary>
public abstract class DocumentMapperBase<TId, T> : IDocumentMapper<TId, T> where T : class
{
    public abstract string CollectionName { get; }
    public abstract int Serialize(T entity, Span<byte> buffer);
    public abstract T Deserialize(ReadOnlySpan<byte> buffer);
    public abstract TId GetId(T entity);
    public abstract void SetId(T entity, TId id);

    public virtual IndexKey ToIndexKey(TId id) => IndexKey.Create(id);
    public virtual TId FromIndexKey(IndexKey key) => key.As<TId>();
}

/// <summary>
/// Base class for mappers using ObjectId as primary key.
/// </summary>
public abstract class ObjectIdMapperBase<T> : DocumentMapperBase<ObjectId, T>, IDocumentMapper<T> where T : class 
{
    public override IndexKey ToIndexKey(ObjectId id) => IndexKey.Create(id);
    public override ObjectId FromIndexKey(IndexKey key) => key.As<ObjectId>();
}

/// <summary>
/// Base class for mappers using Int32 as primary key.
/// </summary>
public abstract class Int32MapperBase<T> : DocumentMapperBase<int, T> where T : class
{
    public override IndexKey ToIndexKey(int id) => IndexKey.Create(id);
    public override int FromIndexKey(IndexKey key) => key.As<int>();
}

/// <summary>
/// Base class for mappers using String as primary key.
/// </summary>
public abstract class StringMapperBase<T> : DocumentMapperBase<string, T> where T : class
{
    public override IndexKey ToIndexKey(string id) => IndexKey.Create(id);
    public override string FromIndexKey(IndexKey key) => key.As<string>();
}

/// <summary>
/// Base class for mappers using Guid as primary key.
/// </summary>
public abstract class GuidMapperBase<T> : DocumentMapperBase<Guid, T> where T : class
{
    public override IndexKey ToIndexKey(Guid id) => IndexKey.Create(id);
    public override Guid FromIndexKey(IndexKey key) => key.As<Guid>();
}
