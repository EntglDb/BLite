using System;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using BLite.Bson;
using BLite.Core.Indexing;
using System.Linq;
using System.Collections.Generic;

namespace BLite.Core.Collections;

/// <summary>
/// Base class for custom mappers that provides bidirectional IndexKey mapping for standard types.
/// </summary>
public abstract class DocumentMapperBase<TId, T> : IDocumentMapper<TId, T> where T : class
{
    public abstract string CollectionName { get; }
    public abstract int Serialize(T entity, BsonSpanWriter writer);
    public abstract T Deserialize(BsonSpanReader reader);
    public abstract TId GetId(T entity);
    public abstract void SetId(T entity, TId id);

    public virtual IndexKey ToIndexKey(TId id) => IndexKey.Create(id);
    public virtual TId FromIndexKey(IndexKey key) => key.As<TId>();

    /// <summary>
    /// The BSON field <see cref="Serialize"/> stores <paramref name="propertyName"/> in, when
    /// <see cref="Deserialize"/> gives the property exactly the value of that field (a plain
    /// auto-property with no converter), so the value of the materialised property can be read from
    /// the raw document instead; <c>null</c> when that does not hold or is not known.
    /// </summary>
    /// <remarks>
    /// Generated mappers override this. The default returns <c>null</c>, so queries that could read a
    /// property from raw BSON materialise the entities instead.
    /// </remarks>
    public virtual string? GetVerbatimFieldName(string propertyName) => null;

    /// <summary>
    /// The BSON field names <see cref="Serialize"/> can write. The collection registers them in the
    /// key dictionary before its first write, and a write of an unregistered name fails.
    /// </summary>
    /// <remarks>
    /// Generated mappers override this with the list the source generator emits, so they use no
    /// reflection. This default reads the names from <typeparamref name="T"/> by reflection, which
    /// trimming breaks: under Native AOT it finds no names and every write fails (#273). A hand-written
    /// mapper used in a trimmed or Native AOT app must override it. The suppression below only quiets
    /// BLite's own build: the trim analysis of the app's publish still reports this getter when a
    /// mapper that does not override it is compiled in.
    /// </remarks>
#pragma warning disable IL2026
    public virtual IEnumerable<string> UsedKeys => GetSchema().GetAllKeys();
#pragma warning restore IL2026
    [RequiresUnreferencedCode("Schema generation uses reflection to discover properties and fields. Ensure all entity types and their members are preserved.")]
    public virtual BsonSchema GetSchema() => BsonSchemaGenerator.FromType<T>();
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
/// Base class for mappers using Int64 as primary key.
/// </summary>
public abstract class Int64MapperBase<T> : DocumentMapperBase<long, T> where T : class
{
    public override IndexKey ToIndexKey(long id) => IndexKey.Create(id);
    public override long FromIndexKey(IndexKey key) => key.As<long>();
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
