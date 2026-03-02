// BLite.Bson — Custom delegate types for BsonSpanReader
// BsonSpanReader is a ref struct; on targets older than .NET 9 / C# 13,
// ref structs cannot be used as generic type arguments (e.g. Func<BsonSpanReader, T>).
// These non-generic delegates solve that constraint without sacrificing type safety.
// Callers can still assign lambda expressions directly: reader => ...

namespace BLite.Bson;

/// <summary>
/// A predicate that evaluates a raw BSON document without deserializing it.
/// </summary>
public delegate bool BsonReaderPredicate(BsonSpanReader reader);

/// <summary>
/// A projector that reads fields directly from a raw BSON document and returns a result,
/// or <c>null</c> to skip the document.
/// </summary>
/// <typeparam name="TResult">The projected result type.</typeparam>
public delegate TResult? BsonReaderProjector<TResult>(BsonSpanReader reader);
