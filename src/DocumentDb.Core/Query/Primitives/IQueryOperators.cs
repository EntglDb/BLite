namespace DocumentDb.Core.Query.Primitives;

/// <summary>
/// Base interface for query operators that produce sequences of documents.
/// Designed for high-performance, streaming execution.
/// </summary>
/// <typeparam name="T">Document type</typeparam>
public interface IQueryOperator<out T>
{
    /// <summary>
    /// Executes the operator and returns an enumerable sequence of results.
    /// Should be implemented as a streaming operation when possible.
    /// </summary>
    IEnumerable<T> Execute();
    
    /// <summary>
    /// Estimates the number of results this operator will produce.
    /// Used by the query optimizer for cost-based decisions.
    /// </summary>
    /// <returns>Estimated cardinality, or -1 if unknown</returns>
    long EstimateCardinality();
}

/// <summary>
/// Operator for full table/collection scan.
/// Fallback when no index is available.
/// </summary>
/// <typeparam name="T">Document type</typeparam>
public interface IScanOperator<out T> : IQueryOperator<T>
{
    /// <summary>
    /// Scans all documents in the collection, optionally applying a filter predicate.
    /// </summary>
    /// <param name="predicate">Optional predicate to filter documents during scan</param>
    IEnumerable<T> Scan(Func<T, bool>? predicate = null);
}

/// <summary>
/// Operator for index-based seek operations (point lookup).
/// O(log n) complexity using BTree index.
/// </summary>
/// <typeparam name="T">Document type</typeparam>
public interface IIndexSeekOperator<out T> : IQueryOperator<T>
{
    /// <summary>
    /// Seeks a specific document by index key.
    /// </summary>
    /// <param name="key">The key to seek</param>
    /// <returns>The matching document, or default if not found</returns>
    T? Seek(object key);
}

/// <summary>
/// Operator for index-based range scan operations.
/// O(log n + k) complexity where k is the number of results.
/// </summary>
/// <typeparam name="T">Document type</typeparam>
public interface IIndexScanOperator<out T> : IQueryOperator<T>
{
    /// <summary>
    /// Scans documents within an index range.
    /// </summary>
    /// <param name="minKey">Minimum key (inclusive), or null for unbounded</param>
    /// <param name="maxKey">Maximum key (inclusive), or null for unbounded</param>
    /// <param name="ascending">True for ascending order, false for descending</param>
    IEnumerable<T> Range(object? minKey, object? maxKey, bool ascending = true);
    
    /// <summary>
    /// Scans documents matching a key prefix (for string keys).
    /// </summary>
    /// <param name="prefix">The prefix to match</param>
    IEnumerable<T> Prefix(string prefix);
}

/// <summary>
/// Operator for filtering a sequence with a predicate.
/// Push-down optimization when possible.
/// </summary>
/// <typeparam name="T">Document type</typeparam>
public interface IFilterOperator<T> : IQueryOperator<T>
{
    /// <summary>
    /// Filters the source sequence using the specified predicate.
    /// </summary>
    /// <param name="source">Source sequence</param>
    /// <param name="predicate">Filter predicate</param>
    IEnumerable<T> Filter(IEnumerable<T> source, Func<T, bool> predicate);
}

/// <summary>
/// Operator for projecting/transforming documents.
/// Zero-allocation when projection is identity.
/// </summary>
/// <typeparam name="TSource">Source document type</typeparam>
/// <typeparam name="TResult">Result type</typeparam>
public interface IProjectOperator<TSource, TResult> : IQueryOperator<TResult>
{
    /// <summary>
    /// Projects each element of the source sequence using the specified selector.
    /// </summary>
    /// <param name="source">Source sequence</param>
    /// <param name="selector">Projection function</param>
    IEnumerable<TResult> Project(IEnumerable<TSource> source, Func<TSource, TResult> selector);
}

/// <summary>
/// Operator for sorting a sequence.
/// Uses index when available to avoid in-memory sort.
/// </summary>
/// <typeparam name="T">Document type</typeparam>
public interface ISortOperator<T> : IQueryOperator<T>
{
    /// <summary>
    /// Sorts the source sequence using the specified key selector.
    /// </summary>
    /// <param name="source">Source sequence</param>
    /// <param name="keySelector">Function to extract sort key</param>
    /// <param name="descending">True for descending order</param>
    IEnumerable<T> Sort<TKey>(IEnumerable<T> source, Func<T, TKey> keySelector, bool descending = false);
}

/// <summary>
/// Operator for pagination (Skip/Take).
/// Optimized for index scans.
/// </summary>
/// <typeparam name="T">Document type</typeparam>
public interface IPaginationOperator<T> : IQueryOperator<T>
{
    /// <summary>
    /// Skips a specified number of elements.
    /// </summary>
    IEnumerable<T> Skip(IEnumerable<T> source, int count);
    
    /// <summary>
    /// Takes a specified number of elements.
    /// </summary>
    IEnumerable<T> Take(IEnumerable<T> source, int count);
}
