using BLite.Core.Indexing;
using System;

namespace BLite.Core.Indexing;

/// <summary>
/// Represents a cursor for traversing a B+Tree index.
/// Provides low-level primitives for building complex queries.
/// </summary>
public interface IBTreeCursor : IDisposable
{
    /// <summary>
    /// Gets the current entry at the cursor position.
    /// Throws InvalidOperationException if cursor is invalid or uninitialized.
    /// </summary>
    IndexEntry Current { get; }

    /// <summary>
    /// Moves the cursor to the first entry in the index.
    /// </summary>
    /// <returns>True if the index is not empty; otherwise, false.</returns>
    bool MoveToFirst();

    /// <summary>
    /// Moves the cursor to the last entry in the index.
    /// </summary>
    /// <returns>True if the index is not empty; otherwise, false.</returns>
    bool MoveToLast();

    /// <summary>
    /// Seeks to the specified key.
    /// If exact match found, positions there and returns true.
    /// If not found, positions at the next greater key and returns false.
    /// </summary>
    /// <param name="key">Key to seek</param>
    /// <returns>True if exact match found; false if positioned at next greater key.</returns>
    bool Seek(IndexKey key);

    /// <summary>
    /// Advances the cursor to the next entry.
    /// </summary>
    /// <returns>True if successfully moved; false if end of index reached.</returns>
    bool MoveNext();

    /// <summary>
    /// Moves the cursor to the previous entry.
    /// </summary>
    /// <returns>True if successfully moved; false if start of index reached.</returns>
    bool MovePrev();
}
