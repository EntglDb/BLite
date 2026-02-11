using BLite.Bson;
using BLite.Core.Storage;

namespace BLite.Core.Indexing;

/// <summary>
/// Hash-based index for exact-match lookups.
/// Uses simple bucket-based hashing with collision handling.
/// </summary>
public sealed class HashIndex
{
    private readonly Dictionary<int, List<IndexEntry>> _buckets;
    private readonly IndexOptions _options;

    public HashIndex(IndexOptions options)
    {
        _options = options;
        _buckets = new Dictionary<int, List<IndexEntry>>();
    }

    /// <summary>
    /// Inserts a key-location pair into the hash index
    /// </summary>
    public void Insert(IndexKey key, DocumentLocation location)
    {
        if (_options.Unique && TryFind(key, out _))
            throw new InvalidOperationException($"Duplicate key violation for unique index");

        var hashCode = key.GetHashCode();
        
        if (!_buckets.TryGetValue(hashCode, out var bucket))
        {
            bucket = new List<IndexEntry>();
            _buckets[hashCode] = bucket;
        }

        bucket.Add(new IndexEntry(key, location));
    }

    /// <summary>
    /// Finds a document location by exact key match
    /// </summary>
    public bool TryFind(IndexKey key, out DocumentLocation location)
    {
        location = default;
        var hashCode = key.GetHashCode();

        if (!_buckets.TryGetValue(hashCode, out var bucket))
            return false;

        foreach (var entry in bucket)
        {
            if (entry.Key == key)
            {
                location = entry.Location;
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Removes an entry from the index
    /// </summary>
    public bool Remove(IndexKey key, DocumentLocation location)
    {
        var hashCode = key.GetHashCode();

        if (!_buckets.TryGetValue(hashCode, out var bucket))
            return false;

        for (int i = 0; i < bucket.Count; i++)
        {
            if (bucket[i].Key == key && 
                bucket[i].Location.PageId == location.PageId &&
                bucket[i].Location.SlotIndex == location.SlotIndex)
            {
                bucket.RemoveAt(i);
                
                if (bucket.Count == 0)
                    _buckets.Remove(hashCode);
                
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Gets all entries matching the key
    /// </summary>
    public IEnumerable<IndexEntry> FindAll(IndexKey key)
    {
        var hashCode = key.GetHashCode();

        if (!_buckets.TryGetValue(hashCode, out var bucket))
            yield break;

        foreach (var entry in bucket)
        {
            if (entry.Key == key)
                yield return entry;
        }
    }
}
