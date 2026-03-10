using System.Collections.Concurrent;

namespace BLite.Core.Storage;

public sealed partial class StorageEngine
{
    // ── Sequence state ────────────────────────────────────────────────────────

    /// <summary>Per-collection locks that serialise sequence increments.</summary>
    private readonly ConcurrentDictionary<string, object> _sequenceLocks =
        new(StringComparer.OrdinalIgnoreCase);

    // ── Public API ────────────────────────────────────────────────────────────

    /// <summary>
    /// Atomically increments and persists the auto-increment counter for
    /// <paramref name="collectionName"/> and returns the new value (1-based).
    /// Thread-safe: uses a per-collection lock so concurrent inserts in
    /// different collections do not contend.
    /// </summary>
    public long GetNextSequence(string collectionName)
    {
        var lockObj = _sequenceLocks.GetOrAdd(collectionName, _ => new object());

        lock (lockObj)
        {
            var meta = GetCollectionMetadata(collectionName)
                       ?? throw new InvalidOperationException(
                           $"Collection '{collectionName}' not found.");

            meta.SequenceValue++;
            SaveCollectionMetadata(meta);
            return meta.SequenceValue;
        }
    }
}
