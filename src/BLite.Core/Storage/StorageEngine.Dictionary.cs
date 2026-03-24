using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
#if NET8_0_OR_GREATER
using System.Collections.Frozen;
#endif

namespace BLite.Core.Storage;

public sealed partial class StorageEngine
{
    private readonly ConcurrentDictionary<string, ushort> _dictionaryCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<ushort, string> _dictionaryReverseCache = new();
    private uint _dictionaryRootPageId;
    private ushort _nextDictionaryId;
    
    // Lock for dictionary modifications (simple lock for now, could be RW lock)
    private readonly object _dictionaryLock = new();

    // Lazily built read-only snapshot for hot-path lookups during serialization.
    // On .NET 8+ this is a FrozenDictionary (~3x faster TryGetValue than ConcurrentDictionary).
    // On netstandard2.1 fallback to a plain Dictionary copy (still faster than ConcurrentDictionary).
    // Invalidated (set to null) whenever a new key is added; rebuilt on next access.
    private volatile IReadOnlyDictionary<string, ushort>? _frozenKeyMapCache;

    private void InitializeDictionary()
    {
        // 1. Read File Header (Page 0) to get Dictionary Root
        var headerBuffer = new byte[PageSize];
        ReadPage(0, null, headerBuffer);
        var header = PageHeader.ReadFrom(headerBuffer);
        
        if (header.DictionaryRootPageId == 0)
        {
            // Initialize new Dictionary
            lock (_dictionaryLock)
            {
                 // Double check
                 ReadPage(0, null, headerBuffer);
                 header = PageHeader.ReadFrom(headerBuffer);
                 if (header.DictionaryRootPageId == 0)
                 {
                     _dictionaryRootPageId = AllocatePage();
                     
                     // Init Dictionary Page
                     var pageBuffer = new byte[PageSize];
                     DictionaryPage.Initialize(pageBuffer, _dictionaryRootPageId);
                     WritePageImmediate(_dictionaryRootPageId, pageBuffer);
                     
                     // Update Header
                     header.DictionaryRootPageId = _dictionaryRootPageId;
                     header.WriteTo(headerBuffer);
                     WritePageImmediate(0, headerBuffer);
                     
                     // Init Next ID
                     _nextDictionaryId = DictionaryPage.ReservedValuesEnd + 1;
                 }
                 else
                 {
                     _dictionaryRootPageId = header.DictionaryRootPageId;
                 }
            }
        }
        else
        {
            _dictionaryRootPageId = header.DictionaryRootPageId;
            
            // Warm cache
            ushort maxId = DictionaryPage.ReservedValuesEnd;
            foreach (var (key, val) in DictionaryPage.FindAllGlobal(this, _dictionaryRootPageId))
            {
                var lowerKey = key.ToLowerInvariant();
                _dictionaryCache[lowerKey] = val;
                _dictionaryReverseCache[val] = lowerKey;
                if (val > maxId) maxId = val;
            }
            _nextDictionaryId = (ushort)(maxId + 1);
        }

        // Pre-register internal keys used for Schema persistence
        RegisterKeys(new[] { "_id", "t", "_v", "f", "n", "b", "s", "a" });
    }

    public ConcurrentDictionary<string, ushort> GetKeyMap() => _dictionaryCache;
    public ConcurrentDictionary<ushort, string> GetKeyReverseMap() => _dictionaryReverseCache;

    /// <summary>
    /// Returns a read-only snapshot of the key map optimised for concurrent lookups.
    /// On .NET 8+ this is a <see cref="FrozenDictionary{TKey,TValue}"/> (~3x faster TryGetValue).
    /// On netstandard2.1 falls back to a plain <see cref="Dictionary{TKey,TValue}"/> copy.
    /// The snapshot is rebuilt lazily whenever a new key is registered; callers must invoke
    /// <see cref="RegisterKeys"/> before requesting the snapshot so all required keys are present.
    /// </summary>
    public IReadOnlyDictionary<string, ushort> GetFrozenKeyMap()
        => _frozenKeyMapCache ?? RebuildFrozenKeyMap();

    private IReadOnlyDictionary<string, ushort> RebuildFrozenKeyMap()
    {
        lock (_dictionaryLock)
        {
            if (_frozenKeyMapCache is null)
            {
#if NET8_0_OR_GREATER
                _frozenKeyMapCache = _dictionaryCache.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);
#else
                _frozenKeyMapCache = new Dictionary<string, ushort>(_dictionaryCache, StringComparer.OrdinalIgnoreCase);
#endif
            }
            return _frozenKeyMapCache;
        }
    }

    /// <summary>
    /// Imports key→ID entries from <paramref name="sourceReverseMap"/> into this engine,
    /// preserving the original <see cref="ushort"/> IDs. Skips entries that are already
    /// registered (by name or by ID). Updates <see cref="_nextDictionaryId"/> so that
    /// future sequential allocations do not collide with any imported ID.
    /// <para>
    /// Used during cross-layout migration so that raw BSON pages copied from the source
    /// remain decodable by the target engine without re-serialisation.
    /// </para>
    /// </summary>
    internal void ImportDictionary(IReadOnlyDictionary<ushort, string> sourceReverseMap)
    {
        lock (_dictionaryLock)
        {
            foreach (var (id, name) in sourceReverseMap)
            {
                var lower = name.ToLowerInvariant();
                // Skip if name already registered (reserved/internal keys) or ID already taken.
                if (_dictionaryCache.ContainsKey(lower) || _dictionaryReverseCache.ContainsKey(id))
                    continue;

                if (!InsertDictionaryEntryGlobal(lower, id))
                    continue; // Best-effort: skip if page is unexpectedly full.

                _dictionaryCache[lower] = id;
                _dictionaryReverseCache[id] = lower;

                // Keep _nextDictionaryId beyond the highest imported ID.
                if (id >= _nextDictionaryId)
                    _nextDictionaryId = (ushort)(id + 1);
            }
            _frozenKeyMapCache = null; // invalidate snapshot after bulk import
        }
    }

    /// <summary>
    /// Gets the ID for a dictionary key, creating it if it doesn't exist.
    /// Thread-safe.
    /// </summary>
    public ushort GetOrAddDictionaryEntry(string key)
    {
        key = key.ToLowerInvariant();
        if (_dictionaryCache.TryGetValue(key, out var id))
        {
            return id;
        }

        lock (_dictionaryLock)
        {
            // Double checked locking
            if (_dictionaryCache.TryGetValue(key, out id))
            {
                return id;
            }

            // Try to find in storage (in case cache is incomplete or another process?)
            // Note: FindAllGlobal loaded everything, so strict cache miss means it's not in DB.
            // BUT if we support concurrent writers (multiple processed), we should re-check DB.
            // Current BLite seems to be single-process exclusive lock (FileShare.None).
            // So in-memory cache is authoritative after load.
            
            // Generate New ID
            ushort nextId = _nextDictionaryId;
            if (nextId == 0) nextId = DictionaryPage.ReservedValuesEnd + 1; // Should be init, but safety

            // Insert into Page
            // usage of default(ulong) or null transaction? 
            // Dictionary updates should ideally be transactional or immediate?
            // "Immediate" for now to simplify, as dictionary is cross-collection.
            // If we use transaction, we need to pass it in. For now, immediate write.
            
            // We need to support "Insert Global" which handles overflow.
            // DictionaryPage.Insert only handles single page.
            
            // We need logic here to traverse chain and find space.
            if (InsertDictionaryEntryGlobal(key, nextId))
            {
                _dictionaryCache[key] = nextId;
                _dictionaryReverseCache[nextId] = key;
                _nextDictionaryId++;
                _frozenKeyMapCache = null; // invalidate snapshot
                return nextId;
            }
            else
            {
                throw new InvalidOperationException("Failed to insert dictionary entry (Storage Full?)");
            }
        }
    }

    public string? GetDictionaryKey(ushort id)
    {
        if (_dictionaryReverseCache.TryGetValue(id, out var key))
            return key;
        return null;
    }
    
    private bool InsertDictionaryEntryGlobal(string key, ushort value)
    {
        var pageId = _dictionaryRootPageId;
        var pageBuffer = new byte[PageSize];
        
        while (true)
        {
            ReadPage(pageId, null, pageBuffer);
            
            // Try Insert
            if (DictionaryPage.Insert(pageBuffer, key, value))
            {
                // Success - Write Back
                WritePageImmediate(pageId, pageBuffer);
                return true;
            }
            
            // Page Full - Check Next Page
            var header = PageHeader.ReadFrom(pageBuffer);
            if (header.NextPageId != 0)
            {
                pageId = header.NextPageId;
                continue;
            }
            
            // No Next Page - Allocate New
            var newPageId = AllocatePage();
            var newPageBuffer = new byte[PageSize];
            DictionaryPage.Initialize(newPageBuffer, newPageId);
            
            // Should likely insert into NEW page immediately to save I/O?
            // Or just link and loop?
            // Let's Insert into new page logic here to avoid re-reading.
            if (!DictionaryPage.Insert(newPageBuffer, key, value))
                return false; // Should not happen on empty page unless key is huge > page
                
            // Write New Page
            WritePageImmediate(newPageId, newPageBuffer);
            
            // Update Previous Page Link
            header.NextPageId = newPageId;
            header.WriteTo(pageBuffer);
            WritePageImmediate(pageId, pageBuffer);
            
            return true;
        }
    }

    /// <summary>
    /// Registers a set of keys in the global dictionary.
    /// Ensures all keys are assigned an ID and persisted.
    /// </summary>
    public void RegisterKeys(IEnumerable<string> keys)
    {
        foreach (var key in keys)
        {
            GetOrAddDictionaryEntry(key.ToLowerInvariant());
        }
    }
}
