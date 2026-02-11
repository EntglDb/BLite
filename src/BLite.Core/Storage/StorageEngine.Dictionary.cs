using System.Collections.Concurrent;
using System.Text;

namespace BLite.Core.Storage;

public sealed partial class StorageEngine
{
    private readonly ConcurrentDictionary<string, ushort> _dictionaryCache = new();
    private readonly ConcurrentDictionary<ushort, string> _dictionaryReverseCache = new();
    private uint _dictionaryRootPageId;
    private ushort _nextDictionaryId;
    
    // Lock for dictionary modifications (simple lock for now, could be RW lock)
    private readonly object _dictionaryLock = new();

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
                _dictionaryCache[key] = val;
                _dictionaryReverseCache[val] = key;
                if (val > maxId) maxId = val;
            }
            _nextDictionaryId = (ushort)(maxId + 1);
        }
    }

    /// <summary>
    /// Gets the ID for a dictionary key, creating it if it doesn't exist.
    /// Thread-safe.
    /// </summary>
    public ushort GetOrAddDictionaryEntry(string key)
    {
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
}
