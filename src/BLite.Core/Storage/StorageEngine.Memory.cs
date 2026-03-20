using BLite.Core.Transactions;

namespace BLite.Core.Storage;

public sealed partial class StorageEngine
{
    /// <summary>
    /// Allocates a new page in the main data file.
    /// </summary>
    /// <returns>Page ID of the allocated page</returns>
    public uint AllocatePage()
    {
        return _pageFile.AllocatePage();
    }

    /// <summary>
    /// Allocates a new index page.
    /// If an index file is configured (<see cref="PageFileConfig.IndexFilePath"/>), the page is allocated
    /// in the index file and its ID is registered for routing. Otherwise falls back to the main data file.
    /// </summary>
    /// <returns>Page ID of the allocated index page</returns>
    public uint AllocateIndexPage()
    {
        if (_indexFile == null)
            return _pageFile.AllocatePage();

        var pageId = _indexFile.AllocatePage();
        _indexPageIds.TryAdd(pageId, 0);
        return pageId;
    }

    /// <summary>
    /// Allocates a new data page for the specified collection.
    /// If per-collection files are configured (<see cref="PageFileConfig.CollectionDataDirectory"/>), the
    /// page is allocated in the collection's own file and the page ID is registered for routing.
    /// Otherwise falls back to the main data file.
    /// </summary>
    public uint AllocateCollectionPage(string collectionName)
    {
        if (_collectionFiles == null)
            return _pageFile.AllocatePage();

        var file = GetOrCreateCollectionFile(collectionName);
        var pageId = file.AllocatePage();
        _collectionPageMap!.TryAdd(pageId, collectionName);
        return pageId;
    }

    /// <summary>
    /// Frees a page.
    /// </summary>
    /// <param name="pageId">Page to free</param>
    public void FreePage(uint pageId)
    {
        GetPageFile(pageId).FreePage(pageId);
    }

    /// <summary>
    /// Returns the <see cref="PageFile"/> responsible for the given page ID.
    /// Routing order: index file → collection file → main data file.
    /// </summary>
    private PageFile GetPageFile(uint pageId)
    {
        if (_indexFile != null && _indexPageIds.ContainsKey(pageId))
            return _indexFile;

        if (_collectionPageMap != null && _collectionPageMap.TryGetValue(pageId, out var collName))
            return GetOrCreateCollectionFile(collName);

        return _pageFile;
    }

    /// <summary>
    /// Gets or creates the <see cref="PageFile"/> for the specified collection.
    /// If <see cref="PageFileConfig.CollectionDataDirectory"/> is not set, returns the main data file.
    /// </summary>
    private PageFile GetOrCreateCollectionFile(string collectionName)
    {
        if (_collectionFiles == null)
            return _pageFile;

        return _collectionFiles.GetOrAdd(collectionName, name =>
        {
            var filePath = Path.Combine(_config.CollectionDataDirectory!, $"{name}.db");
            var pf = new PageFile(filePath, AsStandaloneConfig(_config));
            pf.Open();
            return pf;
        });
    }

    /// <summary>
    /// Drops a collection's dedicated file, reclaiming disk space immediately.
    /// No-op if per-collection files are not configured (embedded mode: use the existing free list).
    /// <para>
    /// Callers must ensure that no concurrent read/write operations are in progress for the
    /// specified collection when this method is invoked, as it closes and deletes the file.
    /// </para>
    /// </summary>
    public void DropCollectionFile(string collectionName)
    {
        if (_collectionFiles == null) return;

        if (_collectionFiles.TryRemove(collectionName, out var pf))
        {
            pf.Dispose();
            var filePath = Path.Combine(_config.CollectionDataDirectory!, $"{collectionName}.db");
            if (File.Exists(filePath)) File.Delete(filePath);

            // Remove all pageId → collectionName mappings for this collection
            var toRemove = _collectionPageMap!
                .Where(kvp => string.Equals(kvp.Value, collectionName, StringComparison.OrdinalIgnoreCase))
                .Select(kvp => kvp.Key)
                .ToList();
            foreach (var pid in toRemove)
                _collectionPageMap!.TryRemove(pid, out _);
        }
    }
}
