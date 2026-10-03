using System;
using System.Threading;
using BLite.Core.Storage;

namespace BLite.Core.Collections;

internal sealed class FreeSpaceIndexProvider
{
    private readonly int _pageSize;
    private readonly bool _shareAcrossCollections;
    private readonly Lazy<FreeSpaceIndex>? _sharedIndex;

    public FreeSpaceIndexProvider(StorageEngine storage)
    {
        if (storage == null) throw new ArgumentNullException(nameof(storage));

        _pageSize = storage.PageSize;
        _shareAcrossCollections = !storage.UsesSeparateCollectionFiles;
        if (_shareAcrossCollections)
        {
            _sharedIndex = new Lazy<FreeSpaceIndex>(
                () => new FreeSpaceIndex(_pageSize, serializeAccess: true),
                LazyThreadSafetyMode.ExecutionAndPublication);
        }
    }

    public FreeSpaceIndex GetIndex() => _shareAcrossCollections
        ? _sharedIndex!.Value
        : new FreeSpaceIndex(_pageSize);

    /// <summary>
    /// Marks pages that were returned to the free list as having no usable space in the
    /// shared index. Without this, collections constructed earlier in the same context keep
    /// the stale "free bytes" they read from the pages' former data headers and route inserts
    /// to pages that are no longer data pages. Only the shared index needs this: per-collection
    /// indexes (separate-files mode) never track pages owned by another collection.
    /// </summary>
    public void InvalidatePages(System.Collections.Generic.IEnumerable<uint> pageIds)
    {
        if (!_shareAcrossCollections) return;
        var index = _sharedIndex!.Value;
        foreach (var pageId in pageIds)
            index.Update(pageId, 0);
    }
}
