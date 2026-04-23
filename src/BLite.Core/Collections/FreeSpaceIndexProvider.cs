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
}
