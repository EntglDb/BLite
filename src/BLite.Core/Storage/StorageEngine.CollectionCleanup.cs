using BLite.Core.Indexing;
using System.Collections.Generic;

namespace BLite.Core.Storage;

public sealed partial class StorageEngine
{
    /// <summary>
    /// In single-file mode, frees fully-deleted slotted data pages so they can be reused immediately.
    /// In multi-file mode this is a no-op because dropping the dedicated collection file reclaims space.
    /// </summary>
    public IReadOnlyList<uint> FreeCollectionPages(string collectionName)
    {
        if (UsesSeparateCollectionFiles)
            return [];

        var freedPages = new List<uint>();
        var buffer = new byte[PageSize];

        for (uint pageId = 2; pageId < _pageFile.NextPageId; pageId++)
        {
            ReadPage(pageId, null, buffer);
            var header = SlottedPageHeader.ReadFrom(buffer);
            if (header.PageType != PageType.Data || header.SlotCount == 0)
                continue;

            bool hasLiveSlots = false;
            for (ushort slotIndex = 0; slotIndex < header.SlotCount; slotIndex++)
            {
                var slotOffset = SlottedPageHeader.Size + (slotIndex * SlotEntry.Size);
                var slot = SlotEntry.ReadFrom(buffer.AsSpan(slotOffset));
                if ((slot.Flags & SlotFlags.Deleted) == 0)
                {
                    hasLiveSlots = true;
                    break;
                }
            }

            if (hasLiveSlots)
                continue;

            FreePage(pageId);
            freedPages.Add(pageId);
        }

        return freedPages;
    }

    internal void FreeCollectionRoots(CollectionMetadata metadata)
    {
        if (metadata.PrimaryRootPageId != 0)
        {
            foreach (var pageId in new BTreeIndex(this, IndexOptions.CreateUnique("_id"), metadata.PrimaryRootPageId).CollectAllPages())
                FreePage(pageId);
        }

        foreach (var index in metadata.Indexes)
        {
            if (index.RootPageId == 0)
                continue;

            switch (index.Type)
            {
                case IndexType.Vector:
                    foreach (var pageId in new VectorSearchIndex(this, IndexOptions.CreateVector(index.Dimensions, index.Metric, 16, 200, index.PropertyPaths), index.RootPageId).CollectAllPages())
                        FreePage(pageId);
                    break;
                case IndexType.Spatial:
                    FreePage(index.RootPageId);
                    break;
                default:
                    var opts = index.IsUnique ? IndexOptions.CreateUnique(index.PropertyPaths) : IndexOptions.CreateBTree(index.PropertyPaths);
                    foreach (var pageId in new BTreeIndex(this, opts, index.RootPageId).CollectAllPages())
                        FreePage(pageId);
                    break;
            }
        }

        FreeLinkedPages(metadata.SchemaRootPageId);
        if (metadata.IsTimeSeries)
            FreeLinkedPages(metadata.TimeSeriesHeadPageId);
    }

    private void FreeLinkedPages(uint firstPageId)
    {
        var headerBuffer = new byte[32];
        var current = firstPageId;
        while (current != 0)
        {
            ReadPageHeader(current, null, headerBuffer);
            var header = PageHeader.ReadFrom(headerBuffer);
            var next = header.NextPageId;
            FreePage(current);
            current = next;
        }
    }
}
