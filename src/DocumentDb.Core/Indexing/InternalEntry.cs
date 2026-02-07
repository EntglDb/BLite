using DocumentDb.Core.Indexing;

namespace DocumentDb.Core.Indexing;

public struct InternalEntry
{
    public IndexKey Key { get; set; }
    public uint PageId { get; set; }

    public InternalEntry(IndexKey key, uint pageId)
    {
        Key = key;
        PageId = pageId;
    }
}
