using BLite.Core.Transactions;

namespace BLite.Core.Storage;

public sealed partial class StorageEngine
{
    /// <summary>
    /// Allocates a new page.
    /// </summary>
    /// <returns>Page ID of the allocated page</returns>
    public uint AllocatePage()
    {
        return _pageFile.AllocatePage();
    }

    /// <summary>
    /// Frees a page.
    /// </summary>
    /// <param name="pageId">Page to free</param>
    public void FreePage(uint pageId)
    {
        _pageFile.FreePage(pageId);
    }
}
