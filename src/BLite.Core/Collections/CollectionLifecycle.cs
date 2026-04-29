using System.Threading;
using System.Threading.Tasks;

namespace BLite.Core.Collections;

internal interface ICollectionLifecycle
{
    string CollectionName { get; }
    Task<int> TruncateCollectionAsync(CancellationToken ct = default);
    void MarkDropped();
}
