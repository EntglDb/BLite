using System.Runtime.InteropServices.JavaScript;
using System.Threading.Tasks;

namespace BLite.Wasm.Interop;

/// <summary>
/// JavaScript interop bridge for the Origin Private File System (OPFS) backend.
/// Methods map 1:1 to the exports in <c>blite-opfs.mjs</c>.
/// </summary>
internal static partial class OpfsInterop
{
    private const string ModuleName = "./blite-opfs.mjs";

    [JSImport("opfsOpen", ModuleName)]
    internal static partial Task<double> OpenAsync(string dbName, int pageSize);

    [JSImport("opfsReadPage", ModuleName)]
    internal static partial int ReadPage(string dbName, int pageId, [JSMarshalAs<JSType.MemoryView>] Span<byte> destination);

    [JSImport("opfsWritePage", ModuleName)]
    internal static partial void WritePage(string dbName, int pageId, [JSMarshalAs<JSType.MemoryView>] Span<byte> source);

    [JSImport("opfsFlush", ModuleName)]
    internal static partial void Flush(string dbName);

    [JSImport("opfsGetSize", ModuleName)]
    internal static partial double GetSize(string dbName);

    [JSImport("opfsTruncate", ModuleName)]
    internal static partial void Truncate(string dbName);

    [JSImport("opfsClose", ModuleName)]
    internal static partial void Close(string dbName);

    [JSImport("opfsIsAvailable", ModuleName)]
    [return: JSMarshalAs<JSType.Boolean>]
    internal static partial bool IsAvailable();

    /// <summary>
    /// Loads the JavaScript module so that all <c>[JSImport]</c> calls can resolve.
    /// Must be called once before any other method in this class.
    /// </summary>
    internal static async Task EnsureLoadedAsync()
    {
        await JSHost.ImportAsync(ModuleName, ModuleName);
    }
}
