using System.Runtime.InteropServices.JavaScript;
using System.Threading.Tasks;

namespace BLite.Wasm.Interop;

/// <summary>
/// JavaScript interop bridge for the OPFS Worker backend.
/// Methods map 1:1 to the exports in <c>blite-opfs-bridge.mjs</c>.
/// All operations are async because they round-trip through a dedicated Web Worker.
/// </summary>
internal static partial class OpfsWorkerInterop
{
    private const string ModuleName = "./blite-opfs-bridge.mjs";

    [JSImport("opfsWorkerOpen", ModuleName)]
    internal static partial Task<double> OpenAsync(string dbName, int pageSize);

    [JSImport("opfsWorkerReadPage", ModuleName)]
    internal static partial Task<string> ReadPageAsync(string dbName, int pageId);

    [JSImport("opfsWorkerWritePage", ModuleName)]
    internal static partial Task WritPageAsync(string dbName, int pageId, string base64Data);

    [JSImport("opfsWorkerFlush", ModuleName)]
    internal static partial Task FlushAsync(string dbName);

    [JSImport("opfsWorkerGetSize", ModuleName)]
    internal static partial Task<double> GetSizeAsync(string dbName);

    [JSImport("opfsWorkerTruncate", ModuleName)]
    internal static partial Task TruncateAsync(string dbName);

    [JSImport("opfsWorkerClose", ModuleName)]
    internal static partial Task CloseAsync(string dbName);

    [JSImport("opfsWorkerIsAvailable", ModuleName)]
    [return: JSMarshalAs<JSType.Boolean>]
    internal static partial bool IsAvailable();

    /// <summary>
    /// Loads the bridge JavaScript module so that all <c>[JSImport]</c> calls can resolve.
    /// Must be called once before any other method in this class.
    /// </summary>
    internal static async Task EnsureLoadedAsync()
    {
        await JSHost.ImportAsync(ModuleName, "../blite-opfs-bridge.mjs");
    }
}
