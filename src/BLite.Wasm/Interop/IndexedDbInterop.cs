using System.Runtime.InteropServices.JavaScript;
using System.Threading.Tasks;

namespace BLite.Wasm.Interop;

/// <summary>
/// JavaScript interop bridge for the IndexedDB storage backend.
/// Methods map 1:1 to the exports in <c>blite-indexeddb.mjs</c>.
/// <para>
/// Async methods exchange binary data as base64 strings because <c>[JSImport]</c>
/// does not support <c>byte[]</c> or <c>Span&lt;byte&gt;</c> on <c>Task</c>-returning methods.
/// This is acceptable because IndexedDB is already the "slower but more compatible" path.
/// </para>
/// </summary>
internal static partial class IndexedDbInterop
{
    private const string ModuleName = "./blite-indexeddb.mjs";

    // ─── Page storage ────────────────────────────────────────────────────────

    [JSImport("idbOpen", ModuleName)]
    internal static partial Task<double> OpenAsync(string dbName);

    [JSImport("idbReadPage", ModuleName)]
    internal static partial Task<string> ReadPageAsync(string dbName, int pageId, int pageSize);

    [JSImport("idbWritePage", ModuleName)]
    internal static partial Task WritePageAsync(string dbName, int pageId, string base64Data);

    [JSImport("idbSaveNextPageId", ModuleName)]
    internal static partial Task SaveNextPageIdAsync(string dbName, int nextPageId);

    [JSImport("idbDeletePage", ModuleName)]
    internal static partial Task DeletePageAsync(string dbName, int pageId);

    [JSImport("idbClose", ModuleName)]
    internal static partial void Close(string dbName);

    [JSImport("idbIsAvailable", ModuleName)]
    [return: JSMarshalAs<JSType.Boolean>]
    internal static partial bool IsAvailable();

    // ─── WAL ─────────────────────────────────────────────────────────────────

    [JSImport("idbWalOpen", ModuleName)]
    internal static partial Task WalOpenAsync(string dbName);

    [JSImport("idbWalAppend", ModuleName)]
    internal static partial Task WalAppendAsync(string dbName, string base64RecordData);

    [JSImport("idbWalReadAll", ModuleName)]
    internal static partial Task<string> WalReadAllAsync(string dbName);

    [JSImport("idbWalClear", ModuleName)]
    internal static partial Task WalClearAsync(string dbName);

    [JSImport("idbWalGetSize", ModuleName)]
    internal static partial Task<double> WalGetSizeAsync(string dbName);

    /// <summary>
    /// Loads the JavaScript module so that all <c>[JSImport]</c> calls can resolve.
    /// Must be called once before any other method in this class.
    /// </summary>
    internal static async Task EnsureLoadedAsync()
    {
        await JSHost.ImportAsync(ModuleName, ModuleName);
    }
}
