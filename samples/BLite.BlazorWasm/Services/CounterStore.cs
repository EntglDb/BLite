using System;
using System.Threading.Tasks;
using BLite.Bson;
using BLite.Core;
using BLite.Wasm;
using System.Runtime.Versioning;

namespace BLite.BlazorWasm.Services;

/// <summary>
/// Singleton service that manages a named counter backed by BLite browser storage.
/// The counter value is persisted across page reloads using IndexedDB or OPFS,
/// depending on the browser's capabilities.
/// </summary>
[SupportedOSPlatform("browser")]
public sealed class CounterStore : IAsyncDisposable
{
    private const string CollectionName = "counters";
    private const string CounterName = "main";

    private BLiteEngine? _engine;
    private BsonId? _documentId;

    // ── Public state ──────────────────────────────────────────────────────────

    /// <summary>Current counter value.</summary>
    public int Value { get; private set; }

    /// <summary>Storage backend that was selected at initialisation time.</summary>
    public WasmStorageBackend Backend { get; private set; } = WasmStorageBackend.Auto;

    /// <summary>UTC timestamp of the last write, or <c>null</c> if not yet written.</summary>
    public DateTimeOffset? LastSaved { get; private set; }

    /// <summary>Whether the store has been initialised.</summary>
    public bool IsInitialized { get; private set; }

    /// <summary>Error message if initialisation or a save operation failed, otherwise <c>null</c>.</summary>
    public string? Error { get; private set; }

    // ── Initialisation ────────────────────────────────────────────────────────

    /// <summary>
    /// Opens (or re-opens) the BLite database and loads the persisted counter value.
    /// Safe to call multiple times; subsequent calls are no-ops.
    /// </summary>
    public async Task InitializeAsync()
    {
        if (IsInitialized)
            return;

        try
        {
            // Detect the backend before creating the engine so we can display it.
            Backend = BLiteWasm.DetectBestBackend();
            _engine = await BLiteWasm.CreateAsync("blite-counter-demo", Backend);

            // Try to restore the previously saved counter value.
            await foreach (var doc in _engine.FindAllAsync(CollectionName))
            {
                if (doc.TryGetString("name", out var name) && name == CounterName)
                {
                    doc.TryGetInt32("value", out var saved);
                    Value = saved;

                    if (doc.TryGetId(out var id))
                        _documentId = id;

                    if (doc.TryGetValue("updated_at", out var tsVal) && tsVal.IsInt64)
                        LastSaved = DateTimeOffset.FromUnixTimeMilliseconds(tsVal.AsInt64);

                    break;
                }
            }

            IsInitialized = true;
        }
        catch (Exception ex)
        {
            Error = $"BLite initialisation failed: {ex.Message}";
        }
    }

    // ── Mutations ─────────────────────────────────────────────────────────────

    /// <summary>Increments the counter by one and persists the new value.</summary>
    public async Task IncrementAsync()
    {
        Value++;
        await SaveAsync();
    }

    /// <summary>Decrements the counter by one and persists the new value.</summary>
    public async Task DecrementAsync()
    {
        Value--;
        await SaveAsync();
    }

    /// <summary>Resets the counter to zero and persists the new value.</summary>
    public async Task ResetAsync()
    {
        Value = 0;
        await SaveAsync();
    }

    // ── Persistence ───────────────────────────────────────────────────────────

    private async Task SaveAsync()
    {
        if (_engine == null)
            return;

        try
        {
            var now = DateTimeOffset.UtcNow;
            var doc = _engine.CreateDocument(
                ["name", "value", "updated_at"],
                b => b
                    .AddString("name", CounterName)
                    .AddInt32("value", Value)
                    .AddInt64("updated_at", now.ToUnixTimeMilliseconds()));

            if (_documentId.HasValue)
            {
                await _engine.UpdateAsync(CollectionName, _documentId.Value, doc);
            }
            else
            {
                _documentId = await _engine.InsertAsync(CollectionName, doc);
            }

            LastSaved = now;
            Error = null;
        }
        catch (Exception ex)
        {
            Error = $"Save failed: {ex.Message}";
        }
    }

    // ── Disposal ──────────────────────────────────────────────────────────────

    public ValueTask DisposeAsync()
    {
        _engine?.Dispose();
        return ValueTask.CompletedTask;
    }
}
