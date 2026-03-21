using BLite.Bson;
using BLite.Core;
using BLite.Core.CDC;
using BLite.Core.Transactions;

namespace BLite.Tests;

/// <summary>
/// Unit tests for <see cref="DynamicChangeStreamObservable"/> — exercises the
/// schema-less Watch() API on DynamicCollection and verifies that the async
/// BridgeAsync loop correctly forwards change events to subscribers.
/// </summary>
public class DynamicChangeStreamObservableTests : IDisposable
{
    private readonly string _dbPath;
    private readonly BLiteEngine _engine;
    private readonly DynamicCollection _col;

    public DynamicChangeStreamObservableTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"dco_test_{Guid.NewGuid()}.db");
        _engine = new BLiteEngine(_dbPath);
        _col = _engine.GetOrCreateCollection("events");
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private BsonDocument MakeDoc(string name) =>
        _col.CreateDocument(
            ["name"],
            b => b.AddString("name", name));

    private static async Task WaitForEvents(List<BsonChangeEvent> list, int expected, int timeoutMs = 500)
    {
        var deadline = Environment.TickCount64 + timeoutMs;
        while (list.Count < expected && Environment.TickCount64 < deadline)
            await Task.Delay(20);
    }

    // ── Insert events ─────────────────────────────────────────────────────────

    [Fact]
    public async Task Watch_Insert_FiresEvent()
    {
        var events = new List<BsonChangeEvent>();
        using var sub = _col.Watch(capturePayload: false).Subscribe(e => events.Add(e));

        _col.Insert(MakeDoc("Alice"));
        _engine.Commit();

        await WaitForEvents(events, 1);

        Assert.Single(events);
        Assert.Equal(OperationType.Insert, events[0].Type);
        Assert.Equal("events", events[0].CollectionName);
    }

    [Fact]
    public async Task Watch_Insert_MultipleDocuments_FiresAllEvents()
    {
        var events = new List<BsonChangeEvent>();
        using var sub = _col.Watch(capturePayload: false).Subscribe(e => events.Add(e));

        for (int i = 0; i < 5; i++)
            _col.Insert(MakeDoc($"Item{i}"));
        _engine.Commit();

        await WaitForEvents(events, 5);

        Assert.Equal(5, events.Count);
        Assert.All(events, e => Assert.Equal(OperationType.Insert, e.Type));
    }

    // ── Payload capture ───────────────────────────────────────────────────────

    [Fact]
    public async Task Watch_CapturePayload_True_PayloadIsSet()
    {
        var events = new List<BsonChangeEvent>();
        using var sub = _col.Watch(capturePayload: true).Subscribe(e => events.Add(e));

        _col.Insert(MakeDoc("WithPayload"));
        _engine.Commit();

        await WaitForEvents(events, 1);

        Assert.Single(events);
        Assert.NotNull(events[0].Payload);
    }

    [Fact]
    public async Task Watch_CapturePayload_False_PayloadIsNull()
    {
        var events = new List<BsonChangeEvent>();
        using var sub = _col.Watch(capturePayload: false).Subscribe(e => events.Add(e));

        _col.Insert(MakeDoc("NoPayload"));
        _engine.Commit();

        await WaitForEvents(events, 1);

        Assert.Single(events);
        Assert.Null(events[0].Payload);
    }

    // ── Update events ─────────────────────────────────────────────────────────

    [Fact]
    public async Task Watch_Update_FiresUpdateEvent()
    {
        var doc = MakeDoc("Original");
        var id = _col.Insert(doc);
        _engine.Commit();

        var events = new List<BsonChangeEvent>();
        using var sub = _col.Watch(capturePayload: false).Subscribe(e => events.Add(e));

        var updated = _col.CreateDocument(["name"],
            b => b.AddString("name", "Updated"));
        _col.Update(id, updated);
        _engine.Commit();

        await WaitForEvents(events, 1);

        Assert.Single(events);
        Assert.Equal(OperationType.Update, events[0].Type);
    }

    // ── Delete events ─────────────────────────────────────────────────────────

    [Fact]
    public async Task Watch_Delete_FiresDeleteEvent()
    {
        var doc = MakeDoc("ToDelete");
        var id = _col.Insert(doc);
        _engine.Commit();

        var events = new List<BsonChangeEvent>();
        using var sub = _col.Watch(capturePayload: false).Subscribe(e => events.Add(e));

        _col.Delete(id);
        _engine.Commit();

        await WaitForEvents(events, 1);

        Assert.Single(events);
        Assert.Equal(OperationType.Delete, events[0].Type);
    }

    // ── Dispose / unsubscribe ─────────────────────────────────────────────────

    [Fact]
    public async Task Watch_Dispose_StopsReceivingEvents()
    {
        var events = new List<BsonChangeEvent>();
        var sub = _col.Watch(capturePayload: false).Subscribe(e => events.Add(e));

        _col.Insert(MakeDoc("Before"));
        _engine.Commit();
        await WaitForEvents(events, 1);
        Assert.Single(events);

        sub.Dispose();

        _col.Insert(MakeDoc("After"));
        _engine.Commit();
        await Task.Delay(200); // let any pending events arrive

        // No new events after dispose
        Assert.Single(events);
    }

    // ── Multiple subscribers ───────────────────────────────────────────────────

    [Fact]
    public async Task Watch_MultipleSubscribers_BothReceiveEvent()
    {
        var events1 = new List<BsonChangeEvent>();
        var events2 = new List<BsonChangeEvent>();

        using var sub1 = _col.Watch(capturePayload: false).Subscribe(e => events1.Add(e));
        using var sub2 = _col.Watch(capturePayload: false).Subscribe(e => events2.Add(e));

        _col.Insert(MakeDoc("Shared"));
        _engine.Commit();

        await WaitForEvents(events1, 1);
        await WaitForEvents(events2, 1);

        Assert.Single(events1);
        Assert.Single(events2);
    }

    // ── Subscribe returns non-null ────────────────────────────────────────────

    [Fact]
    public void Watch_ReturnsNonNullObservable()
    {
        var observable = _col.Watch(capturePayload: false);
        Assert.NotNull(observable);
    }

    [Fact]
    public void Watch_Subscribe_ReturnsNonNullDisposable()
    {
        var sub = _col.Watch(capturePayload: false).Subscribe(_ => { });
        Assert.NotNull(sub);
        sub.Dispose();
    }

    // ── TransactionId and Timestamp ───────────────────────────────────────────

    [Fact]
    public async Task Watch_Event_HasValidTimestampAndTransactionId()
    {
        var events = new List<BsonChangeEvent>();
        using var sub = _col.Watch(capturePayload: false).Subscribe(e => events.Add(e));

        _col.Insert(MakeDoc("Stamped"));
        _engine.Commit();

        await WaitForEvents(events, 1);

        Assert.Single(events);
        Assert.True(events[0].Timestamp > 0);
        Assert.True(events[0].TransactionId > 0);
    }
}
