using System.Diagnostics;
using BLite.Bson;
using BLite.Core;
using BLite.Core.CDC;
using BLite.Core.GDPR;
using BLite.Core.Transactions;
using BLite.Shared;

namespace BLite.Tests.Gdpr;

/// <summary>
/// Acceptance tests for WP2 — CDC Field Masking.
/// Verifies the four-step masking pipeline defined in
/// <see cref="BLite.Core.GDPR.PayloadMask"/> applied during Watch().
/// </summary>
public class CdcMaskingTests : IDisposable
{
    private readonly string _dbPath;
    private readonly CdcMaskingTestContext _db;

    public CdcMaskingTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"blite_cdcmask_{Guid.NewGuid():N}.db");
        _db = new CdcMaskingTestContext(_dbPath);
    }

    public void Dispose()
    {
        _db.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static async Task WaitForEvents<T>(List<T> list, int expected, int timeoutMs = 800)
    {
        var deadline = Environment.TickCount64 + timeoutMs;
        while (list.Count < expected && Environment.TickCount64 < deadline)
            await Task.Delay(20);
    }

    private GdprPerson MakePerson(int id, string email, string name) =>
        new() { Id = id, Email = email, Name = name };

    // ── Test 1: CapturePayload=false — no masking, no payload (regression) ────

    /// <summary>
    /// With <c>CapturePayload = false</c>, observers receive no payload.
    /// Original bytes in the storage page must not be mutated.
    /// </summary>
    [Fact]
    public async Task CapturePayloadFalse_NullEntity_NoMaskingApplied()
    {
        var events = new List<ChangeStreamEvent<int, GdprPerson>>();
        using var sub = _db.GdprPeople.Watch(new WatchOptions { CapturePayload = false })
            .Subscribe(e => events.Add(e));

        await _db.GdprPeople.InsertAsync(MakePerson(1, "alice@example.com", "Alice"));
        await _db.SaveChangesAsync();

        await WaitForEvents(events, 1);

        Assert.Single(events);
        Assert.Equal(OperationType.Insert, events[0].Type);
        // Entity must be null when CapturePayload = false
        Assert.Null(events[0].Entity);
    }

    // ── Test 2: Default mask (CapturePayload=true, no options) ──────────────

    /// <summary>
    /// Default behaviour (GDPR-safe): <c>[PersonalData]</c> fields are masked with "***".
    /// Non-personal fields pass through unchanged.
    /// </summary>
    [Fact]
    public async Task DefaultMask_PersonalDataField_ReplacedWithStars()
    {
        var events = new List<ChangeStreamEvent<int, GdprPerson>>();
        // CapturePayload=true, RevealPersonalData defaults to false — GDPR-safe default.
        using var sub = _db.GdprPeople.Watch(new WatchOptions { CapturePayload = true })
            .Subscribe(e => events.Add(e));

        await _db.GdprPeople.InsertAsync(MakePerson(2, "bob@example.com", "Bob"));
        await _db.SaveChangesAsync();

        await WaitForEvents(events, 1);

        Assert.Single(events);
        var entity = events[0].Entity;
        Assert.NotNull(entity);
        // Email is [PersonalData] → must be masked
        Assert.Equal("***", entity!.Email);
        // Name is not [PersonalData] → must pass through unchanged
        Assert.Equal("Bob", entity.Name);
    }

    // ── Test 3: Opt-in reveal ────────────────────────────────────────────────

    /// <summary>
    /// With <c>RevealPersonalData = true</c>, personal-data fields are delivered in clear.
    /// </summary>
    [Fact]
    public async Task RevealPersonalData_PersonalDataField_DeliveredInClear()
    {
        var events = new List<ChangeStreamEvent<int, GdprPerson>>();
        using var sub = _db.GdprPeople.Watch(new WatchOptions
        {
            CapturePayload = true,
            RevealPersonalData = true
        }).Subscribe(e => events.Add(e));

        await _db.GdprPeople.InsertAsync(MakePerson(3, "carol@example.com", "Carol"));
        await _db.SaveChangesAsync();

        await WaitForEvents(events, 1);

        Assert.Single(events);
        var entity = events[0].Entity;
        Assert.NotNull(entity);
        Assert.Equal("carol@example.com", entity!.Email);
        Assert.Equal("Carol", entity.Name);
    }

    // ── Test 4: Custom mask value ────────────────────────────────────────────

    /// <summary>
    /// A custom <see cref="WatchOptions.PersonalDataMaskValue"/> replaces personal-data
    /// fields with the provided value.
    /// </summary>
    [Fact]
    public async Task CustomMaskValue_PersonalDataField_ReplacedWithCustomValue()
    {
        var events = new List<ChangeStreamEvent<int, GdprPerson>>();
        using var sub = _db.GdprPeople.Watch(new WatchOptions
        {
            CapturePayload = true,
            PersonalDataMaskValue = BsonValue.FromString("[REDACTED]")
        }).Subscribe(e => events.Add(e));

        await _db.GdprPeople.InsertAsync(MakePerson(4, "dave@example.com", "Dave"));
        await _db.SaveChangesAsync();

        await WaitForEvents(events, 1);

        Assert.Single(events);
        var entity = events[0].Entity;
        Assert.NotNull(entity);
        Assert.Equal("[REDACTED]", entity!.Email);
    }

    // ── Test 5: BsonValue.Null mask = drop key ───────────────────────────────

    /// <summary>
    /// When <c>PersonalDataMaskValue = BsonValue.Null</c>, personal-data keys are
    /// <em>removed</em> from the cloned document instead of replaced.
    /// The entity's masked field falls back to its default value after deserialization.
    /// </summary>
    [Fact]
    public async Task NullMaskValue_PersonalDataKey_RemovedFromDocument()
    {
        var events = new List<ChangeStreamEvent<int, GdprPerson>>();
        using var sub = _db.GdprPeople.Watch(new WatchOptions
        {
            CapturePayload = true,
            PersonalDataMaskValue = BsonValue.Null
        }).Subscribe(e => events.Add(e));

        await _db.GdprPeople.InsertAsync(MakePerson(5, "eve@example.com", "Eve"));
        await _db.SaveChangesAsync();

        await WaitForEvents(events, 1);

        Assert.Single(events);
        var entity = events[0].Entity;
        Assert.NotNull(entity);
        // Key removed → deserialized to the CLR default (null for string reference types)
        Assert.Null(entity!.Email);
        Assert.Equal("Eve", entity.Name);
    }

    // ── Test 6: ExcludeFields only ───────────────────────────────────────────

    /// <summary>
    /// With <c>ExcludeFields</c>, listed fields are stripped from the payload.
    /// Personal-data fields not in the exclude list are still masked by default.
    /// </summary>
    [Fact]
    public async Task ExcludeFields_FieldRemovedFromDocument()
    {
        var events = new List<ChangeStreamEvent<int, GdprPerson>>();
        using var sub = _db.GdprPeople.Watch(new WatchOptions
        {
            CapturePayload = true,
            RevealPersonalData = true,          // reveal so we can check the exclude
            ExcludeFields = ["name"]
        }).Subscribe(e => events.Add(e));

        await _db.GdprPeople.InsertAsync(MakePerson(6, "frank@example.com", "Frank"));
        await _db.SaveChangesAsync();

        await WaitForEvents(events, 1);

        Assert.Single(events);
        var entity = events[0].Entity;
        Assert.NotNull(entity);
        // Name was excluded → deserialized to the CLR default (null for string reference types)
        Assert.Null(entity!.Name);
        // Email is revealed (RevealPersonalData=true)
        Assert.Equal("frank@example.com", entity.Email);
    }

    // ── Test 7: IncludeOnlyFields with allowlisted personal-data field ────────

    /// <summary>
    /// <c>IncludeOnlyFields = ["email"]</c> delivers only the Email field in clear —
    /// listing a personal-data field in the allowlist is the consumer's explicit
    /// opt-in regardless of <c>RevealPersonalData</c>.
    /// </summary>
    [Fact]
    public async Task IncludeOnlyFields_PersonalDataField_DeliveredInClear()
    {
        var events = new List<ChangeStreamEvent<int, GdprPerson>>();
        using var sub = _db.GdprPeople.Watch(new WatchOptions
        {
            CapturePayload = true,
            RevealPersonalData = false,         // masking would normally apply
            IncludeOnlyFields = ["email"]       // but allowlist wins
        }).Subscribe(e => events.Add(e));

        await _db.GdprPeople.InsertAsync(MakePerson(7, "grace@example.com", "Grace"));
        await _db.SaveChangesAsync();

        await WaitForEvents(events, 1);

        Assert.Single(events);
        var entity = events[0].Entity;
        Assert.NotNull(entity);
        // Email is in IncludeOnlyFields → in clear
        Assert.Equal("grace@example.com", entity!.Email);
        // Name was not listed → removed; deserialized to CLR default (null for string)
        Assert.Null(entity.Name);
    }

    // ── Test 8: IncludeOnlyFields + ExcludeFields — allowlist wins ───────────

    /// <summary>
    /// When both <c>IncludeOnlyFields</c> and <c>ExcludeFields</c> are set, the
    /// allowlist wins and <c>ExcludeFields</c> is ignored entirely.
    /// </summary>
    [Fact]
    public async Task AllowlistAndExclude_AllowlistWins()
    {
        var events = new List<ChangeStreamEvent<int, GdprPerson>>();
        using var sub = _db.GdprPeople.Watch(new WatchOptions
        {
            CapturePayload = true,
            IncludeOnlyFields = ["name"],       // keep only name
            ExcludeFields = ["name"]            // ExcludeFields is ignored (allowlist wins)
        }).Subscribe(e => events.Add(e));

        await _db.GdprPeople.InsertAsync(MakePerson(8, "hank@example.com", "Hank"));
        await _db.SaveChangesAsync();

        await WaitForEvents(events, 1);

        Assert.Single(events);
        var entity = events[0].Entity;
        Assert.NotNull(entity);
        // Name is in allowlist, so it is present
        Assert.Equal("Hank", entity!.Name);
        // Email not in allowlist → removed; deserialized to CLR default (null for string)
        Assert.Null(entity.Email);
    }

    // ── Test 9: Dynamic stream no-op + single info log ───────────────────────

    /// <summary>
    /// For dynamic (untyped) collections, personal-data masking rule 2 is a no-op and
    /// the payload is delivered as-is. A single info-level <see cref="Trace"/> message
    /// is emitted on the first subscription per collection.
    /// </summary>
    [Fact]
    public async Task DynamicStream_NoPersonalDataMasking_LogEmittedOnce()
    {
        var logMessages = new List<string>();
        var listener = new CapturingTraceListener(logMessages);
        Trace.Listeners.Add(listener);
        try
        {
            var dynamicDbPath = Path.Combine(Path.GetTempPath(), $"blite_cdcmask_dyn_{Guid.NewGuid():N}.db");
            var engine = new BLiteEngine(dynamicDbPath);
            try
            {
                var col = engine.GetOrCreateCollection("dyntest");
                var docFields = new[] { "email", "name" };

                var events = new List<BsonChangeEvent>();

                // First subscription — should emit the advisory.
                using var sub1 = col.Watch(new WatchOptions { CapturePayload = true, RevealPersonalData = false })
                    .Subscribe(e => events.Add(e));

                // Second subscription on the same collection — warning must NOT fire again.
                using var sub2 = col.Watch(new WatchOptions { CapturePayload = true, RevealPersonalData = false })
                    .Subscribe(_ => { });

                var doc = col.CreateDocument(docFields,
                    b => b.AddString("email", "ivan@example.com").AddString("name", "Ivan"));
                await col.InsertAsync(doc);
                await engine.CommitAsync();

                await WaitForEvents(events, 1);

                Assert.Single(events);
                // Document must pass through unchanged (no masking for dynamic collections).
                Assert.NotNull(events[0].Payload);
                Assert.True(events[0].Payload!.TryGetValue("email", out var emailVal));
                Assert.Equal("ivan@example.com", emailVal.AsString);

                // Exactly one advisory message for this collection.
                var cdcLogs = logMessages.Where(m => m.Contains("dyntest") && m.Contains("CapturePayload")).ToList();
                Assert.Single(cdcLogs);
            }
            finally
            {
                engine.Dispose();
                if (File.Exists(dynamicDbPath)) File.Delete(dynamicDbPath);
                var dynamicWal = Path.ChangeExtension(dynamicDbPath, ".wal");
                if (File.Exists(dynamicWal)) File.Delete(dynamicWal);
            }
        }
        finally
        {
            Trace.Listeners.Remove(listener);
        }
    }

    // ── Capturing TraceListener helper ───────────────────────────────────────

    private sealed class CapturingTraceListener : TraceListener
    {
        private readonly List<string> _messages;
        public CapturingTraceListener(List<string> messages) => _messages = messages;
        public override void Write(string? message) { }
        public override void WriteLine(string? message)
        {
            if (message != null) _messages.Add(message);
        }
        public override void TraceEvent(TraceEventCache? eventCache, string source, TraceEventType eventType, int id, string? message)
        {
            if (message != null) _messages.Add(message);
        }
        public override void TraceEvent(TraceEventCache? eventCache, string source, TraceEventType eventType, int id, string? format, params object?[]? args)
        {
            if (format != null)
                _messages.Add(args != null ? string.Format(format, args) : format);
        }
    }
}
