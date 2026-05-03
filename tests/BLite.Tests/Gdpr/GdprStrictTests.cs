using System.Diagnostics;
using BLite.Core;
using BLite.Core.Encryption;
using BLite.Core.GDPR;
using BLite.Core.KeyValue;
using BLite.Core.Metadata;

namespace BLite.Tests.Gdpr;

/// <summary>
/// Acceptance tests for WP3 — GdprMode.Strict privacy-by-default orchestration.
/// Covers every row of the strict-mode invariant table from GDPR_PLAN.md §4.5.
/// </summary>
public class GdprStrictTests : IDisposable
{
    // ── TraceListener helper ──────────────────────────────────────────────────

    /// <summary>
    /// Captures <see cref="Trace.TraceWarning"/> and <see cref="Trace.TraceInformation"/>
    /// calls so tests can assert on log output without an ILogger dependency.
    /// </summary>
    private sealed class CapturingTraceListener : TraceListener
    {
        private readonly List<string> _messages = new();
        public IReadOnlyList<string> Messages => _messages;

        public override void Write(string? message) { }
        public override void WriteLine(string? message)
        {
            if (message != null)
                _messages.Add(message);
        }
    }

    private readonly CapturingTraceListener _listener = new();
    private readonly List<string> _tempPaths = new();

    public GdprStrictTests()
    {
        Trace.Listeners.Add(_listener);
    }

    public void Dispose()
    {
        Trace.Listeners.Remove(_listener);
        foreach (var path in _tempPaths)
        {
            TryDelete(path);
            TryDelete(Path.ChangeExtension(path, ".wal"));
        }
    }

    private string TempDb()
    {
        var path = Path.Combine(Path.GetTempPath(), $"blite_gdpr_strict_{Guid.NewGuid():N}.db");
        _tempPaths.Add(path);
        return path;
    }

    private static void TryDelete(string path)
    {
        try { if (File.Exists(path)) File.Delete(path); } catch { /* best-effort */ }
    }

    private bool HasWarning(int eventId)
        => _listener.Messages.Any(m => m.Contains($"EventId:{eventId}"));

    private int CountWarnings(int eventId)
        => _listener.Messages.Count(m => m.Contains($"EventId:{eventId}"));

    // ── Test 1: GdprMode.None baseline — zero validator activity ─────────────

    /// <summary>
    /// GdprMode.None (default) must never throw and must emit zero validator log lines.
    /// </summary>
    [Fact]
    public void GdprModeNone_NoThrow_ZeroLogLines()
    {
        var path = TempDb();
        // No kvOptions → DefaultGdprMode = None
        using var engine = new BLiteEngine(path);

        // None of the GDPR strict EventIds (9001–9007) should appear.
        for (int id = 9001; id <= 9007; id++)
            Assert.False(HasWarning(id), $"EventId {id} must NOT be emitted for GdprMode.None");
    }

    // ── Test 2: Strict without encryption → InvalidOperationException ─────────

    /// <summary>
    /// Strict mode with no encryption configured must throw an
    /// <see cref="InvalidOperationException"/> whose message contains
    /// "GdprMode.Strict requires".
    /// </summary>
    [Fact]
    public void GdprStrict_NoEncryption_ThrowsInvalidOperation()
    {
        var path = TempDb();
        var kvOpts = new BLiteKvOptions { DefaultGdprMode = GdprMode.Strict };

        var ex = Assert.Throws<InvalidOperationException>(
            () => new BLiteEngine(path, kvOpts));

        Assert.Contains("GdprMode.Strict requires", ex.Message);
    }

    // ── Test 3: Strict + encryption — engine starts ───────────────────────────

    /// <summary>
    /// Strict mode with encryption configured must not throw.
    /// </summary>
    [Fact]
    public void GdprStrict_WithEncryption_DoesNotThrow()
    {
        var path = TempDb();
        var crypto = new CryptoOptions("gdpr-strict-test-passphrase");
        var kvOpts = new BLiteKvOptions { DefaultGdprMode = GdprMode.Strict };

        using var engine = new BLiteEngine(path, crypto, kvOpts);
        Assert.NotNull(engine);
    }

    // ── Test 4: Strict + encryption + no audit sink → audit warning ───────────

    /// <summary>
    /// Strict + encryption but no audit sink registered: engine must start and emit
    /// exactly one warning with EventId <c>GdprStrictAuditMissing</c> (9002).
    /// </summary>
    [Fact]
    public void GdprStrict_EncryptionNoAudit_EmitsAuditMissingWarning()
    {
        var path = TempDb();
        var crypto = new CryptoOptions("gdpr-strict-test-passphrase");
        var kvOpts = new BLiteKvOptions { DefaultGdprMode = GdprMode.Strict };

        using var engine = new BLiteEngine(path, crypto, kvOpts);

        Assert.Equal(1, CountWarnings(GdprStrictValidator.GdprStrictAuditMissing));
    }

    // ── Test 5: Strict forces SecureErase warning (not yet implemented) ────────

    /// <summary>
    /// Strict mode warns that SecureEraseOnDelete is not yet available
    /// (EventId <c>GdprStrictSecureEraseUnavailable</c>, 9006).
    /// </summary>
    [Fact]
    public void GdprStrict_SecureErase_EmitsUnavailableWarning()
    {
        var path = TempDb();
        var crypto = new CryptoOptions("gdpr-strict-test-passphrase");
        var kvOpts = new BLiteKvOptions { DefaultGdprMode = GdprMode.Strict };

        using var engine = new BLiteEngine(path, crypto, kvOpts);

        Assert.True(HasWarning(GdprStrictValidator.GdprStrictSecureEraseUnavailable),
            "Expected GdprStrictSecureEraseUnavailable (9006) warning when Strict mode is active.");
    }

    // ── Test 6: GdprMode enum values are correct ─────────────────────────────

    /// <summary>
    /// Verifies the <see cref="GdprMode"/> enum contract: None = 0, Strict = 1.
    /// </summary>
    [Fact]
    public void GdprMode_EnumValues_AreCorrect()
    {
        Assert.Equal((byte)0, (byte)GdprMode.None);
        Assert.Equal((byte)1, (byte)GdprMode.Strict);
    }

    // ── Test 7: HasGdprMode fluent extension persists the mode ────────────────

    /// <summary>
    /// Verifies that <see cref="EntityTypeBuilderGdprExtensions.HasGdprMode{T}"/> sets
    /// the <c>GdprMode</c> property on the builder.
    /// </summary>
    [Fact]
    public void HasGdprMode_FluentExtension_PersistsMode()
    {
        var builder = new EntityTypeBuilder<GdprPerson>();
        builder.HasGdprMode(GdprMode.Strict);
        Assert.Equal(GdprMode.Strict, builder.GdprMode);
    }

    // ── Test 8: DefaultGdprMode on BLiteKvOptions defaults to None ────────────

    /// <summary>
    /// <see cref="BLiteKvOptions.DefaultGdprMode"/> must default to
    /// <see cref="GdprMode.None"/> (backwards-compatible).
    /// </summary>
    [Fact]
    public void BLiteKvOptions_DefaultGdprMode_IsNone()
    {
        var opts = new BLiteKvOptions();
        Assert.Equal(GdprMode.None, opts.DefaultGdprMode);
    }

    // ── Test 9: GdprModeAttribute carries the mode ───────────────────────────

    /// <summary>
    /// <see cref="GdprModeAttribute"/> must preserve the mode supplied at construction.
    /// </summary>
    [Fact]
    public void GdprModeAttribute_PreservesMode()
    {
        var attr = new GdprModeAttribute(GdprMode.Strict);
        Assert.Equal(GdprMode.Strict, attr.Mode);
    }

    // ── Test 9b: [GdprModeAttribute] is read by ModelBuilder.Entity<T>() ─────

    /// <summary>
    /// <see cref="GdprModeAttribute"/> placed on an entity class must be read by
    /// <see cref="ModelBuilder.Entity{T}"/> and seed <c>EntityTypeBuilder&lt;T&gt;.GdprMode</c>.
    /// Fluent <c>HasGdprMode()</c> called afterwards must override the attribute value.
    /// </summary>
    [Fact]
    public void ModelBuilder_Entity_ReadsGdprModeAttribute()
    {
        // AttributeEntity carries [GdprMode(Strict)] — verify the builder is seeded.
        var modelBuilder = new ModelBuilder();
        var builder = modelBuilder.Entity<AttributeEntity>();
        Assert.Equal(GdprMode.Strict, builder.GdprMode);

        // Fluent call must override the attribute.
        builder.HasGdprMode(GdprMode.None);
        Assert.Equal(GdprMode.None, builder.GdprMode);
    }

    /// <summary>Helper entity for the attribute-seeding test.</summary>
    [GdprMode(GdprMode.Strict)]
    private sealed class AttributeEntity { public int Id { get; set; } }

    // ── Test 9c: DocumentDbContext — [GdprModeAttribute] entity + no encryption → throw ─

    /// <summary>
    /// A context whose entity class carries <c>[GdprMode(GdprMode.Strict)]</c> must
    /// trigger the same encryption enforcement as the fluent <c>HasGdprMode(Strict)</c> path.
    /// </summary>
    [Fact]
    public void DbContext_EntityWithGdprModeAttribute_NoEncryption_Throws()
    {
        var path = TempDb();
        var ex = Assert.Throws<InvalidOperationException>(
            () => new AttributeEntityDbContext(path));
        Assert.Contains("GdprMode.Strict requires", ex.Message);
    }

    private sealed class AttributeEntityDbContext : DocumentDbContext
    {
        public AttributeEntityDbContext(string path) : base(path) { }
        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<AttributeEntity>(); // [GdprMode(Strict)] on the class
    }

    // ── Test 10: Strict + [PersonalData] collection without retention → warning ─

    /// <summary>
    /// Strict mode with a collection that has <c>[PersonalData]</c> fields but no
    /// retention policy must emit a warning with EventId
    /// <c>GdprStrictRetentionWarning</c> (9004).
    /// The test relies on the reflection-based <c>PersonalDataResolver</c> recognizing
    /// the <c>[PersonalData]</c> attribute on <see cref="GdprPerson.Email"/>.
    /// </summary>
    [Fact]
    public void GdprStrict_PersonalDataWithoutRetention_EmitsRetentionWarning()
    {
        const string collectionName = "gdprpersons";
        var path = TempDb();
        var crypto = new CryptoOptions("gdpr-strict-test-passphrase");
        var kvOpts = new BLiteKvOptions { DefaultGdprMode = GdprMode.Strict };

        // Create the collection in a non-Strict engine first so the catalog entry
        // exists when the Strict engine re-opens the same file.
        using (var seed = new BLiteEngine(path, crypto))
        {
            seed.GetOrCreateCollection(collectionName);
        }

        // Warm up the PersonalDataResolver collection cache for the collection.
        // ResolveByCollectionName uses the generated mapper (source-gen) or reflection.
        // GdprPerson.Email carries [PersonalData] so the cache should produce ≥1 field
        // after the assembly scan. We trigger the scan here to avoid timing issues.
        var fields = PersonalDataResolver.ResolveByCollectionName(collectionName);
        // The source-gen mapper for GdprPerson must exist (it is in BLite.Shared which
        // is referenced by BLite.Tests and always compiled).  Assert to catch regressions
        // rather than silently skipping the real assertion.
        Assert.NotEmpty(fields);

        using var engine = new BLiteEngine(path, crypto, kvOpts);

        Assert.True(HasWarning(GdprStrictValidator.GdprStrictRetentionWarning),
            "Expected GdprStrictRetentionWarning (9004) for a [PersonalData] collection without retention.");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DocumentDbContext path (WP3 — typed code-first surface)
    // ─────────────────────────────────────────────────────────────────────────

    // Minimal context subclasses used by the DocumentDbContext tests below.

    /// <summary>
    /// Plain context with no model configuration (engine-wide GdprMode from kvOptions).
    /// </summary>
    private sealed class PlainStrictDbContext : DocumentDbContext
    {
        public PlainStrictDbContext(string path, BLiteKvOptions? kvOptions = null)
            : base(path, kvOptions ?? BLiteKvOptions.Default) { }

        public PlainStrictDbContext(string path, CryptoOptions crypto, BLiteKvOptions? kvOptions = null)
            : base(path, crypto, kvOptions) { }
    }

    /// <summary>
    /// Context that sets <see cref="GdprMode.Strict"/> on <see cref="GdprPerson"/> via
    /// <see cref="EntityTypeBuilderGdprExtensions.HasGdprMode{T}"/> — no engine-wide default.
    /// </summary>
    private sealed class PerEntityStrictDbContext : DocumentDbContext
    {
        public PerEntityStrictDbContext(string path)
            : base(path) { }

        public PerEntityStrictDbContext(string path, CryptoOptions crypto)
            : base(path, crypto) { }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<GdprPerson>().HasGdprMode(GdprMode.Strict);
    }

    // ── Test 11: DocumentDbContext — GdprMode.None baseline ──────────────────

    /// <summary>
    /// A <see cref="DocumentDbContext"/> with no GDPR configuration must emit zero
    /// validator log lines (backwards-compatible baseline).
    /// </summary>
    [Fact]
    public void DbContext_GdprModeNone_ZeroLogLines()
    {
        var path = TempDb();
        using var ctx = new PlainStrictDbContext(path);

        for (int id = 9001; id <= 9007; id++)
            Assert.False(HasWarning(id), $"EventId {id} must NOT be emitted for GdprMode.None");
    }

    // ── Test 12: DocumentDbContext — engine-wide Strict + no encryption → throw ─

    /// <summary>
    /// <see cref="DocumentDbContext"/> constructed with
    /// <see cref="BLiteKvOptions.DefaultGdprMode"/> = Strict and no encryption must
    /// throw <see cref="InvalidOperationException"/> containing "GdprMode.Strict requires".
    /// </summary>
    [Fact]
    public void DbContext_EngineWideStrict_NoEncryption_Throws()
    {
        var path = TempDb();
        var kvOpts = new BLiteKvOptions { DefaultGdprMode = GdprMode.Strict };

        var ex = Assert.Throws<InvalidOperationException>(
            () => new PlainStrictDbContext(path, kvOpts));

        Assert.Contains("GdprMode.Strict requires", ex.Message);
    }

    // ── Test 13: DocumentDbContext — engine-wide Strict + encryption → starts ──

    /// <summary>
    /// <see cref="DocumentDbContext"/> with engine-wide Strict and encryption must start
    /// without throwing.
    /// </summary>
    [Fact]
    public void DbContext_EngineWideStrict_WithEncryption_DoesNotThrow()
    {
        var path = TempDb();
        var crypto = new CryptoOptions("ctx-strict-test");
        var kvOpts = new BLiteKvOptions { DefaultGdprMode = GdprMode.Strict };

        using var ctx = new PlainStrictDbContext(path, crypto, kvOpts);
        Assert.NotNull(ctx);
    }

    // ── Test 14: DocumentDbContext — per-entity Strict + no encryption → throw ─

    /// <summary>
    /// A context that sets <c>HasGdprMode(Strict)</c> on an entity type without encryption
    /// must throw, just as the engine-wide Strict path does.
    /// </summary>
    [Fact]
    public void DbContext_PerEntityStrict_NoEncryption_Throws()
    {
        var path = TempDb();

        var ex = Assert.Throws<InvalidOperationException>(
            () => new PerEntityStrictDbContext(path));

        Assert.Contains("GdprMode.Strict requires", ex.Message);
    }

    // ── Test 15: DocumentDbContext — per-entity Strict + encryption → starts ──

    /// <summary>
    /// A context with per-entity <c>HasGdprMode(Strict)</c> and encryption configured
    /// must start without throwing.
    /// </summary>
    [Fact]
    public void DbContext_PerEntityStrict_WithEncryption_DoesNotThrow()
    {
        var path = TempDb();
        var crypto = new CryptoOptions("ctx-per-entity-strict-test");

        using var ctx = new PerEntityStrictDbContext(path, crypto);
        Assert.NotNull(ctx);
    }

    // ── Test 16: DocumentDbContext — Strict emits audit-missing warning ────────

    /// <summary>
    /// Strict <see cref="DocumentDbContext"/> with encryption but no audit sink must
    /// emit <c>GdprStrictAuditMissing</c> (EventId 9002).
    /// </summary>
    [Fact]
    public void DbContext_Strict_NoAudit_EmitsAuditMissingWarning()
    {
        var path = TempDb();
        var crypto = new CryptoOptions("ctx-strict-audit-test");
        var kvOpts = new BLiteKvOptions { DefaultGdprMode = GdprMode.Strict };

        using var ctx = new PlainStrictDbContext(path, crypto, kvOpts);

        Assert.True(HasWarning(GdprStrictValidator.GdprStrictAuditMissing),
            "Expected GdprStrictAuditMissing (9002) from DocumentDbContext with no audit sink.");
    }
}
