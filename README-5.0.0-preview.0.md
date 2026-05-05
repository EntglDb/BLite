# ⚡ BLite — 5.0.0-preview.0

[![NuGet](https://img.shields.io/nuget/v/BLite?label=nuget&color=blue)](https://www.nuget.org/packages/BLite/5.0.0-preview.0)
[![NuGet Stable](https://img.shields.io/badge/stable-4.4.2-brightgreen)](https://www.nuget.org/packages/BLite/4.4.2)
[![Buy Me a Coffee](https://img.shields.io/badge/sponsor-Buy%20Me%20a%20Coffee-ffdd00?logo=buy-me-a-coffee&logoColor=black)](https://buymeacoffee.com/lucafabbriu)
![Build Status](https://img.shields.io/badge/build-passing-brightgreen)
![License](https://img.shields.io/badge/license-MIT-blue)
![Platform](https://img.shields.io/badge/platform-.NET%2010%20%7C%20netstandard2.1-purple)
![Status](https://img.shields.io/badge/status-preview-orange)

> [!WARNING]
> **This is a preview release.** APIs may change before the v5.0.0 GA release.  
> Current stable version: **[4.4.2](https://www.nuget.org/packages/BLite/4.4.2)** — documented in the [main README](README.md).
>
> **📅 Estimated GA: 30 May 2026.**  
> Please test this preview in non-production environments and [open a GitHub Issue](https://github.com/EntglDb/BLite/issues) for any bug, regression, or unexpected behaviour you encounter. Your feedback is essential to make the GA release solid.

---

## What's new in v5

BLite 5.0.0-preview.0 builds on the solid foundation of v4.4.2 and introduces three major feature pillars — **Encryption at Rest**, **Audit Trail**, and **GDPR Compliance Primitives** — together with a new **multi-process WAL** mode and a **generalized Retention Policy**. BLite Studio has been updated to expose all these features through a dedicated UI.

| Feature | Summary |
|:--------|:--------|
| 🔐 [Encryption at Rest](#-encryption-at-rest) | Transparent AES-256-GCM page-level encryption for `.db`, WAL, and all multi-file layouts |
| 🪵 [Audit Trail](#-audit-trail--performance-monitoring) | `IBLiteAuditSink` callbacks, in-memory `BLiteMetrics`, and OpenTelemetry `ActivitySource` |
| 🛡️ [GDPR Compliance Primitives](#-gdpr-compliance-primitives) | `[PersonalData]`, subject export, database inspection, CDC masking, and `GdprMode.Strict` |
| 🔄 [Multi-Process WAL](#-multi-process-wal-opt-in) | `.wal-shm` sidecar enables N-reader / 1-writer access across OS processes (opt-in) |
| ⏳ [Generalized Retention Policy](#-generalized-retention-policy) | Per-collection document expiry on any typed collection, not only `TimeSeries` |
| 🗑️ [Secure Erase & VACUUM](#️-secure-erase--vacuum-gdpr-art-17) | Slot-level data zeroing on delete for GDPR Art. 17 compliance |
| 🖥️ [BLite Studio](#️-blite-studio-updates) | Encryption unlock, GDPR inspection panel, and subject-data export built into the GUI |

---

## Install

```
dotnet add package BLite --version 5.0.0-preview.0
```

> If you are using `BLite.Bson` or `BLite.Caching` separately, update those packages to `5.0.0-preview.0` as well.

---

## 🔐 Encryption at Rest

BLite 5 adds **transparent page-level encryption** using **AES-256-GCM** — the industry standard for authenticated, tamper-evident encryption. Encryption is applied at the storage layer: pages are encrypted before writing to disk and decrypted after reading. The rest of the engine — LINQ, CDC, ACID transactions, the WAL — works unchanged.

### Threat model

| Threat | Mitigation |
|:-------|:-----------|
| Database file stolen from disk | AES-256-GCM encryption of every page |
| WAL file stolen | WAL records encrypted with a separate file-role nonce |
| Backup file exfiltrated | Same encryption applies; optional separate backup key |
| Single file stolen in multi-file layout | HKDF per-file subkeys — each file is encrypted with a different key |
| Tampered file opened | GCM authentication tag rejected → `CryptographicException` |
| Memory scraping | Out of scope (OS-level responsibility) |

### The typed approach: `DocumentDbContext` (recommended)

The typed API is the highest-level entry point and the preferred way to use BLite in application code. Inherit from `DocumentDbContext`, declare your collections as properties, and configure encryption in the constructor:

```csharp
// 1. Define your entity
public class User
{
    public ObjectId Id { get; set; }
    public string   Name  { get; set; } = "";
    public string   Email { get; set; } = "";
}

// 2. Define your context
public class AppDb : DocumentDbContext
{
    public DocumentCollection<ObjectId, User> Users { get; set; } = null!;

    public AppDb(string path, CryptoOptions crypto)
        : base(path, crypto) { }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<User>()
            .HasIndex(x => x.Email);
    }
}

// 3. Open the encrypted database — identical to opening a plain one
using var db = new AppDb("users.db", new CryptoOptions("my-secret-passphrase"));

// 4. CRUD operations are unchanged
await db.Users.InsertAsync(new User { Id = ObjectId.NewObjectId(), Name = "Alice", Email = "alice@example.com" });
await db.SaveChangesAsync();

var alice = await db.Users.AsQueryable()
    .Where(u => u.Email == "alice@example.com")
    .FirstOrDefaultAsync();
```

No change in application logic is required beyond the constructor. The Source Generator emits the same zero-allocation mapper code; the encryption/decryption layer sits below the mapper at the page-file boundary.

### `CryptoOptions` — two credential modes

```csharp
// Mode 1: Passphrase (PBKDF2-SHA256, 600 000 iterations per OWASP 2023)
//   ✔ Simple — suited for human-typed secrets and desktop/mobile apps
var opts = new CryptoOptions("my-secret-passphrase");

// Mode 2: Master-key bytes (HKDF-SHA256, multi-file server mode)
//   ✔ KMS-friendly — retrieve 32-byte key from Azure Key Vault, AWS KMS, HSM, etc.
//   ✔ Required for multi-file layouts where each file gets its own subkey
byte[] masterKey = await myKeyVault.GetKeyAsync("blite-prod");
var opts = CryptoOptions.FromMasterKey(masterKey);

// Pass to either BLiteEngine (dynamic) or DocumentDbContext (typed):
using var engine = new BLiteEngine("data.db", opts);
// —or—
using var db     = new AppDb("data.db", opts);
```

### Multi-file (server) mode

In multi-file mode each logical collection and index lives in its own physical file. The `EncryptionCoordinator` derives a **unique 256-bit subkey per file** using HKDF-SHA256 so that stealing one file does not expose the others:

```csharp
// Multi-file layout: collections stored in a separate directory
var config = new PageFileConfig
{
    CollectionDataDirectory = "data/collections",
    IndexFilePath           = "data/indexes",
    WalPath                 = "data/main.wal",
};

byte[] masterKey = await myKeyVault.GetKeyAsync("blite-prod");

using var db = new AppDb(
    "data/main.db",
    CryptoOptions.FromMasterKey(masterKey),
    kvOptions: null,
    baseConfig: config);
```

### Migration: plaintext → encrypted

```csharp
await BLiteEngine.MigrateToEncryptedAsync(
    existingPath: "old.db",
    newPath:      "new-encrypted.db",
    keyProvider:  myKeyProvider);

await BLiteEngine.MigrateToPlaintextAsync(
    existingPath: "old-encrypted.db",
    newPath:      "new-plain.db",
    keyProvider:  myKeyProvider);
```

Both migration helpers handle multi-file layouts automatically (all collection files, index files, and WAL are migrated together as an atomic unit).

### Key rotation

```csharp
await engine.RotateEncryptionKeyAsync(
    newKeyProvider: myKeyProvider,
    options: new KeyRotationOptions { EmitAuditEvent = true },
    ct: cancellationToken);
```

### Zero-overhead when disabled

The default `NullCryptoProvider` is a transparent no-op. When encryption is not configured, the JIT eliminates all crypto-related branches through dead-code elimination: **zero overhead for existing code that does not opt in**.

---

## 🪵 Audit Trail & Performance Monitoring

BLite 5 introduces a formal **audit trail layer** on top of the existing real-time metrics subsystem. It provides per-operation callbacks with timing, caller identity, and optional OpenTelemetry integration — all with zero overhead when not configured.

### The typed approach: `DocumentDbContext`

```csharp
public class AppDb : DocumentDbContext
{
    public DocumentCollection<ObjectId, Order> Orders { get; set; } = null!;

    public AppDb(string path) : base(path) { }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>().HasIndex(x => x.Status);
    }
}

// Configure the audit sink after construction
var db = new AppDb("orders.db");

db.ConfigureAudit(new BLiteAuditOptions
{
    Sink                  = new MyAuditSink(),        // your IBLiteAuditSink impl
    EnableMetrics         = true,                     // populate db.AuditMetrics
    SlowOperationThreshold = TimeSpan.FromMilliseconds(50), // emit SlowOperationEvent
    EnableDiagnosticSource = true,                    // emit Activity spans (OTel)
});
```

### `IBLiteAuditSink` — implement once, receive all events

```csharp
public class MyAuditSink : IBLiteAuditSink
{
    // Called after every insert
    public void OnInsert(InsertAuditEvent e)
    {
        Console.WriteLine($"[INSERT] {e.CollectionName} — {e.DocumentSizeBytes} B in {e.Elapsed.TotalMilliseconds:F2} ms");
    }

    // Called after every LINQ / BLQL query
    public void OnQuery(QueryAuditEvent e)
    {
        Console.WriteLine($"[QUERY]  {e.CollectionName} via {e.Strategy} — {e.ResultCount} rows in {e.Elapsed.TotalMilliseconds:F2} ms");
    }

    // Called after every transaction commit
    public void OnCommit(CommitAuditEvent e)
    {
        Console.WriteLine($"[COMMIT] txn {e.TransactionId} — {e.PagesWritten} pages in {e.Elapsed.TotalMilliseconds:F2} ms");
    }

    // Called when an operation exceeds SlowOperationThreshold
    public void OnSlowOperation(SlowOperationEvent e)
    {
        Console.WriteLine($"[SLOW]   {e.OperationType} on {e.CollectionName} took {e.Elapsed.TotalMilliseconds:F2} ms — {e.Detail}");
    }
    // All methods have default no-op implementations — override only what you need
}
```

All methods have default no-op implementations (C# 8+ default interface methods). Override only the events you care about.

### Caller identity

Inject a custom `IAuditContextProvider` to attach a user identity to every event — useful for per-user access logs in multi-tenant apps:

```csharp
public class HttpContextAuditProvider : IAuditContextProvider
{
    private readonly IHttpContextAccessor _ctx;
    public HttpContextAuditProvider(IHttpContextAccessor ctx) => _ctx = ctx;

    public string? GetCurrentUserId() => _ctx.HttpContext?.User?.Identity?.Name;
}

db.ConfigureAudit(new BLiteAuditOptions
{
    Sink            = new MyAuditSink(),
    ContextProvider = new HttpContextAuditProvider(httpContextAccessor),
});
```

### `BLiteMetrics` — in-memory counters

Enable metrics to get a lightweight, lock-free snapshot of database activity:

```csharp
db.ConfigureAudit(new BLiteAuditOptions { EnableMetrics = true });

// Anytime later — all reads use Interlocked, ~10-20 ns overhead
BLiteMetrics m = db.AuditMetrics!;

Console.WriteLine($"Inserts: {m.TotalInserts}");
Console.WriteLine($"Queries: {m.TotalQueries} (index: {m.TotalQueriesIndexScan}, full-scan: {m.TotalQueriesFullScan})");
Console.WriteLine($"Commits: {m.TotalCommits}");
Console.WriteLine($"Cache hit rate: {m.CacheHitRate:P1}");
Console.WriteLine($"Avg insert: {m.AvgInsertMs:F2} ms | Avg query: {m.AvgQueryMs:F2} ms");
```

### OpenTelemetry integration

Set `EnableDiagnosticSource = true` and register a standard OTEL listener. BLite emits `Activity` spans via `BLiteDiagnostics.ActivitySource` (source name: `BLite.Core`). Zero overhead when no listener is registered (`ActivitySource.HasListeners()` costs ~5 ns).

```csharp
// ASP.NET Core / OTEL SDK
services.AddOpenTelemetry()
    .WithTracing(b => b.AddSource("BLite.Core"));
```

### Zero-overhead guarantee

When `ConfigureAudit` is never called, the `_auditOptions` field is `null` and the JIT eliminates all audit branches through dead-code elimination. **Zero overhead for existing code.**

---

## 🛡️ GDPR Compliance Primitives

BLite is an embedded database — **the host application is always the data controller**. BLite's role is to provide the technical primitives that make compliance *possible* and *auditable*, without imposing policies.

> [!NOTE]
> BLite ships **primitives**, not certifications. The DPIA checklist in [docs/DPIA_CHECKLIST.md](docs/DPIA_CHECKLIST.md) helps your legal/DPO team map BLite features to GDPR obligations.

### The typed approach: annotate your entities

Mark personal data at compile time using `[PersonalData]`. The Source Generator picks up the annotation and emits the `PersonalDataFields` static member on the generated mapper — no reflection at runtime.

```csharp
using BLite.Core.GDPR;

public class Customer
{
    public ObjectId Id { get; set; }

    [PersonalData]                          // Art. 4(1): ordinary personal data
    public string Email { get; set; } = "";

    [PersonalData]                          // Art. 4(1)
    public string FullName { get; set; } = "";

    [PersonalData(Sensitivity = DataSensitivity.Sensitive)]   // financial / health data
    public string TaxCode { get; set; } = "";

    [PersonalData(Sensitivity = DataSensitivity.Special)]     // Art. 9 special categories
    public string? MedicalNotes { get; set; }

    // Timestamp used by the retention policy
    [PersonalData(IsTimestamp = true)]
    public DateTime CreatedAt { get; set; }

    public string Segment { get; set; } = "";  // not personal — never masked
}
```

Or configure via the fluent Fluent API in `OnModelCreating`:

```csharp
modelBuilder.Entity<Customer>()
    .Property(x => x.Email)
    .HasPersonalData(DataSensitivity.Personal);
```

### WP1 — Subject export (Art. 15 / Art. 20)

Produce a portable data report for a subject across the entire database in one call:

```csharp
public class AppDb : DocumentDbContext
{
    public DocumentCollection<ObjectId, Customer> Customers { get; set; } = null!;
    public DocumentCollection<ObjectId, Order>    Orders    { get; set; } = null!;
    // … other collections
}

using var db = new AppDb("app.db");

// Art. 15 / 20 — "Give me all data for alice@example.com"
var query = new SubjectQuery
{
    FieldName  = "email",
    FieldValue = BsonValue.FromString("alice@example.com"),
    Format     = SubjectExportFormat.Json,   // or Csv, Bson
};

await using SubjectDataReport report = await db.ExportSubjectDataAsync(query);

// Export to a portable file
await report.WriteToFileAsync("alice-export.json");

// Or stream directly to an HTTP response
await report.ExportAsJsonAsync(httpResponse.BodyWriter.AsStream());

// Inspect in-memory
foreach (var (collection, docs) in report.DataByCollection)
    Console.WriteLine($"{collection}: {docs.Count} document(s)");
```

The engine also exposes the same method as an extension on `BLiteEngine` for the schema-less path.

### WP1 — Database inspection (Art. 30)

Produce a record-of-processing snapshot suitable for DPO reporting:

```csharp
DatabaseInspectionReport report = db.InspectDatabase();

Console.WriteLine($"Path:         {report.DatabasePath}");
Console.WriteLine($"Encrypted:    {report.IsEncrypted}");      // AES-256-GCM enabled?
Console.WriteLine($"Audit active: {report.IsAuditEnabled}");   // IBLiteAuditSink registered?
Console.WriteLine($"Layout:       {(report.IsMultiFileMode ? "Multi-file" : "Single-file")}");

foreach (CollectionInfo col in report.Collections)
{
    Console.WriteLine($"\nCollection: {col.Name}");
    Console.WriteLine($"  Documents:     {col.DocumentCount}");
    Console.WriteLine($"  Storage:       {col.StorageSizeBytes / 1024} KB");
    Console.WriteLine($"  Personal data: {string.Join(", ", col.PersonalDataFields)}");
    if (col.RetentionPolicy is { } rp)
        Console.WriteLine($"  Retention:     MaxAge={rp.MaxAge}, Trigger={rp.Triggers}");
}
```

### WP2 — CDC Field Masking (Art. 5(1)(c))

The Change Data Capture channel is GDPR-safe by default: when `CapturePayload = true`, every field annotated with `[PersonalData]` is automatically masked before the event is dispatched to subscribers. Consumers must explicitly opt in to receive personal data in clear.

```csharp
// Default — GDPR-safe: personal-data fields masked with "***"
using var sub = db.Customers.Watch(new WatchOptions { CapturePayload = true })
    .Subscribe(e =>
    {
        // e.Entity.Email   → "***"
        // e.Entity.FullName → "***"
        // e.Entity.Segment  → "premium"  (not personal — delivered in clear)
        ProcessChange(e);
    });

// Opt in to receive personal data in clear (e.g. for a privileged audit service)
using var privilegedSub = db.Customers.Watch(new WatchOptions
{
    CapturePayload    = true,
    RevealPersonalData = true,   // explicit consent from consuming code
})
.Subscribe(e => FullAuditLog(e));

// Replace the mask value (e.g. drop the field entirely instead of masking)
using var dropSub = db.Customers.Watch(new WatchOptions
{
    CapturePayload        = true,
    PersonalDataMaskValue = BsonValue.Null,  // drop rather than mask
})
.Subscribe(e => DownstreamPipeline(e));

// Allowlist: deliver exactly these fields, regardless of personal-data status
using var allowlistSub = db.Customers.Watch(new WatchOptions
{
    CapturePayload  = true,
    IncludeOnlyFields = ["id", "segment", "createdAt"],  // wins over all masking rules
})
.Subscribe(e => Analytics(e));

// Blocklist: exclude specific additional fields on top of personal-data masking
using var blockSub = db.Customers.Watch(new WatchOptions
{
    CapturePayload = true,
    ExcludeFields  = ["taxCode", "medicalNotes"],
})
.Subscribe(e => ProcessChange(e));
```

**Masking precedence (applied in order on a deep clone of the payload):**
1. If `IncludeOnlyFields` is set → allowlist wins; all other rules are skipped.
2. Else if `RevealPersonalData = false` → replace each `[PersonalData]` field with `PersonalDataMaskValue` (or drop if `BsonValue.Null`).
3. Then apply `ExcludeFields` blocklist.

### WP3 — `GdprMode.Strict` (Art. 25 — Privacy by Default)

`GdprMode.Strict` enforces a privacy-by-design configuration profile at engine open time. Misconfigured databases fail fast rather than silently.

Configure per-entity via the fluent API:

```csharp
public class AppDb : DocumentDbContext
{
    public DocumentCollection<ObjectId, Customer> Customers { get; set; } = null!;

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Customer>()
            .HasGdprMode(GdprMode.Strict)           // enforce for this collection
            .HasRetentionPolicy(
                c => c.CreatedAt,
                maxAge: TimeSpan.FromDays(3 * 365)); // 3-year retention
    }
}
```

Or apply declaratively via the `[GdprMode]` attribute:

```csharp
[GdprMode(GdprMode.Strict)]
public class Customer { /* … */ }
```

Or set an engine-wide default:

```csharp
var db = new AppDb("app.db", new BLiteKvOptions
{
    DefaultGdprMode = GdprMode.Strict,
});
```

When `GdprMode.Strict` is active, the validator runs at engine open and:

| Condition | Behaviour |
|:----------|:----------|
| Encryption absent | **Throws** `InvalidOperationException` |
| No audit sink registered | Emits a `TraceWarning` |
| `[PersonalData]` collection has no retention policy | Emits a `TraceWarning` |
| `SecureEraseOnDelete` not enforceable | Emits a `TraceWarning` |

Strict mode never deletes data, rotates keys, or modifies stored documents on its own.

Full example combining encryption + audit + strict mode:

```csharp
// Configure a fully GDPR-hardened database
var db = new AppDb(
    "app.db",
    new CryptoOptions("production-passphrase"),
    new BLiteKvOptions { DefaultGdprMode = GdprMode.Strict });

db.ConfigureAudit(new BLiteAuditOptions
{
    Sink          = new FileAuditSink("audit.jsonl"),
    EnableMetrics = true,
});

// Engine open succeeds only if all Strict requirements are met
// → encryption: ✔  audit sink: ✔  retention on personal-data collections: ✔
```

---

## 🔄 Multi-Process WAL (opt-in)

v5 introduces a `.wal-shm` sidecar file to enable **N-reader / 1-writer access** from multiple OS processes on the same database file — following the same principles as SQLite's WAL-mode SHM, adapted to BLite's sequential WAL format.

This feature is **opt-in** — all existing single-process behaviour is preserved unchanged.

```csharp
var config = new PageFileConfig
{
    EnableMultiProcessAccess = true,
};

// Process A (writer)
using var db = new AppDb("shared.db", config);

// Process B (reader — opened concurrently in a separate process)
using var db = new AppDb("shared.db", config);
```

> [!IMPORTANT]
> Multi-process WAL requires all cooperating processes to open the database with `EnableMultiProcessAccess = true`. A process that opens with `EnableMultiProcessAccess = false` will hold `FileShare.None` and block other processes.

---

## ⏳ Generalized Retention Policy

Retention policies — previously available only on `TimeSeries` collections — can now be applied to **any typed collection**:

```csharp
protected override void OnModelCreating(ModelBuilder modelBuilder)
{
    // Typed collection: keep orders for 7 years (statutory requirement)
    modelBuilder.Entity<Order>()
        .HasRetentionPolicy(
            timestampSelector: o => o.PlacedAt,
            maxAge:            TimeSpan.FromDays(7 * 365),
            triggers:          RetentionTrigger.OnInsert | RetentionTrigger.Scheduled);

    // Typed collection: keep at most 10 000 log entries
    modelBuilder.Entity<AuditLogEntry>()
        .HasRetentionPolicy(
            timestampSelector: e => e.CreatedAt,
            maxDocumentCount:  10_000);
}
```

Pruning fires transparently on insert (configurable threshold) or on a scheduled basis — no background threads are created.

---

## 🗑️ Secure Erase & VACUUM (GDPR Art. 17)

When a document is deleted, BLite can now **zero the slot** on disk before marking it as free. This satisfies GDPR Art. 17 ("Right to Erasure") requirements where physical deletion — not just logical unlinking — is required.

```csharp
// Enable secure erase per-collection in OnModelCreating
modelBuilder.Entity<Customer>()
    .HasSecureErase(true);

// Run VACUUM to reclaim space and compact the file
await engine.VacuumAsync();
// —or typed—
await db.VacuumAsync();
```

---

## 🖥️ BLite Studio Updates

BLite Studio (the desktop GUI for database exploration) ships with v5 with three new capabilities:

### Encrypted database support

The connection dialog now exposes an **Encryption** section. Enter the passphrase before opening and Studio handles the rest transparently:

- The encryption status badge (`AES-256-GCM enabled` / `Not encrypted`) is shown in the title bar.
- The database layout (`Single-file` / `Multi-file`) is also displayed for multi-file databases.

### GDPR inspection panel

A dedicated **GDPR** sidebar item exposes the Art. 30 inspection report directly inside Studio:

- **Encryption status** — whether AES-256-GCM is active.
- **Audit status** — whether a sink is registered.
- **Layout** — single-file or multi-file.
- **Collections table** — per-collection document count, storage size, personal-data fields, and retention policy.

### Subject-data export (Art. 15 / 20)

The GDPR panel also includes a built-in **Subject Export** form:
1. Enter the field name (e.g. `email`) and field value (e.g. `alice@example.com`).
2. Choose the output format: JSON, CSV, or BSON.
3. Click **Export** and choose a destination file.

Studio calls `BLiteEngine.ExportSubjectDataAsync` internally and shows the number of exported documents on completion.

---

## Migration guide from v4.4.2

v5.0.0-preview.0 is **fully backwards-compatible** with v4.4.2 databases. No file-format changes are required and no API breaking changes have been introduced for existing code.

| Scenario | Action |
|:---------|:-------|
| Open existing unencrypted database | No change required — `NullCryptoProvider` is the default |
| Enable encryption on an existing database | Use `BLiteEngine.MigrateToEncryptedAsync(...)` |
| Add audit trail | Call `ConfigureAudit(...)` after construction |
| Add GDPR annotations | Annotate properties with `[PersonalData]` and rebuild |
| Enable Strict mode | Add `HasGdprMode(GdprMode.Strict)` + configure encryption + audit |
| Enable multi-process access | Set `PageFileConfig.EnableMultiProcessAccess = true` on all processes |

---

## API surface summary

### New types in `BLite.Core.Encryption`

| Type | Description |
|:-----|:------------|
| `ICryptoProvider` | Interface for pluggable encryption providers |
| `AesGcmCryptoProvider` | Built-in AES-256-GCM implementation |
| `NullCryptoProvider` | No-op provider (default — zero overhead) |
| `EncryptionCoordinator` | Per-file HKDF-SHA256 subkey derivation for multi-file mode |
| `CryptoOptions` | Configuration: passphrase mode or master-key mode |
| `KdfAlgorithm` | `Pbkdf2Sha256` (default, 600 000 iterations) |

### New types in `BLite.Core.Audit`

| Type | Description |
|:-----|:------------|
| `IBLiteAuditSink` | Interface with `OnInsert`, `OnQuery`, `OnCommit`, `OnSlowOperation` |
| `BLiteAuditOptions` | Configuration: sink, metrics, slow threshold, DiagnosticSource |
| `BLiteMetrics` | In-memory thread-safe counters (`TotalInserts`, `TotalQueries`, `CacheHitRate`, …) |
| `BLiteDiagnostics` | `ActivitySource` for OpenTelemetry / Application Insights |
| `IAuditContextProvider` | Plug-in for injecting caller identity into audit events |
| `AmbientAuditContext` | Default `IAuditContextProvider` backed by a static ambient context |
| `InsertAuditEvent` | Per-insert event record |
| `QueryAuditEvent` | Per-query event record (`CollectionName`, `Strategy`, `ResultCount`, `Elapsed`) |
| `CommitAuditEvent` | Per-commit event record (`TransactionId`, `PagesWritten`, `WalSizeBytes`, `Elapsed`) |
| `SlowOperationEvent` | Emitted when an operation exceeds `SlowOperationThreshold` |

### New types in `BLite.Core.GDPR`

| Type | Description |
|:-----|:------------|
| `PersonalDataAttribute` | Marks a property as personal data; `Sensitivity` and `IsTimestamp` |
| `DataSensitivity` | `Personal` (Art. 4(1)), `Sensitive` (financial/health), `Special` (Art. 9) |
| `PersonalDataField` | Record struct emitted by source generator on each mapper |
| `SubjectQuery` | Describes an Art. 15/20 lookup (`FieldName`, `FieldValue`, `Collections`, `Format`) |
| `SubjectDataReport` | Result of `ExportSubjectDataAsync`; supports JSON/CSV/BSON export |
| `SubjectExportFormat` | `Json`, `Csv`, `Bson` |
| `DatabaseInspectionReport` | Art. 30 snapshot: encryption, audit, layout, collections |
| `CollectionInfo` | Per-collection document count, size, personal fields, retention |
| `IndexInfo` | Per-index name, fields, uniqueness, encryption flag |
| `RetentionPolicyInfo` | Read-only projection of the active `RetentionPolicy` |
| `GdprMode` | `None` (default), `Strict` (Art. 25) |
| `GdprModeAttribute` | Declarative per-entity `[GdprMode(GdprMode.Strict)]` |
| `GdprEngineExtensions` | `ExportSubjectDataAsync`, `InspectDatabase` on `BLiteEngine` |
| `GdprDocumentDbContextExtensions` | Same surface on `DocumentDbContext` |

### `WatchOptions` additions (CDC Field Masking — WP2)

| Property | Default | Description |
|:---------|:--------|:------------|
| `RevealPersonalData` | `false` | When `false`, `[PersonalData]` fields are masked before dispatch |
| `PersonalDataMaskValue` | `"***"` | Replacement value; set to `BsonValue.Null` to drop the field |
| `ExcludeFields` | `[]` | Additional fields to remove from the payload |
| `IncludeOnlyFields` | `null` | Allowlist: when set, only these fields are delivered (wins over all masking rules) |

### New `BLiteEngine` / `DocumentDbContext` members

| Member | Description |
|:-------|:------------|
| `ConfigureAudit(BLiteAuditOptions)` | Attaches the audit trail subsystem |
| `AuditMetrics` | `BLiteMetrics?` — `null` until `EnableMetrics = true` |
| `new BLiteEngine(path, CryptoOptions, ...)` | Encryption-aware constructor |
| `new MyDbContext(path, CryptoOptions, ...)` | Encryption-aware constructor (typed) |
| `VacuumAsync(ct)` | Compact the database and reclaim free space |
| `RotateEncryptionKeyAsync(IKeyProvider, ...)` | Online key rotation |
| `BLiteEngine.MigrateToEncryptedAsync(...)` | Offline migration: plaintext → encrypted |
| `BLiteEngine.MigrateToPlaintextAsync(...)` | Offline migration: encrypted → plaintext |

---

## Known limitations in this preview

- `RotateEncryptionKeyAsync` is implemented but not yet stress-tested under concurrent write load. Use it during a maintenance window.
- Multi-process WAL (`EnableMultiProcessAccess`) is not yet supported on WASM/Browser targets (tracked in [WASM_SUPPORT.md](WASM_SUPPORT.md)).
- Argon2id KDF is reserved for a future release. PBKDF2-SHA256 (600 000 iterations) and HKDF-SHA256 are the only supported KDFs in this preview.
- Source-generator GDPR metadata emission (`PersonalDataFields`) requires .NET SDK 9+ (Roslyn 4.x). Fallback reflection path is fully functional on all supported runtimes.

---

## Feedback

📢 **We need your testing!**

The GA release is planned for **30 May 2026**. Before then, we ask the community to:

1. Install `5.0.0-preview.0` in a non-production environment.
2. Run your existing test suites — the migration is backwards-compatible.
3. Optionally enable encryption, the audit sink, or GDPR annotations and report your experience.
4. **[Open a GitHub Issue](https://github.com/EntglDb/BLite/issues)** for any bug, performance regression, or unexpected API behaviour you find.

Please include in your issue report:
- BLite version (`5.0.0-preview.0`)
- .NET runtime and target framework
- Minimal reproducible code or test case
- Expected vs. actual behaviour

Thank you for helping make v5 the best BLite release yet. 🙏

---

## Resources

- **[Main README](README.md)** — full feature documentation for the current stable release (v4.4.2)
- **[CHANGELOG.md](CHANGELOG.md)** — detailed commit-level change log
- **[RFC.md](RFC.md)** — full architectural specification (storage engine, WAL, ACID, query processing)
- **[roadmap/v5/ENCRYPTION_PLAN.md](roadmap/v5/ENCRYPTION_PLAN.md)** — encryption architecture details
- **[roadmap/v5/AUDIT_IMPLEMENTATION.md](roadmap/v5/AUDIT_IMPLEMENTATION.md)** — audit trail design
- **[roadmap/v5/GDPR_PLAN.md](roadmap/v5/GDPR_PLAN.md)** — GDPR compliance architecture
- **[docs/DPIA_CHECKLIST.md](docs/DPIA_CHECKLIST.md)** — DPIA checklist for DPO use
- **[Official Documentation → blitedb.com/docs](https://blitedb.com/docs/getting-started)**
