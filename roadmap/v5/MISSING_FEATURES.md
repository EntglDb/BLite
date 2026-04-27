# BLite — Development Plan: Missing Features

> Date: April 27, 2026  
> References: [AUDIT_IMPLEMENTATION.md](AUDIT_IMPLEMENTATION.md), [RFC.md](../../RFC.md), [StorageEngine.Recovery.cs](../../src/BLite.Core/Storage/StorageEngine.Recovery.cs), [DocumentCollection.cs](../../src/BLite.Core/Collections/DocumentCollection.cs)

---

## Overview

Source code analysis has identified the following functional areas that are absent or incomplete in the BLite core. The plan is organized by priority, with effort estimates and dependencies.

---

## 1. Audit Trail Module

### Current state
`src/BLite.Core/Metrics/` is **already implemented** and provides:
- `MetricsDispatcher` — lock-free channel-based event aggregator, zero overhead on the hot path
- `MetricsSnapshot` — per-collection counters: `InsertCount`, `UpdateCount`, `DeleteCount`, `FindCount`, `QueryCount`, plus average latencies (µs)
- Global transaction counters: `TransactionBeginsTotal`, `TransactionCommitsTotal`, `TransactionRollbacksTotal`, `CheckpointsTotal`
- `BLiteMetricsObservable` — push-based `IObservable<MetricsSnapshot>` via `WatchMetrics(interval)`

What is **still missing** is the **formal audit sink layer**, designed in [AUDIT_IMPLEMENTATION.md](AUDIT_IMPLEMENTATION.md):
- `src/BLite.Core/Audit/` directory does not exist
- No `IBLiteAuditSink` interface (individual event callbacks with `userId`, `documentId`)
- No persistent audit log (metrics are in-memory counters, reset on restart)
- No slow-query/slow-commit detection
- No `ActivitySource` / `DiagnosticSource` integration (OpenTelemetry traces)

The two layers serve distinct purposes:

| Capability | Metrics (done) | Audit Sink (missing) |
|-----------|---------------|---------------------|
| Per-operation callback | ❌ | ✅ IBLiteAuditSink |
| Aggregated counters | ✅ MetricsSnapshot | ❌ |
| Latency tracking | ✅ AvgXxxLatencyUs | ✅ per-event ElapsedMicros |
| Caller identity (userId) | ❌ | ✅ via IAuditContextProvider (app-injected, never engine-derived) |
| DocumentId | ❌ | ✅ |
| Persistent log | ❌ (in-memory) | ✅ FileAuditSink (JSONL) |
| Slow-operation alert | ❌ | ✅ SlowQueryThreshold |
| OpenTelemetry traces | ❌ | ✅ ActivitySource (Phase 2) |

### Goal
Implement the `Audit/` layer on top of the existing `Metrics/` infrastructure, following the plan in [AUDIT_IMPLEMENTATION.md](AUDIT_IMPLEMENTATION.md).

### Files to create (per AUDIT_IMPLEMENTATION.md)
| File | Purpose |
|------|---------|
| `src/BLite.Core/Audit/IBLiteAuditSink.cs` | Sink interface with default no-op methods |
| `src/BLite.Core/Audit/AuditEvents.cs` | Record types: `CommitAuditEvent`, `InsertAuditEvent`, `QueryAuditEvent`, `SlowOperationEvent` |
| `src/BLite.Core/Audit/BLiteAuditOptions.cs` | Configuration (sink, `SlowQueryThreshold`, `EnableDiagnosticSource`) |
| `src/BLite.Core/Audit/BLiteMetrics.cs` | In-memory counters (Phase 1) |
| `src/BLite.Core/Audit/BLiteDiagnostics.cs` | `ActivitySource` + `DiagnosticSource` (Phase 2) |

### Hooks to add
Three chokepoints per the existing design:
- `StorageEngine.CommitTransaction` → `CommitAuditEvent`
- `DocumentCollection.InsertDataCore` → `InsertAuditEvent`
- `BTreeQueryProvider.Execute<TResult>` → `QueryAuditEvent`

All hooks are guarded by `_auditOptions is null` so the JIT eliminates the branch when audit is not configured (zero-overhead principle already used in the Metrics layer).

### On `userId` in an embedded, in-process database

BLite is **not a server-based database**. Unlike MySQL or PostgreSQL, there is no network socket, no authentication handshake, and no session management. Access control is entirely handled by the OS: whoever can open the file path (via filesystem permissions) is the only "user" the engine can observe — and that is the OS process identity (`Environment.UserName`), not the application-level user.

This distinction matters:

| Context | Who is the "user"? | Known to BLite? |
|---------|-------------------|----------------|
| Desktop app | The logged-in OS user | ✅ via `Environment.UserName` |
| ASP.NET Core web app | The HTTP request principal (e.g. `claims["sub"]`) | ❌ — BLite has no HTTP context |
| Background service | A service account (`SYSTEM`, `www-data`) | ✅ but meaningless for audit |
| WASM / browser | The browser's sandbox identity | ❌ |
| Mobile (MAUI) | The device user | Partially |

In a web scenario — the most common case where audit identity actually matters — the OS process runs as a single service account (e.g. `www-data`) serving thousands of application users concurrently. **BLite cannot distinguish between them.** Only the application knows which authenticated principal is performing the current operation.

This is the same problem faced by EF Core (`ICurrentUserService`), SQLite (no auth at all), and LiteDB (no auth). The correct solution is **not** for the database engine to derive the user, but to provide a hook through which the application can inject it.

#### Recommended pattern: `IAuditContextProvider`

```csharp
// src/BLite.Core/Audit/IAuditContextProvider.cs

/// <summary>
/// Implemented by the host application to supply the current caller identity
/// to the BLite audit layer.
///
/// BLite has no authentication layer of its own — access is governed solely
/// by OS filesystem permissions on the .db file path. This interface allows
/// the application to correlate database operations with its own security
/// context (e.g. an HTTP request principal, a background job identifier,
/// or a desktop session user).
/// </summary>
public interface IAuditContextProvider
{
    /// <summary>
    /// Returns the identifier of the caller performing the current operation,
    /// or <c>null</c> if no identity is available in the current execution context.
    /// Called synchronously on the hot path — must be fast and non-blocking.
    /// </summary>
    string? GetCurrentUserId();
}
```

A built-in implementation backed by `AsyncLocal<string?>` covers the common case where the application sets the identity at the start of a request or unit of work:

```csharp
// Built-in: src/BLite.Core/Audit/AmbientAuditContext.cs
public static class AmbientAuditContext
{
    private static readonly AsyncLocal<string?> _userId = new();

    /// <summary>
    /// Sets the current user identity for all BLite audit events
    /// emitted on this async execution context.
    /// </summary>
    public static string? CurrentUserId
    {
        get => _userId.Value;
        set => _userId.Value = value;
    }
}

// Default provider used when no custom IAuditContextProvider is configured:
internal sealed class AmbientAuditContextProvider : IAuditContextProvider
{
    public string? GetCurrentUserId() => AmbientAuditContext.CurrentUserId;
}
```

**ASP.NET Core usage example:**
```csharp
// In a middleware or action filter — set once per request:
AmbientAuditContext.CurrentUserId = httpContext.User.FindFirst("sub")?.Value;
```

**Desktop / MAUI usage example:**
```csharp
// On login:
AmbientAuditContext.CurrentUserId = Environment.UserName;
```

If the application supplies no context and uses no `IAuditContextProvider`, `userId` is `null` in audit events — which is correct and not an error. The audit record still captures `collection`, `operation`, `documentId`, `timestamp`, and `success`, which are fully known by the engine.

### Key `IBLiteAuditSink` interface
```csharp
public interface IBLiteAuditSink
{
    void OnInsert(InsertAuditEvent e)         { }  // default no-op
    void OnQuery(QueryAuditEvent e)           { }
    void OnCommit(CommitAuditEvent e)         { }
    void OnSlowOperation(SlowOperationEvent e){ }  // Phase 2
}
```

`InsertAuditEvent` and `QueryAuditEvent` include `string? UserId` populated by calling `IAuditContextProvider.GetCurrentUserId()` at emission time — never sourced internally by the engine.

### Priority: HIGH  
### Estimate: 3–5 days (infrastructure is in place; hooks + sink implementation only)

---

## 2. Complete Backup in Multi-file Mode

### Current state
`StorageEngine.Recovery.cs` L120–122 explicitly documents:
> "In multi-file mode, only the main .db file is backed up. Index files and collection files are NOT included."

`BLiteEngine.BackupAsync` calls only `_pageFile.BackupAsync()`, which copies the main file only.

### Goal
Guarantee a consistent hot backup of all database files in multi-file mode: main file, separate index files, separate collection files, and WAL.

### Required changes

**`StorageEngine.Recovery.cs`**
```
BackupAsync(string targetPath, CancellationToken ct)
  → iterate over all registered PageFiles
  → quiesce writes (or use WAL snapshot) during copy
  → copy in order: WAL flush → main file → collection files → index files
  → generate JSON manifest with SHA-256 checksum per file
```

**`BLiteEngine.BackupAsync`**
- Accept `BackupOptions` (include/exclude indexes, compression, destination path pattern)
- Emit audit events `BackupStarted` / `BackupCompleted`

**Manifest format**
```json
{
  "version": 1,
  "timestamp": "2026-04-27T10:00:00Z",
  "files": [
    { "name": "mydb.db",        "size": 1048576, "sha256": "..." },
    { "name": "mydb.col0.db",   "size": 204800,  "sha256": "..." },
    { "name": "mydb.idx0.db",   "size": 81920,   "sha256": "..." }
  ]
}
```

### Priority: HIGH  
### Estimate: 3–5 days

---

## 3. Secure Erase / VACUUM

### Current state
- `DocumentCollection.DeleteCore` compacts the page via `CompactPage`, freeing the slot, but deleted bytes remain in the physical file until reused or compacted.
- `FreePage()` in `PageFile.cs` rewrites the page with a new buffer (page-level partial wipe), but does not guarantee zero-fill of deleted slots within a partially occupied page.
- README L952: "freed bytes are reusable without a VACUUM pass."

### Goal
Provide explicit mechanisms for secure data erasure:

#### 3a. Slot-level secure erase
During `DeleteCore`, after `CompactPage`, overwrite the freed slot bytes with zeros before returning the page to the pool.

#### 3b. VACUUM command
An explicit command that:
1. Rewrites every page copying only live documents
2. Zero-fills all unused bytes in the file
3. Truncates the file to its minimum required size
4. Supports `VACUUM collection_name` or global VACUUM

**Proposed API**
```csharp
// On BLiteEngine
Task VacuumAsync(VacuumOptions? options = null, CancellationToken ct = default);

// On IDocumentCollection<T>
Task VacuumAsync(CancellationToken ct = default);
```

**`VacuumOptions`**
```csharp
public sealed class VacuumOptions
{
    public bool SecureErase { get; init; } = true;   // zero-fill freed bytes
    public bool TruncateFile { get; init; } = true;
    public bool RebuildIndexes { get; init; } = false;
}
```

### Priority: MEDIUM (HIGH for GDPR scenarios)  
### Estimate: 4–6 days

---

## 4. Generalized Retention Policy

### Current state
`StorageEngine.TimeSeries.cs` implements `RetentionPolicyMs` only for TimeSeries collections (automatic pruning on insert). Regular collections have no automatic retention.

### Goal
Extend retention policy to all collections with configurable triggers:

| Trigger | Description |
|---------|-------------|
| `MaxDocumentCount` | Delete oldest documents when count exceeds N |
| `MaxAgeMs` | Delete documents older than the specified age |
| `MaxSizeBytes` | Delete oldest documents when file size exceeds threshold |
| `Scheduled` | Periodic execution on a configurable schedule |

**Proposed API**
```csharp
builder.HasRetentionPolicy(policy => policy
    .MaxAge(TimeSpan.FromDays(365))
    .MaxDocumentCount(100_000)
    .OnField(x => x.CreatedAt)   // timestamp field to use
    .TriggerOn(RetentionTrigger.OnInsert | RetentionTrigger.Scheduled)
);
```

### Priority: MEDIUM  
### Estimate: 3–4 days

---

## 5. Security Metrics and Observability

### Current state
`MetricsDispatcher` and `MetricsSnapshot` are **already implemented** and fully operational (per-collection operation counters, latencies, transaction and checkpoint counters, pull via `GetMetrics()` and push via `WatchMetrics()`). The module must be explicitly enabled — it is opt-in with zero overhead when disabled.

What is missing are **security-specific counters** not yet present in `MetricsSnapshot`:

| Metric | Type | Status |
|--------|------|--------|
| `InsertsTotal`, `UpdatesTotal`, `DeletesTotal`, `QueriesTotal` | Counter | ✅ Done |
| `TransactionCommitsTotal`, `TransactionRollbacksTotal` | Counter | ✅ Done |
| `CheckpointsTotal`, `AvgCheckpointLatencyUs` | Counter / Avg | ✅ Done |
| `audit.events.total` | Counter per event type | ❌ Missing |
| `security.failed_queries` | Counter (queries rejected by BLQL hardening) | ❌ Missing |
| `storage.vacuum.last_run` | Timestamp | ❌ Missing |
| `storage.vacuum.bytes_freed` | Gauge | ❌ Missing |
| `backup.last_success` | Timestamp | ❌ Missing |
| `backup.duration_ms` | Histogram | ❌ Missing |

### Goal
Extend `MetricsSnapshot` with the missing security counters and optionally expose them via `System.Diagnostics.Metrics` (OpenTelemetry-compatible `Meter`/`Counter<T>` API).

### Priority: LOW  
### Estimate: 1–2 days (base infrastructure already exists)

---

## 6. Data Subject Export (Right of Access)

> See also [GDPR_PLAN.md](GDPR_PLAN.md) §3.

API to extract all documents associated with a data subject identified by a key (e.g. `userId`):

```csharp
IAsyncEnumerable<BsonDocument> ExportSubjectDataAsync(
    string fieldName,
    BsonValue subjectId,
    ExportFormat format = ExportFormat.Json,
    CancellationToken ct = default
);
```

### Priority: MEDIUM (mandatory for GDPR)  
### Estimate: 2 days

---

## 7. Truncate and Drop Collection

### Current state

**`BLiteEngine.DropCollection(string name)`** exists (L205) but is incomplete:
```
// comment in source: "Physical page cleanup is deferred."
_collections.TryRemove(name, out var collection)
collection.Dispose()
_storage.DeleteCollectionMetadata(name)   // removes catalog entry only
// ← pages are NOT freed in single-file mode
// ← DropCollectionFile() is NOT called in multi-file mode
```

`StorageEngine.Memory.cs` already has `DropCollectionFile(string)` which removes the dedicated file in multi-file mode — it is simply not called from `BLiteEngine.DropCollection`.

**`TruncateCollection`** does not exist anywhere in the codebase.

**`DocumentDbContext` / `IDocumentDbContext`** expose neither operation.

**`IDocumentCollection<TId, T>`** has no `TruncateAsync()` method.

---

### Semantics

| Operation | Behaviour |
|-----------|-----------|
| **Truncate** | Deletes all documents and rebuilds empty indexes. Collection metadata, schema, and the collection registration itself are preserved. The collection is immediately usable after truncate. |
| **Drop** | Removes all documents, indexes, metadata, and the collection registration. Physical storage is freed synchronously (multi-file: file deleted; single-file: pages marked free). After drop, any cached reference to the collection must not be used. |

---

### Changes required

#### 7a. `IDocumentCollection<TId, T>` — add `TruncateAsync`

```csharp
// src/BLite.Core/Collections/IDocumentCollection.cs

/// <summary>
/// Deletes all documents in the collection and rebuilds empty indexes.
/// The collection structure (schema, indexes, metadata) is preserved.
/// </summary>
/// <returns>Number of documents deleted.</returns>
Task<int> TruncateAsync(CancellationToken ct = default);
Task<int> TruncateAsync(ITransaction? transaction, CancellationToken ct = default);
```

`Drop` is intentionally **not** added to `IDocumentCollection<TId, T>`: after dropping, the collection reference becomes invalid. Drop is a container-level operation (engine / context).

#### 7b. `BLiteEngine` — fix `DropCollection`, add `TruncateCollectionAsync`

```csharp
// Fix existing DropCollection — make physical cleanup complete:
public bool DropCollection(string name)
{
    ThrowIfDisposed();
    if (!_collections.TryRemove(name, out var collection)) return false;

    collection.Dispose();
    _storage.DeleteCollectionMetadata(name);
    _storage.DropCollectionFile(name);          // no-op in single-file mode
    // single-file mode: FreeCollectionPages(name) — marks all collection pages as free
    return true;
}

// New: truncate
public Task<int> TruncateCollectionAsync(
    string name,
    CancellationToken ct = default);
```

#### 7c. `IDocumentDbContext` — add typed overloads

```csharp
// src/BLite.Core/IDocumentDbContext.cs

/// <summary>
/// Drops the collection registered for entity type <typeparamref name="T"/>.
/// After this call, Set&lt;T&gt;() throws <see cref="InvalidOperationException"/>.
/// </summary>
Task DropCollectionAsync<T>(CancellationToken ct = default) where T : class;

/// <summary>
/// Deletes all documents in the collection for <typeparamref name="T"/> and
/// rebuilds empty indexes. The collection remains registered and usable.
/// </summary>
Task<int> TruncateCollectionAsync<T>(CancellationToken ct = default) where T : class;
```

#### 7d. `DocumentDbContext` — implement and handle invalidated references

After `DropCollectionAsync<T>()` is called, the strongly-typed property (e.g. `db.Users`) still holds a reference to the disposed `DocumentCollection<TId, T>`. The implementation must:

1. Dispose the underlying `DocumentCollection<TId, T>` instance
2. Free physical storage (calls engine-level drop)
3. Unregister the type from the internal collection registry
4. Replace the live reference with a `DroppedCollectionProxy<TId, T>` that throws `InvalidOperationException("Collection 'Users' has been dropped.")` on any method call

The proxy approach avoids null-reference exceptions at the call site while giving a clear, actionable error message.

#### 7e. Single-file mode — `FreeCollectionPages`

In single-file mode, `DropCollectionFile` is a no-op. A new `StorageEngine` method is needed:

```csharp
// Marks all pages owned by the named collection as free,
// making them immediately available for reuse by the page allocator.
void FreeCollectionPages(string collectionName);
```

This is distinct from VACUUM (§3): it marks pages free immediately without rewriting the file. Actual space reclamation on disk still requires a VACUUM pass, but the page allocator can reuse the freed pages right away.

---

### Priority: HIGH  
### Estimate: 3–4 days

---

## Summary Roadmap

| # | Feature | Priority | Estimate | Dependencies |
|---|---------|----------|----------|-------------|
| 1 | Audit Sink (IBLiteAuditSink + hooks) | HIGH | 3–5 d | Metrics ✅ |
| 2 | Multi-file backup | HIGH | 3–5 d | — |
| 3 | Secure Erase / VACUUM | MEDIUM→HIGH | 4–6 d | — |
| 4 | Generalized Retention Policy | MEDIUM | 3–4 d | — |
| 5 | Security Metrics (extend MetricsSnapshot) | LOW | 1–2 d | Metrics ✅ |
| 6 | Data Subject Export | MEDIUM | 2 d | #1 |
| 7 | Truncate & Drop Collection (complete) | HIGH | 3–4 d | — |

**Total estimate: 19–28 days**

---

## Related files
- [AUDIT_IMPLEMENTATION.md](AUDIT_IMPLEMENTATION.md)
- [RFC.md](../../RFC.md)
- [ENCRYPTION_PLAN.md](ENCRYPTION_PLAN.md)
- [GDPR_PLAN.md](GDPR_PLAN.md)
