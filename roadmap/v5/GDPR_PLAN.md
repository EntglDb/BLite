# BLite — GDPR Compliance Plan (v5)

> **Audience:** AI coding agents and human contributors implementing the GDPR compliance feature set.  
> **Status:** Authoritative spec. All GDPR GitHub issues link back to a section of this file.  
> **Last revised:** May 1, 2026 (post-Encryption refactor).  
> **References:** Regulation (EU) 2016/679 (GDPR), [ENCRYPTION_FIX_PLAN.md](ENCRYPTION_FIX_PLAN.md), [AUDIT_IMPLEMENTATION.md](AUDIT_IMPLEMENTATION.md), [MISSING_FEATURES.md](MISSING_FEATURES.md).

---

## 0. Why this document exists

The Encryption feature was originally split into many narrow ShapeUp pitches. Each pitch was self-contained, but no single artifact described the **shared architecture, ownership rules, and cross-cutting invariants**. As a result, agentic implementations drifted (public types that should have been internal, missing `IDisposable` plumbing, ad-hoc `EncryptionCoordinator` arguments leaking into user-facing APIs) and a P14 refactor was required.

This plan exists to prevent the same failure mode for GDPR. It defines, **before any code is written**:

- The single canonical namespace and file layout.
- All shared types, with ownership and lifetime.
- The dependency graph between work packages.
- Hard "do-not-touch" boundaries.
- Per-WP acceptance criteria that are testable.

Every GDPR issue **must** reference §2–§4 of this document and stay within those contracts. Any deviation requires updating this document **first**.

---

## 1. Scope and non-scope

### What BLite must provide

BLite is an embedded, in-process database. Under GDPR the **data controller is the host application**, never the database engine. BLite's role is to provide **technical primitives** that make compliance possible and auditable:

1. A way to mark domain fields as personal data (compile-time metadata).
2. A way to extract all data belonging to a single subject (Art. 15, 20).
3. A way to inspect the database surface for compliance reporting (Art. 30).
4. A way to suppress PII leakage through the change-data-capture (CDC) channel (Art. 5(1)(c)).
5. A safe-default configuration profile that wires the above with encryption + audit + retention + secure erase (Art. 25).
6. A DPIA-oriented documentation deliverable (Art. 35).

### What BLite must **not** do

These are explicit **anti-goals**. An agent that proposes any of the following has misread the plan:

| Anti-goal | Reason |
|---|---|
| Decide what counts as "personal data" at runtime | Only the application can; BLite uses the `[PersonalData]` annotation as a hint. |
| Auto-delete data based on detected PII | Erasure is an application decision; BLite exposes primitives, never policies. |
| Encrypt individual BSON fields | Out of scope. Page-level AES-GCM is the only encryption surface. |
| Implement a new audit module inside `BLite.Core/GDPR/` | Audit lives in `BLite.Core/Audit/` (see AUDIT_IMPLEMENTATION.md). GDPR consumes it. |
| Wrap or re-export `ICryptoProvider`, `EncryptionCoordinator`, or `IBLiteAuditSink` from `BLite.Core/GDPR/` | These types belong to their owning modules. GDPR observes them via existing public/internal APIs only. |
| Add a "compliance certification" claim anywhere in code or docs | BLite ships primitives, not certifications. The DPIA doc ships a legal disclaimer. |
| Modify `EncryptionCoordinator`, `AesGcmCryptoProvider`, `CryptoOptions`, `WriteAheadLog`, or any `Storage/*` file as part of GDPR work | Encryption is feature-frozen post-P14. GDPR is purely a consumer. |

---

## 2. Architectural invariants (apply to **every** work package)

These rules are non-negotiable. Each WP must satisfy them before merge.

### 2.1 Canonical namespace and folder

All new GDPR types live in:

```
src/BLite.Core/GDPR/
└── (one file per public type, PascalCase = filename)
```

- **Namespace:** `BLite.Core.GDPR`
- **Public types:** annotated with one-paragraph XMLDoc explaining who creates and disposes them.
- **No partial types across files.** Each public type is in exactly one file.
- **No re-exports.** GDPR types do not surface `ICryptoProvider`, `IBLiteAuditSink`, or any storage-internal type in their public signatures.

### 2.2 Ownership and lifetime

| Type kind | Created by | Owned by | Disposed by |
|---|---|---|---|
| `SubjectQuery`, `SubjectDataReport` | host app or `BLiteEngine` extension | host app | host app (`SubjectDataReport : IDisposable` if it holds streams) |
| `DatabaseInspectionReport`, `CollectionInfo` | `BLiteEngine.InspectDatabase()` | host app | not disposable (pure data record) |
| `PersonalDataAttribute` | source-gen consumed at compile time | runtime metadata only | n/a |
| `GdprMode` (enum) | host app via `EntityTypeBuilder<T>.HasGdprMode(...)` (per-collection, canonical surface) and optionally `BLiteKvOptions.DefaultGdprMode` (engine-wide default) | engine | engine |

**Rule:** If a GDPR type holds an open file or a cryptographic secret, it **must** implement `IDisposable` and zeroize on dispose.

### 2.3 Source-generator contract

The v5 source generator pipeline already exists (`EntityAnalyzer` → `MapperGenerator` → `CodeGenerator` in [`src/BLite.SourceGenerators/`](../../src/BLite.SourceGenerators/)) and discovers attributes + fluent ModelBuilder calls. **WP1 extends this same pipeline; it does not introduce a parallel metadata infrastructure.**

The generator is taught two new inputs:

1. **Attribute path:** properties annotated with `[PersonalData]` (see §4.1).
2. **Fluent path:** `modelBuilder.Entity<T>().Property(x => x.Email).HasPersonalData(DataSensitivity.Personal)` (additive extension on `PropertyBuilder<T,TProp>`).

The generator emits, **on the already-generated mapper class for each entity**, two static read-only members:

```csharp
// Augmentation of an existing generated mapper class (illustrative — WP1 final shape
// must match the existing generated-class naming and accessor conventions in MapperGenerator/CodeGenerator):
public static partial class XxxMapper // existing generated class for entity Xxx
{
    public static global::System.Collections.Generic.IReadOnlyList<
        global::BLite.Core.GDPR.PersonalDataField> PersonalDataFields { get; } = new[]
    {
        new global::BLite.Core.GDPR.PersonalDataField("Email",   global::BLite.Core.GDPR.DataSensitivity.Personal,  IsTimestamp: false),
        new global::BLite.Core.GDPR.PersonalDataField("Religion", global::BLite.Core.GDPR.DataSensitivity.Special,  IsTimestamp: false),
    };

    public static string? PersonalDataTimestampField { get; } = null;
}
```

WP1 ships the supporting public type:

```csharp
namespace BLite.Core.GDPR;

public readonly record struct PersonalDataField(
    string PropertyName,
    DataSensitivity Sensitivity,
    bool IsTimestamp);
```

**Reflection fallback:** for entity types that are **not** routed through the BLite source generator (dynamic collections, third-party POCOs registered ad-hoc), `internal static class PersonalDataMetadataCache` resolves the same shape (`IReadOnlyList<PersonalDataField>` + nullable timestamp field) by scanning `[PersonalData]` reflectively and caching per `Type`. This is the **only** fallback path; consumers (Subject Export, Inspection) call a single internal resolver `PersonalDataResolver.Resolve(Type entityType)` that prefers source-gen and falls back to reflection.

### 2.4 Cross-feature integration points (read-only consumption)

GDPR consumes — never modifies — these existing surfaces:

| Consumed surface | Used by | Read-only contract |
|---|---|---|
| `BLiteEngine.IsEncryptionEnabled` (or equivalent) | WP1 Inspection, WP3 Strict | bool flag derived from `CryptoOptions != null` and a non-`NullCryptoProvider` provider |
| `BLiteEngine.GetCollectionNames()` | WP1 Subject Export, Inspection | enumeration of registered collection names |
| `IBLiteAuditSink` (`BLite.Core.Audit`, scope of issue #83) | WP3 Strict | resolved from the engine; **WP3 does not implement a sink** — if #83 has not yet shipped, WP3 logs `GdprStrictAuditModuleAbsent` and continues |
| `WatchOptions` (`BLite.Core.CDC`) | WP2 | extended in-place; CDC dispatcher applies the mask |
| `RetentionPolicy` (`BLite.Core.Retention`, [`Retention/RetentionPolicy.cs`](../../src/BLite.Core/Retention/RetentionPolicy.cs)) — already shipped, configured per-collection via `EntityTypeBuilder<T>.HasRetentionPolicy(...)` | WP1 Inspection (projects into `RetentionPolicyInfo`), WP3 Strict (warns if missing on personal-data collections) | inspected for warnings and projected read-only; never modified by GDPR code |

If a consumed surface does not yet exist (e.g., audit sink, retention), the WP **must** degrade gracefully: log a single startup warning naming the missing dependency, then continue. It must **not** throw, and it must **not** create a stand-in implementation of the missing module.

### 2.5 Threading and async

- All public IO methods are async with `CancellationToken ct = default`.
- All collection scans use `IAsyncEnumerable<BsonDocument>` — never `List<BsonDocument>` materialization, except in the final consumer when serialization requires it.
- Subject export must work on a database under concurrent writes (snapshot semantics inherited from `DocumentCollection.QueryAsync`).

### 2.6 Error handling

- **No new exception types** unless absolutely required. Reuse `InvalidOperationException`, `ArgumentException`, `OperationCanceledException`.
- Strict-mode misconfiguration throws `InvalidOperationException` with a message of the form: `"GdprMode.Strict requires <feature>. Configure <option> or set GdprMode = GdprMode.None."`.
- Subject export over a missing field returns an empty report — never throws.

### 2.7 Test coverage gates

Every WP ships with tests in `tests/BLite.Tests/Gdpr/`:

- WP1: ≥ 15 tests covering attribute discovery, subject query roundtrip, inspection of empty/populated/encrypted databases.
- WP2: ≥ 8 tests covering personal-data default masking (the GDPR-safe default), opt-in `RevealPersonalData`, custom `PersonalDataMaskValue` (including `BsonValue.Null` = drop), `ExcludeFields`, `IncludeOnlyFields` (allowlist wins, allowlisted personal-data fields delivered in clear), and dynamic-stream no-op.
- WP3: ≥ 6 tests covering strict-mode startup throw, warn, and successful happy path.
- WP4: not applicable (docs).

The full test suite (`dotnet test BLite.slnx -c Release`) must pass after each WP merges.

---

## 3. Namespace and file layout (canonical, exhaustive)

The complete set of files introduced by the GDPR plan, by WP. Agents must not create files outside this list without updating this section first.

### WP1 — Foundation

| Path | Type kind | Defined in |
|---|---|---|
| `src/BLite.Core/GDPR/PersonalDataAttribute.cs` | `[AttributeUsage(AttributeTargets.Property)] public sealed class PersonalDataAttribute` | WP1 |
| `src/BLite.Core/GDPR/DataSensitivity.cs` | `public enum DataSensitivity : byte { Personal = 1, Sensitive = 2, Special = 3 }` | WP1 |
| `src/BLite.Core/GDPR/PersonalDataField.cs` | `public readonly record struct PersonalDataField(string PropertyName, DataSensitivity Sensitivity, bool IsTimestamp)` (see §2.3) | WP1 |
| `src/BLite.Core/GDPR/PersonalDataResolver.cs` | `internal static class` — single resolver: source-gen first (looks up the generated mapper's static `PersonalDataFields` member via a generated registry hook), reflection fallback otherwise | WP1 |
| `src/BLite.Core/GDPR/PersonalDataMetadataCache.cs` | `internal static class` — reflection-fallback cache, `ConcurrentDictionary<Type, IReadOnlyList<PersonalDataField>>` | WP1 |
| `src/BLite.Core/Metadata/PropertyBuilderExtensions.Gdpr.cs` | `public static class` — `HasPersonalData<T,TProp>(this PropertyBuilder<T,TProp>, DataSensitivity = Personal, bool isTimestamp = false)` fluent extension recognised by the source generator | WP1 |
| `src/BLite.Core/GDPR/SubjectQuery.cs` | `public sealed class SubjectQuery` | WP1 |
| `src/BLite.Core/GDPR/SubjectDataReport.cs` | `public sealed class SubjectDataReport : IAsyncDisposable` | WP1 |
| `src/BLite.Core/GDPR/SubjectExportFormat.cs` | `public enum SubjectExportFormat : byte { Json = 1, Csv = 2, Bson = 3 }` | WP1 |
| `src/BLite.Core/GDPR/GdprEngineExtensions.cs` | `public static class` — `ExportSubjectDataAsync`, `InspectDatabase` | WP1 |
| `src/BLite.Core/GDPR/DatabaseInspectionReport.cs` | `public sealed record DatabaseInspectionReport` | WP1 |
| `src/BLite.Core/GDPR/CollectionInfo.cs` | `public sealed record CollectionInfo` | WP1 |
| `src/BLite.Core/GDPR/IndexInfo.cs` | `public sealed record IndexInfo` | WP1 |
| `src/BLite.Core/GDPR/RetentionPolicyInfo.cs` | `public sealed record RetentionPolicyInfo` — **read-only projection** of the existing `BLite.Core.Retention.RetentionPolicy`, see §4.3 | WP1 |
| `src/BLite.SourceGenerators/Gdpr/PersonalDataAnalyzer.cs` | `internal static class` — extends `EntityAnalyzer` to harvest `[PersonalData]` and the fluent `HasPersonalData(...)` model-builder call | WP1 |
| `src/BLite.SourceGenerators/Gdpr/PersonalDataEmitter.cs` | `internal static class` — extends `CodeGenerator` to emit the `PersonalDataFields` static member on the existing generated mapper class | WP1 |
| `tests/BLite.Tests/Gdpr/PersonalDataAttributeTests.cs` | xUnit tests | WP1 |
| `tests/BLite.Tests/Gdpr/SubjectExportTests.cs` | xUnit tests | WP1 |
| `tests/BLite.Tests/Gdpr/DatabaseInspectionTests.cs` | xUnit tests | WP1 |

### WP2 — CDC Field Masking

| Path | Change |
|---|---|
| `src/BLite.Core/CDC/WatchOptions.cs` | **modify** — add `ExcludeFields`, `IncludeOnlyFields`, `RevealPersonalData` (default `false`), `PersonalDataMaskValue` (default `"***"`) |
| `src/BLite.Core/CDC/ChangeStreamObservable.cs` | **modify** — capture the resolved personal-data field list at `Watch(...)` time and forward the masking policy to the dispatcher |
| `src/BLite.Core/CDC/DynamicChangeStreamObservable.cs` | **modify** — same plumbing for dynamic streams; resolved personal-data list is empty (untyped) |
| `src/BLite.Core/CDC/ChangeStreamDispatcher.cs` | **modify** — apply the four-step masking pipeline (§4.4) when capturing payload |
| `src/BLite.Core/GDPR/PayloadMask.cs` | new `internal static class` — pure BSON projection helpers (`Allowlist`, `Blocklist`, `MaskPersonalData`) |
| `tests/BLite.Tests/Gdpr/CdcMaskingTests.cs` | new — ≥ 8 tests covering personal-data default mask, opt-in reveal, allowlist override, custom mask value (incl. `BsonValue.Null` = drop), exclude, dynamic-stream no-op |

### WP3 — GdprMode.Strict

| Path | Change |
|---|---|
| `src/BLite.Core/GDPR/GdprMode.cs` | new `public enum GdprMode : byte { None = 0, Strict = 1 }` |
| `src/BLite.Core/GDPR/GdprModeAttribute.cs` | new `[AttributeUsage(AttributeTargets.Class)] public sealed class GdprModeAttribute(GdprMode mode) : Attribute` — for code-first per-entity configuration |
| `src/BLite.Core/Metadata/EntityTypeBuilder.Gdpr.cs` | new — adds `EntityTypeBuilder<T>.HasGdprMode(GdprMode mode)` fluent method (mirrors `HasRetentionPolicy`); persists on the existing `EntityTypeBuilder<T>` in `Metadata/EntityTypeBuilder.cs` (additive only) |
| `src/BLite.Core/KeyValue/BLiteKvOptions.cs` | **modify** — add `public GdprMode DefaultGdprMode { get; init; } = GdprMode.None;` (engine-wide default; per-collection wins when set) |
| `src/BLite.Core/GDPR/GdprStrictValidator.cs` | new `internal static class` — invoked at engine open, **per collection**, after the catalog is loaded |
| `tests/BLite.Tests/Gdpr/GdprStrictTests.cs` | new |

### WP4 — DPIA documentation

| Path | Change |
|---|---|
| `docs/DPIA_CHECKLIST.md` | new |

---

## 4. Public API contract (binding for all WPs)

This section is the **single normative copy** of every public signature introduced by the plan. Issues quote from here verbatim.

### 4.1 `[PersonalData]`

```csharp
namespace BLite.Core.GDPR;

[AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
public sealed class PersonalDataAttribute : Attribute
{
    /// <summary>Sensitivity tier. Default: Personal (Art. 4(1)).</summary>
    public DataSensitivity Sensitivity { get; init; } = DataSensitivity.Personal;

    /// <summary>True if this property holds the timestamp eligible for retention/MaxAge policies.</summary>
    public bool IsTimestamp { get; init; }
}

public enum DataSensitivity : byte
{
    Personal = 1,    // Art. 4(1): name, email, address
    Sensitive = 2,   // health, financial, SSN
    Special = 3      // Art. 9 special categories: race, religion, biometrics, …
}
```

### 4.2 Subject access (Art. 15) and portability (Art. 20)

```csharp
namespace BLite.Core.GDPR;

public sealed class SubjectQuery
{
    public required string FieldName { get; init; }
    public required BsonValue FieldValue { get; init; }
    public IReadOnlyList<string>? Collections { get; init; }
    public SubjectExportFormat Format { get; init; } = SubjectExportFormat.Json;
}

public sealed class SubjectDataReport : IAsyncDisposable
{
    public DateTimeOffset GeneratedAt { get; init; }
    public BsonValue SubjectId { get; init; }
    public IReadOnlyDictionary<string, IReadOnlyList<BsonDocument>> DataByCollection { get; init; }

    public Task WriteToFileAsync(string path, CancellationToken ct = default);
    public Task WriteToStreamAsync(Stream stream, CancellationToken ct = default);

    public Task ExportAsJsonAsync(Stream output, CancellationToken ct = default);
    public Task ExportAsCsvAsync(Stream output, string collection, CancellationToken ct = default);
    public Task ExportAsBsonAsync(Stream output, CancellationToken ct = default);

    public ValueTask DisposeAsync();
}

public static class GdprEngineExtensions
{
    public static Task<SubjectDataReport> ExportSubjectDataAsync(
        this BLiteEngine engine,
        SubjectQuery query,
        CancellationToken ct = default);

    public static DatabaseInspectionReport InspectDatabase(this BLiteEngine engine);
}
```

### 4.3 Database inspection (Art. 30)

```csharp
namespace BLite.Core.GDPR;

public sealed record DatabaseInspectionReport(
    string DatabasePath,
    bool IsEncrypted,
    bool IsAuditEnabled,
    bool IsMultiFileMode,
    IReadOnlyList<CollectionInfo> Collections);

public sealed record CollectionInfo(
    string Name,
    long DocumentCount,
    long StorageSizeBytes,
    IReadOnlyList<IndexInfo> Indexes,
    IReadOnlyList<string> PersonalDataFields,
    RetentionPolicyInfo? RetentionPolicy);

public sealed record IndexInfo(string Name, IReadOnlyList<string> Fields, bool IsUnique, bool IsEncrypted);
/// <summary>Read-only projection of the existing <see cref="BLite.Core.Retention.RetentionPolicy"/> for inspection reports.</summary>
public sealed record RetentionPolicyInfo(
    string? TimestampField,
    TimeSpan? MaxAge,
    long? MaxDocumentCount,
    long? MaxSizeBytes,
    string Triggers); // RetentionTrigger.ToString() (e.g. "OnInsert, Scheduled")
```

### 4.4 CDC field masking (Art. 5(1)(c))

```csharp
// Modified type, additive only:
namespace BLite.Core.CDC;

public sealed class WatchOptions
{
    /// <summary>When false (default), CDC delivers metadata only. When true, the dispatcher captures the BSON payload and applies the masking rules below.</summary>
    public bool CapturePayload { get; init; } = false;

    /// <summary>
    /// When true and <see cref="CapturePayload"/> is also true, fields annotated <see cref="BLite.Core.GDPR.PersonalDataAttribute"/>
    /// (or registered via <c>HasPersonalData(...)</c>) are emitted **in clear**. When false (default), every personal-data field of the
    /// watched collection's entity type is replaced with <see cref="PersonalDataMaskValue"/> on the cloned payload before dispatch.
    /// This is the GDPR-safe default: the consumer must explicitly opt in to see personal data on the change stream.
    /// </summary>
    public bool RevealPersonalData { get; init; } = false;

    /// <summary>
    /// Replacement value used for personal-data fields when <see cref="RevealPersonalData"/> is false.
    /// Default: <c>BsonValue.FromString("***")</c>. Set to <see cref="BsonValue.Null"/> to drop instead of mask.
    /// </summary>
    public BsonValue PersonalDataMaskValue { get; init; } = BsonValue.FromString("***");

    /// <summary>Extra keys to remove from the cloned payload, in addition to the personal-data masking. Ignored when <see cref="IncludeOnlyFields"/> is non-null.</summary>
    public IReadOnlyList<string> ExcludeFields { get; init; } = Array.Empty<string>();

    /// <summary>If non-null, the dispatched payload contains **only** these keys (allowlist). Wins over <see cref="ExcludeFields"/> and over the personal-data masking: if a personal-data field is on this allowlist it will be emitted in clear regardless of <see cref="RevealPersonalData"/>.</summary>
    public IReadOnlyList<string>? IncludeOnlyFields { get; init; }
}
```

**Masking rules (apply in this order, on a clone of the payload, only when `CapturePayload == true`):**

1. If `IncludeOnlyFields` is non-null: produce a new `BsonDocument` containing only those keys (allowlist wins; both `ExcludeFields` and personal-data masking are skipped — the consumer has explicitly stated which fields they want, and that statement is authoritative).
2. Else if `RevealPersonalData == false` and the watched collection has resolvable personal-data metadata (`PersonalDataResolver.Resolve(entityType)` returns a non-empty list): for each personal-data property name `P`, replace `clone[P]` with `PersonalDataMaskValue` (or remove the key if `PersonalDataMaskValue` is `BsonValue.Null`). Field-name resolution mirrors the existing mapper (the BSON key generated by the source generator / reflection fallback, **not** the CLR property name).
3. Then, if `ExcludeFields` is non-empty: remove those keys from the cloned document.
4. Else (after rules 2 and 3 have run, or if rule 2 did not apply): deliver the cloned document.

For **dynamic / untyped collections** (no registered entity type, e.g. `DynamicChangeStreamObservable`), there is no personal-data metadata to resolve — rule 2 is a no-op and the consumer is expected to use `ExcludeFields` / `IncludeOnlyFields` explicitly. The dispatcher logs a single `info`-level event the first time a dynamic collection is watched with `CapturePayload = true` and `RevealPersonalData = false` to make this fact discoverable.

If `CapturePayload == false`, all four masking fields are irrelevant and CDC dispatch behaves exactly as today (metadata-only event).

**Default summary (the GDPR-safe path):** `new WatchOptions { CapturePayload = true }` — without setting anything else — masks every `[PersonalData]` field of the watched typed collection automatically. The user must say `RevealPersonalData = true` (or list the field in `IncludeOnlyFields`) to see clear values.

### 4.5 GdprMode.Strict (Art. 25)

```csharp
namespace BLite.Core.GDPR;

public enum GdprMode : byte { None = 0, Strict = 1 }

[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = true)]
public sealed class GdprModeAttribute : Attribute
{
    public GdprMode Mode { get; }
    public GdprModeAttribute(GdprMode mode) => Mode = mode;
}
```

**Canonical configuration surface (per-collection, mirrors `HasRetentionPolicy`):**

```csharp
namespace BLite.Core.Metadata;

public static class EntityTypeBuilderGdprExtensions
{
    public static EntityTypeBuilder<T> HasGdprMode<T>(
        this EntityTypeBuilder<T> builder,
        global::BLite.Core.GDPR.GdprMode mode) where T : class;
}
```

**Engine-wide default (used only when neither `[GdprMode]` nor `HasGdprMode(...)` is set on a collection):**

```csharp
// Additive change to BLite.Core.KeyValue.BLiteKvOptions:
public GdprMode DefaultGdprMode { get; init; } = GdprMode.None;
```

Resolution order at engine open, per collection: `EntityTypeBuilder<T>.HasGdprMode` → `[GdprMode]` attribute on the entity type → `BLiteKvOptions.DefaultGdprMode` → `GdprMode.None`.

**Strict-mode validation table (invoked at engine ctor, after all wiring):**

| Invariant | Standard | Strict | On violation in Strict |
|---|---|---|---|
| Encryption configured (`CryptoOptions != null` and provider is not `NullCryptoProvider`) | optional | required | throw `InvalidOperationException` |
| Audit sink registered (`IBLiteAuditSink` resolvable — scope of issue #83) | optional | required | if #83 has shipped and a sink is not configured, log a warning and continue (do not auto-construct a sink — sink defaults are owned by #83); if #83 has not shipped, log `GdprStrictAuditModuleAbsent` once and continue |
| `WatchOptions.CapturePayload` default | `false` | `false` (unchanged) | n/a — but any explicit `true` in strict mode logs a warning |
| Secure erase on delete | off | on | force `SecureEraseOnDelete = true`; log info |
| Retention policy on collections with `[PersonalData]` fields | optional | warned | for each missing, log a single warning at startup |

Strict mode never deletes existing data, never rotates keys, never modifies stored documents. It only validates configuration and logs once at startup.

---

## 5. Dependency graph

```
WP1 (Foundation: PersonalData + Subject Export + Inspection)
  ├── consumes: BLiteEngine.GetCollectionNames, IsEncryptionEnabled
  ├── consumes (optional): IBLiteAuditSink (for IsAuditEnabled flag only)
  └── consumes (optional): RetentionPolicy registry

WP2 (CDC Masking)
  └── independent — modifies CDC only

WP3 (GdprMode.Strict)
  ├── depends on WP1 (PersonalData metadata for retention warnings)
  ├── depends on WP2 (CapturePayload-immutability hook)
  ├── consumes: encryption module (must be configured)
  ├── consumes: audit module (issue #83 — IBLiteAuditSink); graceful if #83 not yet shipped
  └── consumes (graceful): VACUUM / SecureErase (MISSING_FEATURES §3)

WP4 (DPIA docs)
  └── depends on WP1, WP2, WP3 (documents what they ship)
```

**Implementation order:** WP1 → WP2 (parallel-safe with WP1) → WP3 → WP4. WP3 must not be merged before WP1 + WP2 because it references their public surface.

---

## 6. Work packages — detailed specs

### WP1 — Foundation: `[PersonalData]` + Subject Export + Database Inspection

**GDPR articles:** 4(1), 5(1)(c), 15, 20, 25, 30.  
**GitHub issue:** rewrite of #89 (consolidates the closed #91 and #92).  
**Estimate:** 4–6 days.

#### A. Tasks (in order)

1. Add the attribute and enum (§4.1) under `BLite.Core.GDPR/`. Add `PersonalDataField` record struct (§2.3).
2. Add `PropertyBuilderExtensions.Gdpr.cs` (`HasPersonalData<T,TProp>(...)`) so the fluent path mirrors `HasConversion`/`HasIndex` already supported by the source generator.
3. Add `PersonalDataResolver` (single resolver) and `PersonalDataMetadataCache` (reflection fallback, thread-safe, per-`Type` cached). Resolver order: generated-mapper static accessor → reflection over `[PersonalData]`.
4. Extend the existing source generator (do not create a new `[Generator]` class): `PersonalDataAnalyzer` augments `EntityAnalyzer` to harvest `[PersonalData]` + the fluent `HasPersonalData` call (the same mechanism `MapperGenerator` already uses for `HasConversion`/`HasIndex`); `PersonalDataEmitter` augments `CodeGenerator` to emit `PersonalDataFields` and `PersonalDataTimestampField` on the **existing generated mapper class** for each entity.
5. Add `SubjectQuery`, `SubjectExportFormat`, `SubjectDataReport` (§4.2).
6. Implement `BLiteEngine.ExportSubjectDataAsync` extension:
   - Resolve target collections: `query.Collections ?? engine.GetCollectionNames()`.
   - For each collection, attempt indexed lookup on `query.FieldName` first; fall back to `BLiteEngine.FindAsync(collectionName, predicate, ct)` (existing `IAsyncEnumerable<BsonDocument>` primitive — see [`BLiteEngine.cs`](../../src/BLite.Core/BLiteEngine.cs)). For typed collections obtained through `GetCollection<TKey,T>()`, use `DocumentCollection<TKey,T>.ScanAsync` / `FindAllAsync`. **No new scan helper is added.**
   - Stream results into a per-collection `IAsyncEnumerable<BsonDocument>` and aggregate into `DataByCollection`.
   - Honor `ct` at every async hop.
   - Empty result is a valid report — never throw.
7. Add `DatabaseInspectionReport`, `CollectionInfo`, `IndexInfo`, `RetentionPolicyInfo` (§4.3). `RetentionPolicyInfo` is built by **projecting** the existing `BLite.Core.Retention.RetentionPolicy` instance returned by the catalog (`MaxAgeMs → MaxAge`, `MaxDocumentCount`, `MaxSizeBytes`, `TimestampField`, `Triggers.ToString()`) — **no duplication of state**.
8. Implement `BLiteEngine.InspectDatabase`:
   - Walk the catalog without doing IO beyond what `Catalog.Snapshot` already does.
   - `IsEncrypted = engine has CryptoOptions and provider != NullCryptoProvider`.
   - `IsAuditEnabled = audit sink registered` (resolved from the engine's audit hook; `false` if issue #83 has not yet shipped).
   - `PersonalDataFields` resolved per collection via `PersonalDataResolver.Resolve(entityType)` (source-gen first, reflection fallback).
   - `RetentionPolicy` populated by projecting the existing per-collection `RetentionPolicy` (or `null` if none configured).
   - `StorageSizeBytes` in multi-file mode includes all sibling files; in single-file mode is the page count belonging to the collection × page size.
9. Add three test files (§3 WP1 row).

#### B. Acceptance criteria

- A model with `[PersonalData] string Email` and `[PersonalData(Sensitivity = Special)] string Religion` exposes `PersonalDataFields = ["Email", "Religion"]` and `SensitiveFields = ["Religion"]` after a clean build.
- `ExportSubjectDataAsync` over a 3-collection database returns documents only from collections containing the target field/value; unrelated collections appear with empty lists.
- `ExportAsJsonAsync` produces valid UTF-8 JSON, schema `{ "generatedAt": ISO-8601, "subjectId": …, "data": { "<col>": [ … ] } }`.
- `ExportAsCsvAsync` for a single collection emits header + one row per document; nested BSON values are JSON-stringified into the cell.
- `ExportAsBsonAsync` emits a length-prefixed BLite-portable stream re-importable by `BLiteEngine.ImportAsync` (or, if no import API exists yet, by reading the documents back via the BSON reader — a roundtrip test must pass).
- `InspectDatabase` on an encrypted multi-file database reports `IsEncrypted = true`, `IsMultiFileMode = true`, and lists all collection files in `StorageSizeBytes`.
- `InspectDatabase` on a database with no `[PersonalData]` annotations and no source-gen reports `PersonalDataFields = []` (not null).

#### C. Do-not-touch

- Do not modify `BLiteEngine` ctors. Add functionality only via extension methods in `GdprEngineExtensions`.
- Do not modify any file under `src/BLite.Core/Encryption/`, `src/BLite.Core/Storage/`, or `src/BLite.Core/Audit/`.
- Do not introduce a new "GDPR runtime" service registered in the engine. The Foundation is stateless beyond the metadata cache.

---

### WP2 — CDC Field Masking

**GDPR article:** 5(1)(c).  
**GitHub issue:** rewrite of #90.  
**Estimate:** 1–2 days.

**Design intent.** During `Watch(...)` the consumer must be able to **explicitly** decide whether personal-data fields are delivered in clear or masked. The default is GDPR-safe: personal data is **masked**. The user opts in to see clear values via `RevealPersonalData = true` or by listing the specific field in `IncludeOnlyFields`.

#### A. Tasks

1. Add `ExcludeFields`, `IncludeOnlyFields`, `RevealPersonalData`, `PersonalDataMaskValue` to `WatchOptions` (§4.4). Defaults: empty array; null; **false**; `BsonValue.FromString("***")`.
2. Plumb the new fields end-to-end: `WatchOptions` → `ChangeStreamObservable` / `DynamicChangeStreamObservable` (carry the resolved personal-data field list and the mask value alongside `_capturePayload`) → `ChangeStreamDispatcher.Subscribe(...)` (extend the signature additively, or wrap the four masking inputs in a small `internal readonly record struct PayloadMaskingPolicy`).
3. In the CDC dispatch path (the same code that today handles `CapturePayload == true`), apply the masking rules from §4.4 on a **clone** of the BSON document before notifying observers. The clone happens once per dispatch, regardless of how many observers are subscribed; per-observer policies are evaluated against that clone.
4. Add `internal static class PayloadMask` with three methods, all returning a new `BsonDocument`:
   - `Allowlist(BsonDocument doc, IReadOnlyList<string> keep)`
   - `Blocklist(BsonDocument doc, IReadOnlyList<string> remove)`
   - `MaskPersonalData(BsonDocument doc, IReadOnlyList<PersonalDataField> fields, BsonValue maskValue)` — replaces each personal-data key with `maskValue`, or removes the key when `maskValue.IsNull`.
5. Resolve the personal-data field list once, at `Watch(...)` time, via `PersonalDataResolver.Resolve(entityType)` (WP1). Cache it on the observable so the dispatch path is O(personalFields) per event, not O(types) per event. For dynamic / untyped collections the list is empty and rule 2 of §4.4 is a no-op.
6. Tests (≥ 8): round-trip every combination across both typed and dynamic streams — `CapturePayload = false`, `RevealPersonalData = true`, `RevealPersonalData = false` with one / many `[PersonalData]` fields, custom `PersonalDataMaskValue` (string and `BsonValue.Null`), `ExcludeFields` only, `IncludeOnlyFields` only (asserting that an allowlisted personal-data field IS emitted in clear), `IncludeOnlyFields` + `ExcludeFields` (allowlist wins), and a regression test that `CapturePayload = false` is byte-identical to today.

#### B. Acceptance criteria

- With `CapturePayload = false`, observers receive the same payload they receive today (a regression test must lock this in).
- With `CapturePayload = true` and **no other options set**, watching a typed collection whose entity has `[PersonalData] string Email` delivers a document where `Email == "***"` and all other keys are unchanged. The default behavior is GDPR-safe without any user configuration.
- With `CapturePayload = true` and `RevealPersonalData = true`, every personal-data field is delivered in clear (only `ExcludeFields` / `IncludeOnlyFields` still apply).
- With `CapturePayload = true`, `RevealPersonalData = false`, and `PersonalDataMaskValue = BsonValue.Null`, personal-data keys are **removed** from the cloned document instead of masked.
- With `CapturePayload = true` and `IncludeOnlyFields = ["Email"]`, the dispatched document contains only `Email` **in clear**, regardless of `RevealPersonalData` (the allowlist is the consumer's authoritative statement of intent).
- With both `IncludeOnlyFields` and `ExcludeFields` set, the allowlist wins and `ExcludeFields` is ignored (documented in XMLDoc).
- For a dynamic collection (no registered entity type) with `CapturePayload = true` and `RevealPersonalData = false`, the dispatched document is unchanged (no metadata available) and a single info-level log is emitted on the first such subscription per collection.
- The original in-memory page buffer is never mutated; assert via reference equality on the source page bytes after dispatch.

#### C. Do-not-touch

- Do not modify the storage page format.
- Do not modify CDC subscription transport (`Channel<T>`, `IObservable<T>`).
- Do not introduce a new public options class — extend `WatchOptions` only. The internal `PayloadMaskingPolicy` record struct (if introduced in task 2) stays `internal`.
- Do not call `PersonalDataResolver.Resolve(entityType)` on the hot dispatch path — resolve once at `Watch(...)` time and cache on the observable.

---

### WP3 — `GdprMode.Strict` orchestration

**GDPR article:** 25.  
**GitHub issue:** rewrite of #93.  
**Estimate:** 1–2 days (after WP1 + WP2 merged).

#### A. Tasks

1. Add `GdprMode` enum and `GdprModeAttribute` (§4.5) under `BLite.Core.GDPR/`.
2. Add `EntityTypeBuilder<T>.HasGdprMode(GdprMode)` fluent extension in `src/BLite.Core/Metadata/EntityTypeBuilder.Gdpr.cs` (additive; mirrors the existing `HasRetentionPolicy` pattern in [`EntityTypeBuilder.cs`](../../src/BLite.Core/Metadata/EntityTypeBuilder.cs)). Persist the resolved mode on the existing `EntityTypeBuilder<T>` instance.
3. Add `public GdprMode DefaultGdprMode { get; init; } = GdprMode.None;` to `BLite.Core.KeyValue.BLiteKvOptions` ([`KeyValue/BLiteKvOptions.cs`](../../src/BLite.Core/KeyValue/BLiteKvOptions.cs)). This is the **only** engine-wide options class in v5; do not introduce `BLiteEngineOptions`.
4. Implement `internal static class GdprStrictValidator` with one entry point: `Apply(BLiteEngine engine, string collectionName, GdprMode resolvedMode, BLiteKvOptions options, ILogger? logger)`. Called at engine open, **per collection**, after the catalog is loaded.
5. The validator implements the table in §4.5. Use a single dedicated `EventId` per emitted log line (`GdprStrictEncryptionMissing`, `GdprStrictAuditMissing`, `GdprStrictAuditModuleAbsent`, `GdprStrictRetentionWarning`, `GdprStrictSecureEraseEnabled`, `GdprStrictCdcCapturePayloadWarning`).
6. Tests: throw when encryption missing on a Strict collection; warn when audit sink missing (when #83 is shipped) and emit `GdprStrictAuditModuleAbsent` (when #83 is not shipped); warn when retention missing on a `[PersonalData]` collection; do nothing when resolved mode is `GdprMode.None`.

#### B. Acceptance criteria

- Opening a `BLiteEngine` whose catalog contains a collection resolved to `GdprMode.Strict` and no `CryptoOptions` throws `InvalidOperationException` whose message contains `"GdprMode.Strict requires"`.
- Opening with a Strict collection, encryption configured, audit module shipped (#83 closed) but no audit sink registered: engine starts, one warning emitted with EventId `GdprStrictAuditMissing` (no auto-construction).
- Opening with a Strict collection, encryption configured, audit module **not** shipped (#83 still open): engine starts, exactly one log line with EventId `GdprStrictAuditModuleAbsent` is emitted (idempotent across multiple Strict collections).
- Opening with a Strict collection, encryption + audit, but the collection has `[PersonalData]` fields and no retention configured: engine starts, one warning emitted per such collection with EventId `GdprStrictRetentionWarning`.
- A collection resolved to `GdprMode.None` (default) must produce zero log lines from the validator regardless of engine configuration.

#### C. Do-not-touch

- Do not modify `CryptoOptions`, `EncryptionCoordinator`, or any encryption file.
- Do not modify the `IBLiteAuditSink` interface or implement an audit sink — that is the scope of issue #83. WP3 only consumes the resolved sink (or its absence).
- Do not implement retention or VACUUM as part of WP3 — only inspect their presence.
- Do not introduce a `BLiteEngineOptions` class. The canonical options class is `BLiteKvOptions`.

#### D. Graceful degradation matrix

| Missing dependency | WP3 behavior |
|---|---|
| Audit module not yet implemented (issue #83 still open) | log a single warning `GdprStrictAuditModuleAbsent`, continue without throwing — never construct a stand-in sink |
| Retention module not yet implemented | skip the retention warning loop silently |
| `SecureEraseOnDelete` setting not yet implemented | log a warning `GdprStrictSecureEraseUnavailable`, continue |

WP3 must not block on these dependencies. It may be merged before they land.

---

### WP4 — DPIA Documentation

**GDPR article:** 35.  
**GitHub issue:** rewrite of #94.  
**Estimate:** 1 day.

#### A. Deliverable

Single file: `docs/DPIA_CHECKLIST.md`. Sections:

1. Scope statement: BLite is an embedded in-process database; the data controller is the host application.
2. Risk + mitigation table (rows enumerated below — every row is a separate Markdown row, never collapsed).
3. Configuration checklist (encryption, audit, secure erase, retention, CDC masking).
4. Integration checklist (what the application must implement: key management, access control, subject erasure workflow).
5. GDPR article → BLite feature mapping (one row per article cited in §1 of this plan).
6. Legal disclaimer.

#### B. Mandatory risk rows (do not collapse)

| Risk | Likelihood | Impact | BLite mitigation | Cross-reference |
|---|---|---|---|---|
| Unauthorised access to the `.db` file | Medium | High | Encryption at-rest (AES-256-GCM, page-level) | ENCRYPTION_FIX_PLAN.md |
| Exfiltration via backup file | Low | High | Encrypted backups (same key derivation) | ENCRYPTION_FIX_PLAN.md |
| Residual data after `DeleteAsync` | Medium | Medium | Slot-level secure erase; VACUUM | MISSING_FEATURES.md §3 |
| Compromised audit log | Low | High | Append-only `FileAuditSink`; optional cryptographic signing | AUDIT_IMPLEMENTATION.md |
| Encryption key loss | Low | Critical | `IKeyProvider` with external KMS | ENCRYPTION_PLAN.md |
| CDC PII leak | Medium | Medium | Field masking via `WatchOptions.ExcludeFields`/`IncludeOnlyFields` | this plan §4.4 |
| Retention policy not enforced | Low | Medium | Generalised retention + Strict-mode warning | MISSING_FEATURES.md §4, this plan §4.5 |
| WAL plaintext exposure | Medium | High | WAL encryption (sibling provider, role = 3) | ENCRYPTION_PLAN.md |
| Subject access request not actionable | Medium | Medium | `ExportSubjectDataAsync` (§4.2) | this plan §4.2 |
| Compliance auditor lacks visibility | Low | Medium | `InspectDatabase` (§4.3) | this plan §4.3 |
| Mis-configured GDPR-sensitive deployment | Medium | High | `GdprMode.Strict` (§4.5) | this plan §4.5 |

#### C. Acceptance criteria

- File compiles in Markdown lint (no broken cross-links to files in this repo).
- Every risk row maps to a feature that either ships in v5 or is explicitly marked `(planned)`.
- Legal disclaimer present at the bottom, text matches §9 of this plan verbatim.

#### D. Do-not-touch

- No legal claims of "GDPR-certified" or "GDPR-compliant out of the box".
- No code changes.

---

## 7. Resolved design decisions (formerly open questions)

All five questions originally listed here have been resolved against the v5 codebase. Decisions are now binding for all WPs; deviations require updating this section first.

- **Q1 — Retention policy reuse (RESOLVED):** `RetentionPolicy` and `RetentionPolicyBuilder` already exist (see [`src/BLite.Core/Metadata/EntityTypeBuilder.cs`](../../src/BLite.Core/Metadata/EntityTypeBuilder.cs) and [`src/BLite.Core/DynamicCollection.cs`](../../src/BLite.Core/DynamicCollection.cs) — `EntityTypeBuilder<T>.HasRetentionPolicy(...)`, `DynamicCollection.SetRetentionPolicy(...)`, `BLiteEngine.SetRetentionPolicy(...)`/`GetTimeSeriesConfig(...)`). **WP1 `RetentionPolicyInfo` is a read-only projection of the existing `RetentionPolicy`** (with TTL field name and retention duration). It is not a new persistence type.
- **Q2 — Configuration surface (RESOLVED):** the canonical pattern in v5 is **per-collection fluent configuration via `EntityTypeBuilder<T>`** (mirrors `HasRetentionPolicy`). WP3 ships `EntityTypeBuilder<T>.HasGdprMode(GdprMode)` and a `[GdprMode(GdprMode.Strict)]` attribute. The single shipping engine-wide options class is `BLiteKvOptions` ([`src/BLite.Core/KeyValue/BLiteKvOptions.cs`](../../src/BLite.Core/KeyValue/BLiteKvOptions.cs)); an engine-wide default `DefaultGdprMode` may be added there, but **per-entity configuration overrides it** and is the documented surface. There is no `BLiteEngineOptions` class in the codebase — do not invent one.
- **Q3 — Source generator integration (RESOLVED):** the existing source-gen pipeline (`EntityAnalyzer` → `MapperGenerator` → `CodeGenerator` in `src/BLite.SourceGenerators/`) already discovers attributes and fluent ModelBuilder calls (e.g. `Property(...).HasConversion<T>()`, `HasIndex(...)`). **WP1 extends this same pipeline** to recognise (a) the `[PersonalData]` attribute on properties and (b) a fluent `PropertyBuilder<T,TProp>.HasPersonalData(...)` extension, and emits the personal-data metadata into the **already-generated entity mapper**. There is no separate `IPersonalDataMetadata` infrastructure: the generated mapper exposes `PersonalDataFields` (name list + categories) as a static property on the same generated class, with a reflection-based fallback for unmapped types.
- **Q4 — Async streaming (RESOLVED):** `BLiteEngine.FindAllAsync(string collectionName, CancellationToken)` and `BLiteEngine.FindAsync(string collectionName, predicate, CancellationToken)` already return `IAsyncEnumerable<BsonDocument>` for **dynamic (by-name) collections** ([`src/BLite.Core/BLiteEngine.cs`](../../src/BLite.Core/BLiteEngine.cs#L643)). For typed access use `DocumentCollection<TKey,T>.ScanAsync` / `FindAllAsync`. **WP1 uses these existing primitives directly**; no new scan helper is added.
- **Q5 — Audit sink default (RESOLVED):** the `IBLiteAuditSink` contract and its file-based default implementation are the scope of **issue #83** ([Audit] Audit Trail Module). **WP3 takes a hard dependency on #83** (`IBLiteAuditSink` from `BLite.Core` audit namespace) and does not invent a sink. Default file path policy lives in #83. WP3 only wires the existing sink into the Strict bootstrap and emits subject-export / inspection / masking events through it.

---

## 8. Cross-WP integration tests

Beyond per-WP tests, three end-to-end tests live in `tests/BLite.Tests/Gdpr/EndToEndGdprTests.cs`:

1. **Strict-mode happy path:** create an encrypted DB with `[PersonalData]` model, audit sink, retention; call `ExportSubjectDataAsync`; assert subject's data returned and an audit event recorded for the export operation.
2. **Subject export with CDC masking:** open a watch with `ExcludeFields = ["Email"]`; insert and immediately export; assert CDC observers saw masked payload but `ExportSubjectDataAsync` returned full payload (export is a controller operation, masking is a CDC concern).
3. **Inspection roundtrip:** populate a 3-collection DB; call `InspectDatabase`; assert counts match `CountAsync` and `IsEncrypted`/`IsAuditEnabled` agree with engine state.

These tests are added in WP3 (the first point at which all surfaces are present).

---

## 9. Legal disclaimer (verbatim — must be cited in WP4)

> This document is a technical plan, not legal advice. GDPR compliance depends on the overall application implementation, not on the embedded database alone. The host application acts as the data controller and is responsible for lawful processing, lawful basis, subject communication, and final compliance assessment. Engaging a Data Protection Officer (DPO) for the formal assessment is strongly recommended.

---

## 10. Glossary

- **Data controller** — the entity that determines purposes and means of processing (the host application in BLite scenarios).
- **Personal data** — any information relating to an identified or identifiable natural person (Art. 4(1)).
- **Special categories** — Art. 9 categories: race, ethnic origin, political opinions, religion, trade-union membership, genetic/biometric/health data, sex life, sexual orientation.
- **Subject access request (SAR)** — a request under Art. 15 for the data held about the subject; serviced by `ExportSubjectDataAsync`.
- **Crypto-shredding** — making encrypted data irrecoverable by destroying the key; an accepted Art. 17 erasure technique. Out of scope for BLite v5 (no per-subject key support).

---

## 11. Change log

- **2026-05-02** — §7 questions resolved against the v5 codebase: WP1 reuses existing `RetentionPolicy`; GDPR config is per-collection via `EntityTypeBuilder<T>.HasGdprMode` (mirroring `HasRetentionPolicy`); source-gen pipeline is **extended**, not duplicated, to emit personal-data metadata; WP1 uses existing `BLiteEngine.FindAllAsync(string,...)` streaming; WP3 takes a hard dependency on issue #83 for `IBLiteAuditSink`.
- **2026-05-01** — full rewrite. Replaces the per-article ShapeUp pitches with a single architecture spec + 4 cohesive work packages. Issues #91 and #92 consolidated into WP1.
- **2026-04-27** — original draft (per-article structure).
