# BLite — Development Plan: GDPR Compliance

> Date: April 27, 2026  
> References: Regulation (EU) 2016/679 (GDPR), [MISSING_FEATURES.md](MISSING_FEATURES.md), [ENCRYPTION_PLAN.md](ENCRYPTION_PLAN.md), [DocumentCollection.cs](../../src/BLite.Core/Collections/DocumentCollection.cs), [ChangeStreamDispatcher.cs](../../src/BLite.Core/CDC/ChangeStreamDispatcher.cs)

---

## Overview

BLite is an embedded database: GDPR compliance responsibility lies primarily with the **data controller** (the application integrating BLite). However, the engine itself must provide the **technical primitives** that make compliance possible and auditable — following the **Privacy by Design** principle (Art. 25 GDPR).

This document identifies current gaps and the plan to address them, organized by GDPR article.

---

## GDPR Article → BLite Feature Mapping

| GDPR Article | Principle | Required feature | Status |
|--------------|-----------|-----------------|--------|
| Art. 5(1)(b) | Purpose limitation | Purpose tags on collections | ❌ Missing |
| Art. 5(1)(c) | Data minimisation | Field-level masking / exclusion | ❌ Missing |
| Art. 5(1)(e) | Storage limitation | Generalised retention policy | ⚠️ Partial (TimeSeries only) |
| Art. 5(1)(f) | Integrity and confidentiality | Encryption at-rest | ❌ Missing |
| Art. 15 | Right of access | Data subject export | ❌ Missing |
| Art. 17 | Right to erasure | Secure erase / VACUUM | ⚠️ Partial |
| Art. 20 | Data portability | Structured export formats | ❌ Missing |
| Art. 25 | Privacy by Design | Secure defaults, minimisation | ⚠️ Partial |
| Art. 30 | Records of processing | Audit trail | ❌ Not implemented |
| Art. 32 | Security of processing | Encryption + Audit | ❌ Missing |
| Art. 35 | DPIA | Impact assessment tooling | ❌ Missing |

---

## 1. Encryption at Rest (Art. 5(1)(f), Art. 32)

> See [ENCRYPTION_PLAN.md](ENCRYPTION_PLAN.md) for full details.

### Gap
No encryption implemented. `.db`, WAL, and backup files are all plaintext.

### Actions
1. Implement `ICryptoProvider` / `AesGcmCryptoProvider` in the storage layer
2. Encrypt the WAL with the same derived key as the corresponding database file
3. Encrypt all backup files (optionally with a separate backup key)
4. Implement `EncryptionCoordinator` for multi-file (server) mode with per-file HKDF subkeys
5. Document mandatory configuration for production environments

---

## 2. Audit Trail (Art. 30, Art. 32)

> See [MISSING_FEATURES.md §1](MISSING_FEATURES.md) for full details.

### Gap
`src/BLite.Core/Audit/` does not exist. CDC is not a formal audit trail.

### Actions
1. Implement `IBLiteAuditSink` with a built-in JSONL file sink
2. Record: who read/wrote/deleted, when, which collection, which document
3. Guarantee the audit log is **append-only** and not modifiable by the application
4. Support cryptographic signing of entries (optional, for high-compliance environments)

### Audit log privacy
- **Do not include the document payload** in audit entries by default — only `documentId`, `operation`, `timestamp`, `userId`
- Payload capture may be enabled with `AuditOptions.CapturePayload = true` only if the data is not PII, or if it is already encrypted

---

## 3. Right of Access — Art. 15

### Gap
No API to extract all data belonging to a single data subject.

### Proposed API

```csharp
// On BLiteEngine
Task<SubjectDataReport> ExportSubjectDataAsync(
    SubjectQuery query,
    CancellationToken ct = default
);

public sealed class SubjectQuery
{
    /// <summary>
    /// Name of the field that identifies the subject (e.g. "UserId", "Email")
    /// </summary>
    public required string FieldName { get; init; }

    /// <summary>
    /// Value to search for
    /// </summary>
    public required BsonValue FieldValue { get; init; }

    /// <summary>
    /// If null, searches all collections. Otherwise only the specified ones.
    /// </summary>
    public IReadOnlyList<string>? Collections { get; init; }

    public ExportFormat Format { get; init; } = ExportFormat.Json;
}

public sealed class SubjectDataReport
{
    public DateTimeOffset GeneratedAt { get; init; }
    public BsonValue SubjectId { get; init; }
    public IReadOnlyDictionary<string, IReadOnlyList<BsonDocument>> DataByCollection { get; init; }

    public Task WriteToFileAsync(string path, CancellationToken ct = default);
    public Task WriteToStreamAsync(Stream stream, CancellationToken ct = default);
}
```

### Files to create
| File | Purpose |
|------|---------|
| `src/BLite.Core/GDPR/SubjectQuery.cs` | Data subject query |
| `src/BLite.Core/GDPR/SubjectDataReport.cs` | Export result |
| `src/BLite.Core/GDPR/GdprExtensions.cs` | Extension methods on `BLiteEngine` |

### Estimate: 2–3 days

---

## 4. Right to Erasure — Art. 17

### Architectural boundary

The **right to erasure is an application-layer obligation**, not a database primitive. BLite is an in-process, schema-less embedded store: it has no concept of "data subject", no knowledge of which fields identify a person, and no authority to decide which data may or must be deleted. Only the application holding the data — the data controller — can make that determination.

BLite's role is to provide **complete, reliable, and physically effective deletion primitives** that the application can call when fulfilling an erasure request. The application is responsible for:
- Identifying all collections and documents belonging to a data subject
- Deciding the scope of deletion (single collection vs. all collections)
- Triggering deletion at the appropriate point in its own workflow
- Logging the erasure event in its own compliance records

### What BLite must guarantee

| Guarantee | Current state | Plan |
|-----------|--------------|------|
| Logical delete (document removed from queries) | ✅ Done (`DeleteAsync`) | — |
| Physical page cleanup on `DropCollection` | ⚠️ Deferred | Fix in [MISSING_FEATURES.md §7](MISSING_FEATURES.md) |
| Slot-level zero-fill after `DeleteAsync` | ❌ Missing | [MISSING_FEATURES.md §3](MISSING_FEATURES.md) |
| VACUUM — physically compact and truncate file | ❌ Missing | [MISSING_FEATURES.md §3](MISSING_FEATURES.md) |
| `TruncateAsync` on collection | ❌ Missing | [MISSING_FEATURES.md §7](MISSING_FEATURES.md) |

### Encryption as erasure

A well-known GDPR-accepted technique for embedded databases is **crypto-shredding**: if data is encrypted at-rest with a per-subject or per-collection key, deleting the key makes the ciphertext irrecoverable, satisfying Art. 17 without requiring physical overwrite. This requires the encryption layer described in [ENCRYPTION_PLAN.md](ENCRYPTION_PLAN.md).

### Estimate: no dedicated BLite work — depends on §3 and §7 of MISSING_FEATURES.md

---

## 5. Data Portability — Art. 20

### Gap
No export mechanism in a portable format (JSON, CSV).

### Proposed API

```csharp
// Extends SubjectDataReport
public sealed class SubjectDataReport
{
    // ...
    public Task ExportAsJsonAsync(Stream output, CancellationToken ct = default);
    public Task ExportAsCsvAsync(Stream output, string collection, CancellationToken ct = default);
    public Task ExportAsBsonAsync(Stream output, CancellationToken ct = default); // for import into another BLite instance
}
```

### Estimate: 1–2 days (closely tied to Art. 15)

---

## 6. Data Minimisation (Art. 5(1)(c)) and Privacy by Design (Art. 25)

### 6a. Field-level masking in CDC

**Current issue**: `Watch()` in `DocumentCollection.cs` (L2956) with `capturePayload=true` exposes the full BSON document. If the document contains PII (e.g. email, SSN, health data), this is broadcast in plaintext to all observers.

**Solution**:
```csharp
// Add to WatchOptions:
public sealed class WatchOptions
{
    public bool CapturePayload { get; init; } = false;

    /// <summary>
    /// If CapturePayload = true, these fields are stripped from the payload before notification.
    /// </summary>
    public IReadOnlyList<string> ExcludeFields { get; init; } = [];

    /// <summary>
    /// If set, only these fields are included in the payload.
    /// </summary>
    public IReadOnlyList<string>? IncludeOnlyFields { get; init; }
}
```

### 6b. Field-level masking in queries

Add **projection** support in LINQ/BLQL queries to exclude sensitive fields from results:
```csharp
collection.Query()
    .Where(x => x.CategoryId == 1)
    .Select(x => new { x.Id, x.Name })  // excludes PII like Email, SSN
    .ToListAsync();
```
> Verify whether LINQ projection is already supported — if so, document it as a minimisation tool.

### 6c. `[PersonalData]` annotations for Source Generators

Add a `[PersonalData]` attribute:
```csharp
[BLiteCollection]
public class User
{
    public int Id { get; set; }

    [PersonalData]
    public string Email { get; set; }

    [PersonalData(Sensitivity = DataSensitivity.Sensitive)]
    public string TaxCode { get; set; }
}
```

The Source Generator can:
- Automatically generate the PII field list for subject export/deletion
- Emit a warning if a `[PersonalData]` field is included in an unencrypted index

### Estimate: 3–4 days

---

## 7. Storage Limitation (Art. 5(1)(e))

### Gap
Retention exists only for TimeSeries. Regular collections have no automatic TTL.

### Actions
Implement the generalised retention policy described in [MISSING_FEATURES.md §4](MISSING_FEATURES.md), with support for:
- `MaxAgeMs` on a `[PersonalData(IsTimestamp = true)]` field
- Audit notification before automatic deletion
- `RetentionPolicyApplied` event in CDC (optional, configurable)

### Estimate: covered by MISSING_FEATURES §4 (3–4 days)

---

## 8. Records of Processing Activities — Art. 30

Art. 30 requires a written record of processing activities. BLite cannot create this register on behalf of the application, but it can provide a **structured template** and an **inspection API** to facilitate its compilation.

### Inspection API
```csharp
// On BLiteEngine
DatabaseInspectionReport InspectDatabase();

public sealed class DatabaseInspectionReport
{
    public string DatabasePath { get; init; }
    public bool IsEncrypted { get; init; }
    public bool IsAuditEnabled { get; init; }
    public IReadOnlyList<CollectionInfo> Collections { get; init; }
}

public sealed class CollectionInfo
{
    public string Name { get; init; }
    public long DocumentCount { get; init; }
    public IReadOnlyList<IndexInfo> Indexes { get; init; }
    public IReadOnlyList<string> PersonalDataFields { get; init; }  // from [PersonalData] attributes
    public RetentionPolicyInfo? RetentionPolicy { get; init; }
}
```

### Estimate: 2 days

---

## 9. Data Protection Impact Assessment (DPIA) — Art. 35

BLite must provide documentation and tooling to support the data controller's DPIA.

### DPIA checklist for applications using BLite

Create `docs/DPIA_CHECKLIST.md` with:

**Identified risks and mitigations**

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|-----------|
| Unauthorised access to `.db` file | Medium | High | Encryption at-rest (ENCRYPTION_PLAN §2) |
| Exfiltration via backup | Low | High | Backup encryption (ENCRYPTION_PLAN §6) |
| Residual data after deletion | Medium | Medium | VACUUM / Secure Erase (MISSING_FEATURES §3) |
| Compromised audit log | Low | High | Append-only sink, cryptographic signing |
| Encryption key loss | Low | Critical | `IKeyProvider` with external KMS |
| CDC PII leak | Medium | Medium | Field masking in `WatchOptions` |
| Retention policy not enforced | Low | Medium | Retention policy + alert |

### Estimate: 1 day (documentation only)

---

## 10. Privacy by Default Configuration

Add a GDPR compliance flag to `BLiteEngineOptions` that automatically sets recommended defaults:

```csharp
var db = new BLiteEngine(new BLiteEngineOptions
{
    Filename = "mydata.db",
    GdprMode = GdprModeOptions.Strict
    // Equivalent to:
    // - Encryption = required (exception if not configured)
    // - Audit = enabled (FileAuditSink at default path)
    // - CDC CapturePayload default = false (immutable)
    // - Secure erase on delete = true
    // - Retention policy warning for collections with [PersonalData] fields
});
```

| Setting | Standard default | `GdprMode.Strict` default |
|---------|-----------------|--------------------------|
| Encryption | Disabled | Required (exception if missing) |
| Audit | Disabled | Enabled (file sink) |
| CDC CapturePayload | false | false (immutable) |
| Secure erase on delete | No | Yes |
| Retention policy | Optional | Warning if `[PersonalData]` present without retention |

---

## Summary Roadmap

| # | Feature | GDPR Article | Priority | Estimate | Dependency |
|---|---------|-------------|----------|----------|-----------|
| 1 | Encryption at-rest | Art. 32 | HIGH | 22 d | — |
| 2 | Audit Trail | Art. 30, 32 | HIGH | 5–8 d | — |
| 3 | Data Subject Export (Art. 15) | Art. 15 | HIGH | 2–3 d | — |
| 4 | Secure Erase / VACUUM + complete Drop/Truncate | Art. 17 | HIGH | 4–6 d + 3–4 d | — |
| 5 | Data Portability (Art. 20) | Art. 20 | MEDIUM | 1–2 d | #3 |
| 6 | CDC field masking | Art. 5(1)(c) | MEDIUM | 1 d | — |
| 7 | `[PersonalData]` attribute | Art. 25 | MEDIUM | 2–3 d | — |
| 8 | Generalised retention | Art. 5(1)(e) | MEDIUM | 3–4 d | — |
| 9 | Database inspection API | Art. 30 | LOW | 2 d | #7 |
| 10 | `GdprMode.Strict` | Art. 25 | LOW | 1 d | #1, #2 |
| 11 | DPIA Checklist | Art. 35 | LOW | 1 d | — |

**Total estimate: 44–53 days** (significant overlap with MISSING_FEATURES and ENCRYPTION_PLAN)

---

## Legal Disclaimer

> This document is a technical plan, not legal advice. GDPR compliance depends on the overall application implementation, not just the embedded database. Involving a Data Protection Officer (DPO) for the final assessment is strongly recommended.

---

## Related files
- [MISSING_FEATURES.md](MISSING_FEATURES.md)
- [ENCRYPTION_PLAN.md](ENCRYPTION_PLAN.md)
- [AUDIT_IMPLEMENTATION.md](AUDIT_IMPLEMENTATION.md)
- [RFC.md](../../RFC.md)
