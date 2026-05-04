# BLite — DPIA Checklist (Art. 35)

> **Purpose:** This checklist supports teams using BLite in GDPR-sensitive applications to populate their Article 35 Data Protection Impact Assessment (DPIA) with engine-specific risks, mitigations, configuration steps, and integration responsibilities.
>
> **Source of truth:** [`roadmap/v5/GDPR_PLAN.md`](../roadmap/v5/GDPR_PLAN.md) §6 WP4.

---

## 1. Scope Statement

BLite is an **embedded, in-process database**. It runs inside the host application's process and has no network listener, no remote access surface, and no independent authentication layer.

Under GDPR, **the data controller is the host application**, not the database engine. BLite's role is to provide technical primitives that make compliance *possible* and *auditable*:

- Marking domain fields as personal data at compile time (`[PersonalData]`).
- Extracting all data belonging to a single data subject (Art. 15, 20).
- Inspecting the database surface for compliance reporting (Art. 30).
- Suppressing PII leakage through the change-data-capture (CDC) channel (Art. 5(1)(c)).
- A safe-default configuration profile that wires encryption + audit + retention + secure erase (Art. 25).
- This DPIA documentation deliverable (Art. 35).

BLite **does not** determine what counts as personal data at runtime, auto-delete data based on detected PII, certify an application as "GDPR-compliant", or relieve the host application of its controller obligations.

---

## 2. Risk + Mitigation Table

The following table enumerates all engine-level risks identified in [`roadmap/v5/GDPR_PLAN.md`](../roadmap/v5/GDPR_PLAN.md) §6 WP4(B). Each row is listed separately; rows are never collapsed even when mitigations overlap.

Features the engine will not implement are marked **(won't implement)** — the host application is responsible for those cases.

| Risk | Likelihood | Impact | BLite mitigation | Cross-reference |
|---|---|---|---|---|
| Unauthorised access to the `.db` file | Medium | High | Encryption at-rest (AES-256-GCM, page-level) | [`roadmap/v5/ENCRYPTION_FIX_PLAN.md`](../roadmap/v5/ENCRYPTION_FIX_PLAN.md) |
| Exfiltration via backup file | Low | High | Encrypted backups (same key derivation) | [`roadmap/v5/ENCRYPTION_FIX_PLAN.md`](../roadmap/v5/ENCRYPTION_FIX_PLAN.md) |
| Residual data after `DeleteAsync` | Medium | Medium | Slot-level secure erase via `VacuumAsync` (`SecureErase = true`); per-delete `SecureEraseOnDelete` engine toggle **(won't implement — call `VacuumAsync` after deletion)** | [`roadmap/v5/MISSING_FEATURES.md`](../roadmap/v5/MISSING_FEATURES.md) §3 |
| Compromised audit log | Low | High | `IBLiteAuditSink`; implement an append-only file sink with optional cryptographic signing in the host application **(won't implement in engine)** | [`roadmap/v5/AUDIT_IMPLEMENTATION.md`](../roadmap/v5/AUDIT_IMPLEMENTATION.md) |
| Encryption key loss | Low | Critical | `IKeyProvider` with external KMS | [`roadmap/v5/ENCRYPTION_PLAN.md`](../roadmap/v5/ENCRYPTION_PLAN.md) |
| CDC PII leak | Medium | Medium | Field masking via `WatchOptions.ExcludeFields`/`IncludeOnlyFields` | [`roadmap/v5/GDPR_PLAN.md`](../roadmap/v5/GDPR_PLAN.md) §4.4 |
| Retention policy not enforced | Low | Medium | Generalised retention policy (`HasRetentionPolicy`); `GdprMode.Strict` warns on missing retention | [`roadmap/v5/GDPR_PLAN.md`](../roadmap/v5/GDPR_PLAN.md) §4.5 |
| WAL plaintext exposure | Medium | High | WAL encryption (sibling provider, role = 3) | [`roadmap/v5/ENCRYPTION_PLAN.md`](../roadmap/v5/ENCRYPTION_PLAN.md) |
| Subject access request not actionable | Medium | Medium | `ExportSubjectDataAsync` | [`roadmap/v5/GDPR_PLAN.md`](../roadmap/v5/GDPR_PLAN.md) §4.2 |
| Compliance auditor lacks visibility | Low | Medium | `InspectDatabase` | [`roadmap/v5/GDPR_PLAN.md`](../roadmap/v5/GDPR_PLAN.md) §4.3 |
| Mis-configured GDPR-sensitive deployment | Medium | High | `GdprMode.Strict` | [`roadmap/v5/GDPR_PLAN.md`](../roadmap/v5/GDPR_PLAN.md) §4.5 |

---

## 3. Configuration Checklist

The following settings **must** be enabled for any deployment that processes high-risk personal data. Skipping any item leaves the corresponding risk unmitigated.

- [ ] **Encryption configured** — supply a `CryptoOptions` instance (passphrase or `IKeyProvider`) when opening the engine. Verify `DatabaseInspectionReport.IsEncryptionEnabled == true` after open.
- [ ] **Audit sink registered** — implement and register `IBLiteAuditSink` via `BLiteEngine.ConfigureAudit(...)`. All subject-export and inspection operations emit audit events through this sink.
- [ ] **Secure erase on vacuum** — call `VacuumAsync(new VacuumOptions { SecureErase = true })` as part of your maintenance schedule and after bulk deletions of personal data to overwrite freed page bytes. A per-delete engine toggle will not be provided; the host application must call `VacuumAsync` explicitly as part of its erasure workflow.
- [ ] **Retention policy on `[PersonalData]` collections** — configure `HasRetentionPolicy(...)` on every `EntityTypeBuilder<T>` that carries `[PersonalData]` fields so that data is not retained beyond its lawful purpose.
- [ ] **CDC payload masking** — set `WatchOptions.ExcludeFields` or `WatchOptions.IncludeOnlyFields` on every `Watch(...)` call that may observe personal-data collections, or leave `CapturePayload = false` (the default) for streams that do not require payload visibility.
- [ ] **`GdprMode = Strict` enabled** — set `DefaultGdprMode = GdprMode.Strict` in `BLiteKvOptions`, or call `.HasGdprMode(GdprMode.Strict)` per collection via `EntityTypeBuilder<T>`. Strict mode validates that encryption, audit, and retention are configured at engine-open time and logs actionable warnings for any gap.

---

## 4. Integration Checklist

The following responsibilities belong to the **host application** (the data controller). BLite cannot fulfil them on the application's behalf.

- [ ] **External key management** — store encryption keys or passphrases in an external KMS or HSM. Implement `IKeyProvider` to supply keys at open time. Never embed raw key material in application source code or configuration files.
- [ ] **Host-level access control on the `.db` file path** — restrict OS-level file permissions on the database file and its directory so that only the application process (and authorised administrators) can read or write the file. BLite has no independent access-control layer.
- [ ] **Subject identifier convention** — choose a stable, consistent field name (e.g. `userId`, `subjectId`) as the `SubjectQuery.FieldName` for all `ExportSubjectDataAsync` calls. Annotate the corresponding property with `[PersonalData]` so it appears in `DatabaseInspectionReport`.
- [ ] **Deletion workflow** — implement a subject-erasure workflow that: (1) calls `DeleteAsync` on all relevant documents, (2) calls `VacuumAsync(new VacuumOptions { SecureErase = true })` to physically overwrite freed pages, and (3) records the erasure in the external audit log. A built-in per-delete secure-erase toggle will not be provided; the explicit `VacuumAsync` call is the supported erasure mechanism.
- [ ] **Audit log retention outside BLite** — define and enforce a retention policy for the external audit log produced by your `IBLiteAuditSink` implementation. BLite emits events but does not manage the long-term storage or deletion of audit records.

---

## 5. GDPR Article → BLite Feature Mapping

| GDPR Article | Summary | BLite feature |
|---|---|---|
| Art. 4(1) | Definition of personal data | `[PersonalData]` attribute; `DataSensitivity` enum (`Personal`, `Sensitive`, `Special`) |
| Art. 5(1)(b) | Purpose limitation | Host application responsibility; BLite provides collection-level isolation |
| Art. 5(1)(c) | Data minimisation | `WatchOptions.ExcludeFields` / `IncludeOnlyFields`; `[PersonalData]` metadata for field-level awareness |
| Art. 5(1)(e) | Storage limitation | `HasRetentionPolicy(...)` on `EntityTypeBuilder<T>`; `GdprMode.Strict` warns on missing retention |
| Art. 5(1)(f) | Integrity and confidentiality | AES-256-GCM page encryption; WAL encryption; `IBLiteAuditSink` audit trail |
| Art. 15 | Right of access (subject access request) | `ExportSubjectDataAsync` |
| Art. 17 | Right to erasure | `DeleteAsync` + `VacuumAsync` (`SecureErase = true`) |
| Art. 20 | Right to data portability | `ExportSubjectDataAsync` (JSON / BSON export formats) |
| Art. 25 | Data protection by design and by default | `GdprMode.Strict`; `[PersonalData]` compile-time annotation; `CryptoOptions` required in Strict mode |
| Art. 30 | Records of processing activities | `InspectDatabase` → `DatabaseInspectionReport` (collections, indexes, encryption status, retention policies) |
| Art. 32 | Security of processing | AES-256-GCM encryption at-rest; WAL encryption; encrypted backups; `IBLiteAuditSink` |
| Art. 35 | Data protection impact assessment | This document |

---

## 6. Legal Disclaimer

> This document is a technical plan, not legal advice. GDPR compliance depends on the overall application implementation, not on the embedded database alone. The host application acts as the data controller and is responsible for lawful processing, lawful basis, subject communication, and final compliance assessment. Engaging a Data Protection Officer (DPO) for the formal assessment is strongly recommended.
