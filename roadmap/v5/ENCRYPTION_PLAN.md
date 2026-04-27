# BLite — Development Plan: Encryption

> Date: April 27, 2026  
> References: [RFC.md L894–900](../../RFC.md), [PageFile.cs](../../src/BLite.Core/Storage/PageFile.cs), [WriteAheadLog.cs](../../src/BLite.Core/Transactions/WriteAheadLog.cs)

---

## Overview

BLite currently has no encryption at rest or in transit. RFC.md (L894–900) lists "AES-256 encryption" as a future enhancement. This document defines the architecture, API, and implementation plan — including full support for **multi-file (server) mode**.

### Threat Model

| Threat | Description | Target mitigation |
|--------|-------------|------------------|
| `.db` file theft | File copied by attacker with filesystem access | Encryption at-rest |
| WAL file theft | WAL contains after-images in plaintext | WAL encryption |
| Backup exfiltration | Backup file copied or leaked | Backup encryption |
| Memory scraping | Data in memory during operations | Out of scope (OS-level) |
| Key leakage | Key hard-coded or derived insecurely | Key management API |
| Multi-file partial theft | Only some files stolen (e.g. one collection file) | Per-file subkey derivation |

---

## Architecture

Encryption is handled as a **transparent layer between the in-memory buffer and the disk**. Pages are encrypted before being written to disk and decrypted after being read. The rest of the engine is unchanged.

```
[BLiteEngine]
     │
     ▼
[StorageEngine / DocumentCollection]
     │  reads/writes PageBuffer (plaintext in memory)
     ▼
[EncryptionCoordinator]
     │  resolves ICryptoProvider per file
     ├─► ICryptoProvider (main .db)
     ├─► ICryptoProvider (col0.db)
     ├─► ICryptoProvider (idx0.db)
     └─► ICryptoProvider (WAL)
     │
     ▼
[PageFile / WriteAheadLog]  → disk (ciphertext)
```

The `EncryptionCoordinator` (new class) owns the master key and derives per-file subkeys. Each `PageFile` and `WriteAheadLog` holds a reference to its own `ICryptoProvider` instance.

---

## 1. `ICryptoProvider` Interface

```csharp
// src/BLite.Core/Encryption/ICryptoProvider.cs
namespace BLite.Core.Encryption;

public interface ICryptoProvider : IDisposable
{
    /// <summary>
    /// Encrypts a page in place using a temporary buffer.
    /// The in-memory page buffer is never modified.
    /// </summary>
    void Encrypt(int pageId, ReadOnlySpan<byte> plaintext, Span<byte> ciphertext);

    /// <summary>
    /// Decrypts a page. Input and output have the same logical size.
    /// Throws <see cref="CryptographicException"/> if the authentication tag is invalid.
    /// </summary>
    void Decrypt(int pageId, ReadOnlySpan<byte> ciphertext, Span<byte> plaintext);

    /// <summary>
    /// Returns the 64-byte file header to write at the start of the file
    /// (salt, KDF parameters, version, file role).
    /// </summary>
    ReadOnlySpan<byte> GetFileHeader();

    /// <summary>
    /// Initializes the provider from an existing file header.
    /// </summary>
    void LoadFromFileHeader(ReadOnlySpan<byte> header);
}
```

---

## 2. Built-in implementation: `AesGcmCryptoProvider`

### Algorithm
- **Cipher**: AES-256-GCM (authenticated, tamper-evident)
- **IV/Nonce**: 12 bytes, deterministically derived from `fileRole (1 byte) || fileIndex (2 bytes) || pageId (4 bytes) || databaseSalt (5 bytes)` — globally unique across all files in the same database
- **GCM tag**: 16 bytes, stored after the ciphertext — `PageSize` must be increased by 16 bytes (e.g. 4096 → 4112), or the tag stored in the page header area
- **KDF**: PBKDF2-SHA256 (100,000 iterations) or Argon2id to derive the 32-byte master key from passphrase

### `FileHeader` structure (64 bytes)
```
Offset  Size  Field
  0       4   Magic: 0x424C4345 ("BLCE")
  4       1   Version: 1
  5       1   Algorithm: 1 = AES-256-GCM
  6       1   KDF: 1 = PBKDF2, 2 = Argon2id
  7       1   FileRole: 0 = main, 1 = collection, 2 = index, 3 = WAL
  8      32   Database salt (random, generated at creation, shared across all files)
 40       4   KDF iterations
 44       2   FileIndex (0-based, for collection/index files)
 46      18   Reserved / future use
```

### Files to create
| File | Purpose |
|------|---------|
| `src/BLite.Core/Encryption/ICryptoProvider.cs` | Interface |
| `src/BLite.Core/Encryption/AesGcmCryptoProvider.cs` | AES-256-GCM implementation |
| `src/BLite.Core/Encryption/NullCryptoProvider.cs` | No-op (default when encryption is disabled) |
| `src/BLite.Core/Encryption/EncryptionCoordinator.cs` | Per-file provider factory and master key holder |
| `src/BLite.Core/Encryption/CryptoOptions.cs` | Options (passphrase, KDF algorithm, iterations) |
| `src/BLite.Core/Encryption/KeyDerivation.cs` | PBKDF2 / Argon2id / HKDF helpers |

---

## 3. Integration in `PageFile`

### Read hook (near L264, after `MemoryMappedFile.Read`)
```csharp
// After reading bytes from the file into a temporary buffer:
if (_crypto is not NullCryptoProvider)
    _crypto.Decrypt(pageId, rawBuffer, pageBuffer);
else
    rawBuffer.CopyTo(pageBuffer);
```

### Write hook (before writing to disk)
```csharp
// Before writing pageBuffer to the file:
if (_crypto is not NullCryptoProvider)
{
    _crypto.Encrypt(pageId, pageBuffer, encryptedBuffer); // encryptedBuffer from ArrayPool
    WriteToFile(encryptedBuffer);
}
else
{
    WriteToFile(pageBuffer);
}
```

**Important**: the in-memory page buffer is always plaintext. Encryption/decryption operates on a temporary buffer (stack-allocated or from `ArrayPool<byte>`) to avoid corrupting the page cache.

---

## 4. Integration in `WriteAheadLog`

The WAL must be encrypted separately because it has its own format (sequential records, not fixed pages).

### Approach
- Each WAL record is encrypted with AES-256-GCM using `sequenceNumber || databaseSalt` as the nonce (12 bytes total: 8-byte sequence + 4-byte salt slice)
- The WAL uses `FileRole = 3` for its `ICryptoProvider`, so nonces are globally unique
- `TruncateAsync` (L230) requires no changes — the ciphertext is discarded on truncation

---

## 5. Multi-file (Server Mode) Encryption

In server mode, BLite opens multiple physical files per logical database:

```
mydb.db          — main file (header pages, metadata)
mydb.col0.db     — collection 0 pages
mydb.col1.db     — collection 1 pages
mydb.idx0.db     — index 0 pages
mydb.idx1.db     — index 1 pages
mydb.wal         — Write-Ahead Log
```

### Key challenge: nonce uniqueness
Using the same AES-256-GCM key for all files with a nonce derived only from `pageId` would cause nonce reuse if two files share the same `pageId` value — a catastrophic AES-GCM failure.

### Solution: per-file subkey derivation with HKDF

Each file derives its own 256-bit subkey from the master key using HKDF-SHA256:

```
SubKey_i = HKDF-SHA256(
    inputKeyMaterial: masterKey,          // 32 bytes
    salt:             databaseSalt,       // 32 bytes, from main FileHeader
    info:             fileRole || fileIndex  // 3 bytes context label
)
```

This ensures:
- Each file is encrypted with a **different key** — theft of one file does not expose others
- Nonces within each file can simply be `pageId`-derived without cross-file collision risk
- The master key never touches the disk — only derived subkeys are used at encryption time

### `EncryptionCoordinator`

```csharp
// src/BLite.Core/Encryption/EncryptionCoordinator.cs
public sealed class EncryptionCoordinator : IDisposable
{
    private readonly byte[] _masterKey;       // zeroed on Dispose
    private readonly byte[] _databaseSalt;    // read from main FileHeader

    /// <summary>
    /// Creates or opens the main file crypto provider.
    /// Reads the database salt from the main FileHeader for all subsequent derivations.
    /// </summary>
    public ICryptoProvider CreateForMainFile();

    /// <summary>
    /// Derives a subkey for a collection file and returns its crypto provider.
    /// </summary>
    public ICryptoProvider CreateForCollection(int collectionIndex);

    /// <summary>
    /// Derives a subkey for an index file and returns its crypto provider.
    /// </summary>
    public ICryptoProvider CreateForIndex(int indexIndex);

    /// <summary>
    /// Derives a subkey for the WAL and returns its crypto provider.
    /// </summary>
    public ICryptoProvider CreateForWal();

    public void Dispose()
    {
        CryptographicOperations.ZeroMemory(_masterKey);
    }
}
```

### Multi-file open sequence

```
BLiteEngine.OpenAsync(path, options)
  → IKeyProvider.GetKeyAsync(dbName)           // fetch master key
  → EncryptionCoordinator.Create(masterKey)
  → coordinator.CreateForMainFile()            → PageFile(main, cryptoMain)
  → (for each collection file i)
       coordinator.CreateForCollection(i)      → PageFile(col_i, cryptoCol_i)
  → (for each index file j)
       coordinator.CreateForIndex(j)           → PageFile(idx_j, cryptoIdx_j)
  → coordinator.CreateForWal()                 → WriteAheadLog(wal, cryptoWal)
  → masterKey zeroed in EncryptionCoordinator after all providers are initialized
```

### Adding a new collection or index file at runtime

When a new collection or index file is created (dynamic schema expansion in server mode), `EncryptionCoordinator` must still be alive to derive the subkey for the new file. The coordinator holds only the derived salt and HKDF parameters — not the original passphrase — so it can remain in memory for the engine lifetime without sensitive material exposure.

---

## 6. Backup Encryption

`StorageEngine.Recovery.BackupAsync` must:
1. Re-encrypt each page using the backup `ICryptoProvider` (which may use a different key from the operational one)
2. Write a `FileHeader` for each backed-up file with the backup salt and KDF parameters
3. Include a manifest with per-file SHA-256 checksums

**`BackupOptions` (extended)**
```csharp
public sealed class BackupOptions
{
    /// <summary>
    /// Crypto provider to use for the backup. Null = use the same key as the database.
    /// Useful for cloud backups where a different key is required.
    /// </summary>
    public ICryptoProvider? BackupCryptoProvider { get; init; }

    public bool IncludeAllFiles { get; init; } = true;
    public bool GenerateManifest { get; init; } = true;
}
```

---

## 7. Key Management API

BLite does not manage keys directly. It provides hooks for the host application to integrate its own KMS.

```csharp
// src/BLite.Core/Encryption/IKeyProvider.cs
public interface IKeyProvider
{
    /// <summary>
    /// Returns the 32-byte master key for the database.
    /// The application may retrieve it from Azure Key Vault, AWS KMS, HSM, etc.
    /// </summary>
    ValueTask<ReadOnlyMemory<byte>> GetKeyAsync(string databaseName, CancellationToken ct);

    /// <summary>
    /// Notifies the provider that key rotation is required.
    /// </summary>
    ValueTask NotifyKeyRotationAsync(string databaseName, CancellationToken ct);
}
```

---

## 8. Key Rotation

Key rotation requires rewriting all pages across all files:

```
1. Open database with old key (read-only)
2. IKeyProvider.GetNewKey() → new master key
3. Create new EncryptionCoordinator with new master key
4. For each file: decrypt page (old subkey) → encrypt page (new subkey)
5. Atomically rename new files over old ones
6. Update FileHeader salt in each file
7. Emit audit event KeyRotation
```

**Proposed API**
```csharp
Task RotateEncryptionKeyAsync(
    IKeyProvider newKeyProvider,
    KeyRotationOptions? options = null,
    CancellationToken ct = default
);
```

---

## 9. User Configuration

```csharp
var db = new BLiteEngine(new BLiteEngineOptions
{
    Filename = "mydata.db",
    Encryption = new EncryptionOptions
    {
        // Option 1: direct passphrase (simple apps)
        Passphrase = "my-secret-passphrase",

        // Option 2: external key provider (production)
        KeyProvider = new MyAzureKeyVaultProvider(),

        // KDF parameters
        Algorithm = EncryptionAlgorithm.AesGcm256,
        Kdf = KdfAlgorithm.Pbkdf2,
        KdfIterations = 100_000
    }
});
```

---

## 10. Compatibility and Migration

### Plaintext → Encrypted
```csharp
await BLiteEngine.MigrateToEncryptedAsync(
    existingPath: "old.db",
    newPath: "new.db",
    keyProvider: myKeyProvider
);
```

### Encrypted → Plaintext
```csharp
await BLiteEngine.MigrateToPlaintextAsync(
    existingPath: "old-encrypted.db",
    newPath: "new-plain.db",
    keyProvider: myKeyProvider
);
```

Both operations handle multi-file databases automatically (all collection files, index files, and WAL are migrated together as an atomic unit).

---

## 11. Test Plan

| Category | Test |
|----------|------|
| Unit | `AesGcmCryptoProvider`: encrypt/decrypt round-trip for 1000 random pageIds |
| Unit | Tamper detection: flip 1 byte in encrypted file → `CryptographicException` |
| Unit | `KeyDerivation`: same salt + passphrase → same key (deterministic) |
| Unit | `NullCryptoProvider`: verifies data is unchanged |
| Unit | HKDF subkey derivation: different file roles → different subkeys |
| Unit | Nonce uniqueness: no collisions across 10k pages across 10 files |
| Integration | Open encrypted database with wrong key → clear error |
| Integration | Encrypted backup → restore → data integrity check |
| Integration | Key rotation → database readable with new key, not with old |
| Integration | Multi-file mode: all collection and index files encrypted/decrypted correctly |
| Integration | Add new collection at runtime → new file gets correct subkey |
| Regression | All existing tests pass with `NullCryptoProvider` (default) |
| Security | `.db` file opened in hex editor: no plaintext document visible |
| Security | WAL file: no plaintext data visible |
| Security | Backup file: no plaintext data visible |

---

## Implementation Roadmap

| Phase | Content | Estimate |
|-------|---------|----------|
| **Phase 1** | `ICryptoProvider`, `NullCryptoProvider`, hooks in `PageFile` | 3 d |
| **Phase 2** | `AesGcmCryptoProvider`, `KeyDerivation` (PBKDF2 + HKDF) | 3 d |
| **Phase 3** | WAL encryption | 2 d |
| **Phase 4** | `EncryptionCoordinator` — multi-file subkey derivation | 3 d |
| **Phase 5** | `IKeyProvider`, integration in `BLiteEngine` options | 2 d |
| **Phase 6** | Backup encryption, migration tooling (single + multi-file) | 3 d |
| **Phase 7** | Key rotation | 3 d |
| **Phase 8** | Full test suite | 3 d |

**Total estimate: 22 days**

---

## Security Notes

- Never use **AES-CBC** (padding oracle vulnerability) — only **AES-GCM** or **ChaCha20-Poly1305**
- The AES-GCM nonce must be **unique per (key, pageId)**; the nonce derived from `fileRole || fileIndex || pageId || salt` achieves this globally
- Never log the passphrase, derived key, or salt in plain text in logs, metrics, or audit events
- Zero-fill derived keys after use: `CryptographicOperations.ZeroMemory(keySpan)`
- Follow OWASP recommendations: [Cryptographic Storage Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Cryptographic_Storage_Cheat_Sheet.html)

---

## Related files
- [RFC.md](../../RFC.md)
- [MISSING_FEATURES.md](MISSING_FEATURES.md)
- [GDPR_PLAN.md](GDPR_PLAN.md)
