# Piano di correzione — Encryption (analisi vs baseline 4.4.2)

Documento di lavoro derivato dall'analisi del modulo encryption corrente.
Nessuna regressione di correttezza identificata rispetto a 4.4.2.
Le voci sotto sono ordinate per priorità.

## Priorità Alta — chiusura gap funzionale

- [x] **P1 — `DocumentDbContext` costruttori encryption-aware**
  - Aggiungere overload `protected DocumentDbContext(string databasePath, CryptoOptions crypto, BLiteKvOptions? kvOptions = null)`.
  - Aggiungere overload `protected DocumentDbContext(string databasePath, EncryptionCoordinator coordinator, PageFileConfig? config = null, BLiteKvOptions? kvOptions = null)`.
  - File: `src/BLite.Core/DocumentDbContext.cs`.
  - Simmetria con i costruttori già presenti in `BLiteEngine`.
  - Test: aggiungere copertura in `tests/BLite.Tests/EncryptionTests.cs` (round-trip insert/query con `DocumentDbContext` cifrato).

- [ ] **P2 — Validazione combinazioni invalide**
  - In `BLiteEngine` e `DocumentDbContext`: se `PageFileConfig.IsServerLayout` (collection dir / index path settati) e viene passato `CryptoOptions`, lanciare `ArgumentException` con messaggio che indica `EncryptionCoordinator` come scelta corretta.
  - Decidere se preferire factory statiche `Encrypted(...)` / `EncryptedServer(...)` come API pubblica forte.

- [ ] **P3 — `MemoryPageStorage`: rifiutare crypto provider non-null**
  - In `StorageEngine.Memory.cs` (apertura in-memory): se `config.CryptoProvider != null && !(it is NullCryptoProvider)` lanciare `NotSupportedException("Encryption is not supported for in-memory storage")`.
  - Evita silenzioso no-op che inganna l'utente.

## Priorità Media — robustezza concorrenza/security

- [x] **P4 — `volatile` su campi pubblicati cross-thread**
  - `EncryptionCoordinator._databaseSalt` → `volatile byte[]?` o `Volatile.Read/Write`.
  - `AesGcmCryptoProvider._aesGcm` → `volatile AesGcm?`.
  - `CoordinatedFileProvider._aesGcm` → idem.

- [x] **P5 — `CryptographicOperations.ZeroMemory` su `_key`**
  - In `AesGcmCryptoProvider.Dispose`: sostituire `Array.Clear(_key, 0, _key.Length)` con `CryptographicOperations.ZeroMemory(_key)`.
  - Allineamento a `CoordinatedFileProvider.InitialiseAesGcm` che lo fa già correttamente.

- [x] **P6 — `Interlocked.CompareExchange` su `SetDatabaseSalt`** (lock-based)
  - In `EncryptionCoordinator.SetDatabaseSalt`: assegnamento atomico al primo set, mantenendo il check di mismatch sui successivi.

- [x] **P7 — Passphrase via `byte[]` / `ReadOnlySpan<byte>`**
  - Aggiungere costruttore `CryptoOptions(ReadOnlySpan<byte> passphraseBytes, ...)`.
  - Permette zeroing del segreto e bypass di `string interning`.
  - Mantenere il costruttore `string` per back-compat.

- [x] **P8 — Default PBKDF2 iterations 100k → 600k**
  - `CryptoOptions.DefaultIterations` aggiornato a 600\_000 (OWASP 2023).
  - File esistenti continuano a usare il valore salvato in header → no regressione di compatibilità.
  - Aggiornare i test che usano `iterations: 1` (sono già espliciti, OK).
  - Aggiornare documentazione `CHANGELOG.md`.

## Priorità Bassa — pulizia API/doc

- [x] **P9 — XMLDoc su `PageFileConfig.CryptoProvider`**
  - Documentare la regola: "Use `EncryptionCoordinator.CreateForMainFile()` when the layout is server-mode".

- [x] **P10 — Documentare ownership di `ICryptoProvider`**
  - In `WriteAheadLog`, `PageFile`, `StorageEngine`: chiarire che provider distinti vengono creati via `CreateSiblingProvider` per evitare double-dispose.

- [x] **P11 — Estrarre `CoordinatedFileProvider`**
  - Spostare in file dedicato `internal sealed class CoordinatedFileProvider`.
  - Riduce la dimensione di `EncryptionCoordinator.cs` (~430 LoC) e abilita unit test dedicato.

- [x] **P12 — Benchmark encryption baseline**
  - Aggiungere `EncryptionBenchmarks` in `tests/BLite.Benchmark/` per: page Encrypt, page Decrypt, WAL record encrypt, open-time PBKDF2 single vs multi-file.

- [x] **P13 — Decidere su `KdfAlgorithm` enum**
  - Un solo valore `Pbkdf2Sha256`: o documentare roadmap (Argon2id) o renderlo `internal`.

- [x] **P14 — `EncryptionCoordinator` come componente interno**
  - Razionale: il coordinator è plumbing obbligatorio in modalità multi-file (deriva subkey HKDF distinte per main/index/wal/collection per evitare nonce-reuse). L'utente non deve mai costruirlo direttamente: l'unico input è la master key.
  - `EncryptionCoordinator` è ora `internal sealed`; i factory method `CreateForMainFile/Collection/Index/Wal` sono `internal`.
  - Aggiunta `CryptoOptions.FromMasterKey(ReadOnlySpan<byte> masterKey)` come unico entry point pubblico per la modalità HKDF/multi-file (master key 32 byte, copiata e zeroizzata in `ClearSecret`).
  - Il ctor `BLiteEngine(string, CryptoOptions, ...)` (e l'analogo `DocumentDbContext`) fa dispatch automatico in base a `crypto.IsMasterKeyMode`: passphrase ⇒ single-file PBKDF2; master key ⇒ server layout con coordinator costruito internamente. L'engine/context possiede il coordinator e ne effettua il `Dispose` (zero della master key) alla propria dispose.
  - I ctor `(string, EncryptionCoordinator, ...)` esistenti su `BLiteEngine` e `DocumentDbContext` sono ora `internal` (visibili solo ai test via `InternalsVisibleTo`).
  - Aggiunto `InternalsVisibleTo("BLite.Shared")` su `BLite.Core` e `InternalsVisibleTo("BLite.Tests"/"BLite.NetStandard21.Tests")` su `BLite.Shared` per consentire ai test esistenti di continuare a usare il path coordinator-based.
  - File: `src/BLite.Core/Encryption/CryptoOptions.cs`, `src/BLite.Core/Encryption/EncryptionCoordinator.cs`, `src/BLite.Core/BLiteEngine.cs`, `src/BLite.Core/DocumentDbContext.cs`, `src/BLite.Core/BLite.Core.csproj`, `tests/BLite.Shared/BLite.Shared.csproj`, `tests/BLite.Shared/MultiFileTestContext.cs`, `tests/BLite.Tests/EncryptionTests.cs`.
  - Test: 70/70 encryption pass; 2157 net10.0 + 20 net6.0 totale verde.
  - Supersede parzialmente P11 (l'estrazione di `CoordinatedFileProvider` resta valida come refactor strutturale interno, ma non è più correlata alla superficie pubblica).

---

## Stato

| ID | Descrizione | Stato |
|----|-------------|-------|
| P1 | DocumentDbContext encryption ctors | ✅ done |
| P2 | Validazione combinazioni invalide | ⏭️ n/a (non raggiungibile dalle API attuali) |
| P3 | MemoryPageStorage rifiuta crypto | ⏭️ n/a (non raggiungibile dalle API attuali) |
| P4 | volatile su campi crypto | ✅ done |
| P5 | ZeroMemory su _key | ✅ done |
| P6 | Interlocked SetDatabaseSalt | ✅ done (lock-based) |
| P7 | Passphrase byte[] / ClearSecret | ✅ done |
| P8 | PBKDF2 default 600k (OWASP 2023) | ✅ done |
| P9 | XMLDoc PageFileConfig | ✅ done |
| P10 | Doc ownership crypto provider | ✅ done |
| P11 | Estrarre CoordinatedFileProvider | ✅ done |
| P12 | Benchmarks encryption | ✅ done |
| P13 | KdfAlgorithm enum | ✅ done (documented as forward-compatible) |
| P14 | EncryptionCoordinator internal + FromMasterKey | ✅ done |
