# BLite Audit & Performance Monitoring — Piano di Implementazione

> Revisione: March 2026  
> Target: `BLite.Core` v3.2.0 — net10.0 + netstandard2.1

---

## Indice

1. [Panoramica](#1-panoramica)
2. [Struttura dei file da creare / modificare](#2-struttura-dei-file-da-creare--modificare)
3. [FASE 1 — Sink + Metriche in-process](#fase-1--sink--metriche-in-process)
   - [1.1 Event record types](#11-event-record-types)
   - [1.2 IBLiteAuditSink interface](#12-ibliteauditsink-interface)
   - [1.3 BLiteAuditOptions](#13-bliteauditoptions)
   - [1.4 BLiteMetrics](#14-blitemetrics)
   - [1.5 Iniezione in StorageEngine](#15-iniezione-in-storageengine)
   - [1.6 Hook in CommitTransaction](#16-hook-in-committransaction)
   - [1.7 Hook in InsertDataCore (DocumentCollection)](#17-hook-in-insertdatacore-documentcollection)
   - [1.8 Hook in BTreeQueryProvider.Execute](#18-hook-in-btreequeryprovidere-execute)
   - [1.9 Esposizione in BLiteEngine e DocumentDbContext](#19-esposizione-in-bliteengine-e-documentdbcontext)
4. [FASE 2 — DiagnosticSource / Activity + Slow Query](#fase-2--diagnosticsource--activity--slow-query)
   - [2.1 BLiteDiagnostics (ActivitySource + DiagnosticSource)](#21-blitediagnostics-activitysource--diagnosticsource)
   - [2.2 Activity in CommitTransaction](#22-activity-in-committransaction)
   - [2.3 Activity in Execute<TResult>](#23-activity-in-executetresult)
   - [2.4 Slow query / slow commit detection](#24-slow-query--slow-commit-detection)
   - [2.5 Propagazione strategia IndexOptimizer → sink](#25-propagazione-strategia-indexoptimizer--sink)
5. [Compatibilità netstandard2.1](#5-compatibilità-netstandard21)
6. [Test da scrivere](#6-test-da-scrivere)

---

## 1. Panoramica

Il sistema di audit si compone di tre elementi ortogonali:

```
┌───────────────────────────────────────────────────────────────┐
│  BLiteAuditOptions  (configurazione, iniettata nel costruttore)│
│  ├── IBLiteAuditSink?         (callback utente — Fase 1)       │
│  ├── BLiteMetrics?            (contatori Interlocked — Fase 1) │
│  ├── SlowQueryThreshold       (TimeSpan — Fase 2)              │
│  └── EnableDiagnosticSource   (bool — Fase 2)                  │
├───────────────────────────────────────────────────────────────┤
│  Chokepoint 1: StorageEngine.CommitTransaction(ulong)          │
│  Chokepoint 2: DocumentCollection.InsertDataCore(...)          │
│  Chokepoint 3: BTreeQueryProvider.Execute<TResult>(...)        │
└───────────────────────────────────────────────────────────────┘
```

**Principio zero-overhead:** tutti gli hook sono protetti da un guard `_auditOptions is null` o `_sink is null`. Con opzioni non configurate, il JIT elimina i branch per dead-code elimination.

---

## 2. Struttura dei file da creare / modificare

```
src/BLite.Core/
├── Audit/                          ← NUOVA CARTELLA
│   ├── AuditEvents.cs              ← NUOVO — record types degli eventi
│   ├── IBLiteAuditSink.cs          ← NUOVO — interfaccia pubblica
│   ├── BLiteAuditOptions.cs        ← NUOVO — configurazione
│   └── BLiteMetrics.cs             ← NUOVO — contatori in-memory (Fase 1)
│   └── BLiteDiagnostics.cs         ← NUOVO — ActivitySource (Fase 2)
│
├── Storage/
│   ├── StorageEngine.cs            ← MODIFICA — aggiungere campo _auditOptions
│   └── StorageEngine.Transactions.cs ← MODIFICA — hook in CommitTransaction
│
├── Collections/
│   └── DocumentCollection.cs       ← MODIFICA — hook in InsertDataCore
│
├── Query/
│   └── BTreeQueryProvider.cs       ← MODIFICA — hook in Execute<TResult>
│
├── BLiteEngine.cs                  ← MODIFICA — nuovi costruttori con BLiteAuditOptions
└── DocumentDbContext.cs            ← MODIFICA — nuovi costruttori con BLiteAuditOptions
```

---

## FASE 1 — Sink + Metriche in-process

### 1.1 Event record types

**File da creare:** `src/BLite.Core/Audit/AuditEvents.cs`

```csharp
namespace BLite.Core.Audit;

/// <summary>Emesso al completamento di ogni CommitTransaction.</summary>
public readonly record struct CommitAuditEvent(
    ulong TransactionId,
    string CollectionName,   // "" se commit coinvolge più collection o è cross-collection
    int PagesWritten,
    int WalSizeBytes,
    TimeSpan Elapsed);

/// <summary>Emesso al completamento di ogni InsertDataCore.</summary>
public readonly record struct InsertAuditEvent(
    ulong TransactionId,
    string CollectionName,
    int DocumentSizeBytes,
    TimeSpan Elapsed);

/// <summary>Emesso al completamento di ogni Execute<TResult> in BTreeQueryProvider.</summary>
public readonly record struct QueryAuditEvent(
    string CollectionName,
    QueryStrategy Strategy,      // Fase 2: index name propagato qui
    string? IndexName,           // null se Strategy != IndexScan
    int ResultCount,
    TimeSpan Elapsed);

/// <summary>Emesso quando un'operazione supera la soglia configurata (Fase 2).</summary>
public readonly record struct SlowOperationEvent(
    SlowOperationType OperationType,
    string CollectionName,
    TimeSpan Elapsed,
    string? Detail);             // es. espressione LINQ o nome indice

public enum QueryStrategy : byte
{
    Unknown = 0,
    IndexScan = 1,
    BsonScan = 2,
    FullScan = 3
}

public enum SlowOperationType : byte
{
    Insert = 1,
    Query = 2,
    Commit = 3
}
```

---

### 1.2 IBLiteAuditSink interface

**File da creare:** `src/BLite.Core/Audit/IBLiteAuditSink.cs`

```csharp
namespace BLite.Core.Audit;

/// <summary>
/// Interfaccia implementabile dall'utente per ricevere eventi di audit.
/// Tutti i metodi vengono chiamati in modo sincrono nel thread dell'operazione.
/// Implementazioni lente (es. scrittura su file) devono accodarsi internamente.
/// </summary>
public interface IBLiteAuditSink
{
    void OnInsert(InsertAuditEvent e)  { }  // default: no-op (interfaccia con default impl)
    void OnQuery(QueryAuditEvent e)    { }
    void OnCommit(CommitAuditEvent e)  { }
    void OnSlowOperation(SlowOperationEvent e) { }  // Fase 2 — può essere ignorato in Fase 1
}
```

> **Nota:** i default methods (`{ }`) richiedono C# 8+ e sono già compatibili con il progetto (`LangVersion=latest`). Su netstandard2.1 i default interface methods sono supportati dal runtime .NET Core 3.0+.

---

### 1.3 BLiteAuditOptions

**File da creare:** `src/BLite.Core/Audit/BLiteAuditOptions.cs`

```csharp
namespace BLite.Core.Audit;

public sealed class BLiteAuditOptions
{
    /// <summary>Sink custom dell'utente. Se null, nessun callback viene invocato.</summary>
    public IBLiteAuditSink? Sink { get; set; }

    /// <summary>
    /// Se non null, popola il singleton BLiteMetrics accessibile via BLiteEngine.Metrics
    /// e DocumentDbContext.Metrics.
    /// </summary>
    public bool EnableMetrics { get; set; } = false;

    // ── Fase 2 ──────────────────────────────────────────────────────────────────

    /// <summary>
    /// Soglia oltre la quale viene emesso un SlowOperationEvent sul sink.
    /// null = soglia disabilitata.
    /// </summary>
    public TimeSpan? SlowQueryThreshold { get; set; }

    /// <summary>
    /// Se true, emette System.Diagnostics.Activity tramite BLiteDiagnostics.ActivitySource.
    /// Richiede un listener OpenTelemetry o DiagnosticListener attivo per avere overhead.
    /// </summary>
    public bool EnableDiagnosticSource { get; set; } = false;
}
```

---

### 1.4 BLiteMetrics

**File da creare:** `src/BLite.Core/Audit/BLiteMetrics.cs`

```csharp
using System.Threading;

namespace BLite.Core.Audit;

/// <summary>
/// Contatori cumulativi thread-safe. Accessibili via BLiteEngine.Metrics / DocumentDbContext.Metrics.
/// Tutti gli aggiornamenti usano Interlocked per ~10-20 ns overhead.
/// </summary>
public sealed class BLiteMetrics
{
    private long _totalInserts;
    private long _totalQueriesIndexScan;
    private long _totalQueriesBsonScan;
    private long _totalQueriesFullScan;
    private long _totalCommits;
    private long _pageCacheHits;       // hit su _walCache o _walIndex
    private long _pageCacheMisses;     // lettura da PageFile
    private long _totalInsertMs;       // per calcolo media mobile
    private long _totalQueryMs;

    // ── Lettura (snapshot istantaneo) ───────────────────────────────────────────
    public long TotalInserts            => Interlocked.Read(ref _totalInserts);
    public long TotalQueriesIndexScan   => Interlocked.Read(ref _totalQueriesIndexScan);
    public long TotalQueriesBsonScan    => Interlocked.Read(ref _totalQueriesBsonScan);
    public long TotalQueriesFullScan    => Interlocked.Read(ref _totalQueriesFullScan);
    public long TotalCommits            => Interlocked.Read(ref _totalCommits);
    public long PageCacheHits           => Interlocked.Read(ref _pageCacheHits);
    public long PageCacheMisses         => Interlocked.Read(ref _pageCacheMisses);

    public double AvgInsertMs =>
        _totalInserts == 0 ? 0 : (double)Interlocked.Read(ref _totalInsertMs) / _totalInserts;

    public double AvgQueryMs =>
        TotalQueries == 0 ? 0 : (double)Interlocked.Read(ref _totalQueryMs) / TotalQueries;

    public long TotalQueries =>
        TotalQueriesIndexScan + TotalQueriesBsonScan + TotalQueriesFullScan;

    public double CacheHitRate
    {
        get
        {
            var total = PageCacheHits + PageCacheMisses;
            return total == 0 ? 0 : (double)PageCacheHits / total;
        }
    }

    // ── Aggiornamento (interno a BLite.Core) ────────────────────────────────────
    internal void RecordInsert(TimeSpan elapsed)
    {
        Interlocked.Increment(ref _totalInserts);
        Interlocked.Add(ref _totalInsertMs, (long)elapsed.TotalMilliseconds);
    }

    internal void RecordQuery(QueryStrategy strategy, TimeSpan elapsed)
    {
        ref long counter = ref strategy switch
        {
            QueryStrategy.IndexScan => ref _totalQueriesIndexScan,
            QueryStrategy.BsonScan  => ref _totalQueriesBsonScan,
            _                       => ref _totalQueriesFullScan
        };
        Interlocked.Increment(ref counter);
        Interlocked.Add(ref _totalQueryMs, (long)elapsed.TotalMilliseconds);
    }

    internal void RecordCommit()   => Interlocked.Increment(ref _totalCommits);
    internal void RecordCacheHit() => Interlocked.Increment(ref _pageCacheHits);
    internal void RecordCacheMiss()=> Interlocked.Increment(ref _pageCacheMisses);

    /// <summary>Azzera tutti i contatori (utile per test o reset periodico).</summary>
    public void Reset()
    {
        Interlocked.Exchange(ref _totalInserts, 0);
        Interlocked.Exchange(ref _totalQueriesIndexScan, 0);
        Interlocked.Exchange(ref _totalQueriesBsonScan, 0);
        Interlocked.Exchange(ref _totalQueriesFullScan, 0);
        Interlocked.Exchange(ref _totalCommits, 0);
        Interlocked.Exchange(ref _pageCacheHits, 0);
        Interlocked.Exchange(ref _pageCacheMisses, 0);
        Interlocked.Exchange(ref _totalInsertMs, 0);
        Interlocked.Exchange(ref _totalQueryMs, 0);
    }
}
```

> **Nota netstandard2.1:** `Interlocked.Read(ref long)` non esiste su netstandard2.1 (solo su .NET 5+). Usare `Interlocked.CompareExchange(ref field, 0, 0)` come polyfill, oppure aggiungere un `#if NETSTANDARD2_1` block. Il progetto ha già il file `Polyfills.cs` — aggiungere lì il metodo helper se necessario.

---

### 1.5 Iniezione in StorageEngine

**File da modificare:** `src/BLite.Core/Storage/StorageEngine.cs`

#### A) Aggiungere campo
```csharp
// dopo: private readonly SemaphoreSlim _commitLock = new(1, 1);
private BLiteAuditOptions? _auditOptions;
```

#### B) Aggiungere metodo di configurazione (preferibile all'iniezione nel costruttore per non rompere le firme)
```csharp
/// <summary>Configura il sistema di audit. Chiamato da BLiteEngine/DocumentDbContext dopo la costruzione.</summary>
internal void ConfigureAudit(BLiteAuditOptions options)
{
    _auditOptions = options;
    if (options.EnableMetrics)
        Metrics = new BLiteMetrics();
}

internal BLiteMetrics? Metrics { get; private set; }
```

> Alternativa: passare `BLiteAuditOptions?` come parametro opzionale nel costruttore `StorageEngine(string, PageFileConfig, BLiteAuditOptions?)`. Scegliere l'approccio più coerente con lo stile del progetto. Il metodo `ConfigureAudit` è più safe perché non richiede modifiche a tutti i siti di costruzione interni.

---

### 1.6 Hook in CommitTransaction

**File da modificare:** `src/BLite.Core/Storage/StorageEngine.Transactions.cs`

Metodo target: `public void CommitTransaction(ulong transactionId)`

Il metodo attualmente:
1. Acquisce `_commitLock`
2. Scrive records WAL
3. Muove `_walCache → _walIndex`
4. Auto-checkpoint se WAL > 4 MB

#### Modifica — wrappare con Stopwatch:

```csharp
public void CommitTransaction(ulong transactionId)
{
    // ── AUDIT: start ─────────────────────────────────────────────────────────
    var sw = _auditOptions is not null ? System.Diagnostics.Stopwatch.StartNew() : null;
    // ─────────────────────────────────────────────────────────────────────────

    _commitLock.Wait();
    try
    {
        // ... codice esistente invariato ...

        var pagesWritten = /* già calcolato nel loop esistente */ walEntries.Count;

        // ... flush WAL esistente ...
    }
    finally
    {
        _commitLock.Release();
    }

    // ── AUDIT: emit ───────────────────────────────────────────────────────────
    if (sw is not null)
    {
        sw.Stop();
        var walSize = (int)_wal.GetCurrentSize();
        var elapsed = sw.Elapsed;

        _auditOptions!.Sink?.OnCommit(new CommitAuditEvent(
            TransactionId: transactionId,
            CollectionName: string.Empty,   // StorageEngine non conosce la collection
            PagesWritten: 0,                // TODO Fase 1.6b: estrarre contatore pagine
            WalSizeBytes: walSize,
            Elapsed: elapsed));

        Metrics?.RecordCommit();
    }
    // ─────────────────────────────────────────────────────────────────────────
}
```

> **Nota sul conteggio pagine:** il loop di scrittura WAL usa `_walCache[transactionId]` — la dimensione del dizionario prima dello spostamento in `_walIndex` è il numero di pagine scritte. Catturarla con `var pagesWritten = _walCache.TryGetValue(transactionId, out var pages) ? pages.Count : 0;` **prima** del `_commitLock.Wait()` non è safe perché concorrente. Catturarla **dopo** aver acquisito il lock, prima del loop WAL, è il momento corretto.

Stessa modifica da replicare in `CommitTransactionAsync` per il path async.

---

### 1.7 Hook in InsertDataCore (DocumentCollection)

**File da modificare:** `src/BLite.Core/Collections/DocumentCollection.cs`

Metodo target: `private void InsertDataCore(TId id, T entity, ReadOnlySpan<byte> docData)` (linea ~1461)

`DocumentCollection` non ha accesso diretto a `_auditOptions` — lo ottiene tramite `_storage.Metrics` e un campo `IBLiteAuditSink?` passato in costruzione oppure recuperato da `StorageEngine`.

**Approccio consigliato:** esporre `_storage.Metrics` e `_storage._auditOptions` tramite una property interna di `StorageEngine`:

```csharp
// In StorageEngine.cs:
internal IBLiteAuditSink? AuditSink => _auditOptions?.Sink;
```

Poi in `DocumentCollection.InsertDataCore`:

```csharp
private void InsertDataCore(TId id, T entity, ReadOnlySpan<byte> docData)
{
    // ── AUDIT: start ────────────────────────────────────────────────────────
    var sw = _storage.AuditSink is not null || _storage.Metrics is not null
        ? System.Diagnostics.Stopwatch.StartNew()
        : null;
    // ────────────────────────────────────────────────────────────────────────

    // ... codice esistente invariato ...
    // _primaryIndex.Insert(...)
    // _indexManager.InsertIntoAll(...)
    // NotifyCdc(...)

    // ── AUDIT: emit ─────────────────────────────────────────────────────────
    if (sw is not null)
    {
        sw.Stop();
        var elapsed = sw.Elapsed;
        var docSize = docData.Length;
        var txnId = /* transaction.TransactionId già disponibile nel metodo */ 0UL;

        var evt = new InsertAuditEvent(txnId, _collectionName, docSize, elapsed);
        _storage.AuditSink?.OnInsert(evt);
        _storage.Metrics?.RecordInsert(elapsed);
    }
    // ────────────────────────────────────────────────────────────────────────
}
```

> **Nota:** `InsertDataCore` ottiene già il transaction dalla chiamata `GetCurrentTransactionOrStart()` — conservare il riferimento in una variabile locale `var transaction = ...` per passare `transaction.TransactionId` all'evento.

---

### 1.8 Hook in BTreeQueryProvider.Execute

**File da modificare:** `src/BLite.Core/Query/BTreeQueryProvider.cs`

Metodo target: `public TResult Execute<TResult>(Expression expression)`

`BTreeQueryProvider` ha accesso a `_collection` (di tipo `DocumentCollection<TId, T>`), che ha accesso a `_storage`. Rispettare lo stesso pattern di accesso:

```csharp
public TResult Execute<TResult>(Expression expression)
{
    // ── AUDIT: start ────────────────────────────────────────────────────────
    var sw = _collection._storage.AuditSink is not null || _collection._storage.Metrics is not null
        ? System.Diagnostics.Stopwatch.StartNew()
        : null;
    var strategy = QueryStrategy.Unknown;
    string? indexName = null;
    // ────────────────────────────────────────────────────────────────────────

    // [1] Visit expression → model
    var model = BTreeExpressionVisitor.Visit(expression);

    // [2] Push-down select
    if (/* condizioni esistenti */)
    {
        // ... codice esistente ...
    }

    // [3] Data fetch — modificare per catturare strategy:
    IEnumerable<T> source;
    if (IndexOptimizer.TryOptimize(model, _collection._indexManager.GetIndexes(), out var indexResult))
    {
        strategy = QueryStrategy.IndexScan;        // ← NUOVO
        indexName = indexResult.IndexName;         // ← NUOVO
        source = /* chiamata esistente */;
    }
    else if (BsonExpressionEvaluator.TryCompile<T>(model.WhereClause, out var predicate))
    {
        strategy = QueryStrategy.BsonScan;         // ← NUOVO
        source = _collection.Scan(predicate);
    }
    else
    {
        strategy = QueryStrategy.FullScan;         // ← NUOVO
        source = _collection.FindAll();
    }

    // [4/5] Pipeline esistente... conta risultati per l'audit:
    var result = ExecutePipeline<TResult>(model, source, whereAlreadyApplied: strategy == QueryStrategy.IndexScan);
    var resultCount = /* estrarre da result se è IEnumerable; altrimenti -1 */ -1;

    // ── AUDIT: emit ─────────────────────────────────────────────────────────
    if (sw is not null)
    {
        sw.Stop();
        var elapsed = sw.Elapsed;
        var collName = _collection.CollectionName;  // esporre se non già pubblico

        var evt = new QueryAuditEvent(collName, strategy, indexName, resultCount, elapsed);
        _collection._storage.AuditSink?.OnQuery(evt);
        _collection._storage.Metrics?.RecordQuery(strategy, elapsed);
    }
    // ────────────────────────────────────────────────────────────────────────

    return result;
}
```

> **Nota sul `resultCount`:** `ExecutePipeline` restituisce già il risultato finale tipizzato. Se `TResult` è `List<T>` o `T[]`, il conteggio è disponibile. Se è `int` (Count query) è il risultato stesso. Per semplicità in Fase 1, passare `-1` (unknown) e risolvere in Fase 2 con un contatore nel pipeline.

> **Nota su `_collection.CollectionName`:** il campo `_collectionName` è `private` in `DocumentCollection`. Va reso `internal` oppure si aggiunge una property `internal string CollectionName => _collectionName;`.

---

### 1.9 Esposizione in BLiteEngine e DocumentDbContext

**File da modificare:** `src/BLite.Core/BLiteEngine.cs`

```csharp
// Aggiungere costruttore overload:
public BLiteEngine(string databasePath, BLiteAuditOptions auditOptions)
    : this(databasePath, PageFileConfig.DetectFromFile(databasePath) ?? PageFileConfig.Default, null)
{
    _storage.ConfigureAudit(auditOptions);
}

public BLiteEngine(string databasePath, PageFileConfig config, BLiteAuditOptions auditOptions)
    : this(databasePath, config, null)
{
    _storage.ConfigureAudit(auditOptions);
}

// Aggiungere property pubblica:
/// <summary>Metriche in-process. Non null solo se BLiteAuditOptions.EnableMetrics = true.</summary>
public BLiteMetrics? Metrics => _storage.Metrics;
```

**File da modificare:** `src/BLite.Core/DocumentDbContext.cs`

Stesso pattern: aggiungere overload del costruttore che accetta `BLiteAuditOptions` e chiama `_storage.ConfigureAudit(options)` al termine della costruzione (dopo `InitializeCollections()`). Aggiungere `public BLiteMetrics? Metrics => _storage.Metrics;`.

---

## FASE 2 — DiagnosticSource / Activity + Slow Query

### 2.1 BLiteDiagnostics (ActivitySource + DiagnosticSource)

**File da creare:** `src/BLite.Core/Audit/BLiteDiagnostics.cs`

```csharp
using System.Diagnostics;

namespace BLite.Core.Audit;

/// <summary>
/// Sorgenti diagnostiche statiche per l'integrazione OpenTelemetry.
/// Attive solo se esiste almeno un listener (IsEnabled check a ~5 ns).
/// </summary>
public static class BLiteDiagnostics
{
    public const string ActivitySourceName = "BLite";
    public const string ActivitySourceVersion = "3.2.0";

    /// <summary>
    /// ActivitySource per l'integrazione con OpenTelemetry / Application Insights.
    /// Aggiungere il listener con: ActivitySource.AddActivityListener(...)
    /// oppure via AddOpenTelemetry().WithTracing(b => b.AddSource("BLite"))
    /// </summary>
    public static readonly ActivitySource ActivitySource =
        new(ActivitySourceName, ActivitySourceVersion);

    // Nomi delle Activity — costanti per evitare allocazione di stringhe
    public const string CommitActivityName  = "blite.commit";
    public const string InsertActivityName  = "blite.insert";
    public const string QueryActivityName   = "blite.query";
}
```

> **Dipendenza:** `System.Diagnostics.DiagnosticSource` è inclusa nel BCL da .NET Core 3.0+. Per netstandard2.1 è disponibile tramite il pacchetto NuGet `System.Diagnostics.DiagnosticSource`. Verificare se `BLite.Core.csproj` ne ha già bisogno (attualmente non ce l'ha). Se si vuole evitare la dipendenza: wrappare tutto in `#if NET5_0_OR_GREATER` e fornire un no-op per netstandard2.1.

---

### 2.2 Activity in CommitTransaction

**File da modificare:** `src/BLite.Core/Storage/StorageEngine.Transactions.cs`

Aggiungere **all'inizio** di `CommitTransaction(ulong transactionId)`, prima del Stopwatch della Fase 1:

```csharp
using var activity = _auditOptions?.EnableDiagnosticSource == true
    ? BLiteDiagnostics.ActivitySource.StartActivity(BLiteDiagnostics.CommitActivityName)
    : null;

activity?.SetTag("db.system", "blite");
activity?.SetTag("db.blite.transaction_id", transactionId.ToString());
```

Al termine, prima di emettere il sink:

```csharp
activity?.SetTag("db.blite.pages_written", pagesWritten.ToString());
activity?.SetTag("db.blite.wal_size_bytes", walSize.ToString());
// In caso di eccezione (nel catch):
// activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
```

---

### 2.3 Activity in Execute<TResult>

**File da modificare:** `src/BLite.Core/Query/BTreeQueryProvider.cs`

```csharp
using var activity = _collection._storage.AuditOptions?.EnableDiagnosticSource == true
    ? BLiteDiagnostics.ActivitySource.StartActivity(BLiteDiagnostics.QueryActivityName)
    : null;

activity?.SetTag("db.system", "blite");
activity?.SetTag("db.collection.name", _collection.CollectionName);

// Dopo la scelta della strategia (punto [3]):
activity?.SetTag("db.blite.query_strategy", strategy.ToString());
activity?.SetTag("db.blite.index_name", indexName ?? "none");

// Dopo ExecutePipeline:
activity?.SetTag("db.blite.result_count", resultCount.ToString());
```

---

### 2.4 Slow query / slow commit detection

**Modifiche a:** `StorageEngine.Transactions.cs` e `BTreeQueryProvider.cs`

Aggiungere al termine dell'emit della Fase 1, dopo `Metrics?.RecordCommit()`:

```csharp
// In CommitTransaction — slow commit detection:
if (_auditOptions!.SlowQueryThreshold is { } threshold && elapsed > threshold)
{
    _auditOptions.Sink?.OnSlowOperation(new SlowOperationEvent(
        SlowOperationType.Commit,
        CollectionName: string.Empty,
        Elapsed: elapsed,
        Detail: $"TxnId={transactionId}, Pages={pagesWritten}"));
}
```

```csharp
// In BTreeQueryProvider.Execute — slow query detection:
if (_collection._storage.AuditOptions?.SlowQueryThreshold is { } threshold && elapsed > threshold)
{
    _collection._storage.AuditSink?.OnSlowOperation(new SlowOperationEvent(
        SlowOperationType.Query,
        CollectionName: _collection.CollectionName,
        Elapsed: elapsed,
        Detail: $"Strategy={strategy}, Index={indexName ?? "none"}, Results={resultCount}"));
}
```

> **Accesso a `AuditOptions`:** aggiungere `internal BLiteAuditOptions? AuditOptions => _auditOptions;` in `StorageEngine.cs` analogamente ad `AuditSink`.

---

### 2.5 Propagazione strategia IndexOptimizer → sink

**File da modificare:** `src/BLite.Core/Query/IndexOptimizer.cs`

Attualmente `IndexOptimizer.TryOptimize()` restituisce `bool`. Per trasmettere il nome dell'indice utilizzato al sink senza allocazioni aggiuntive, usare un parametro `out`:

```csharp
// Firma attuale (verificare il file reale):
public static bool TryOptimize<T>(QueryModel model, IReadOnlyList<IndexDefinition> indexes, ...)

// Firma modificata:
public static bool TryOptimize<T>(
    QueryModel model,
    IReadOnlyList<IndexDefinition> indexes,
    ...,
    out string? selectedIndexName)   // ← NUOVO parametro out
```

Tutti i siti di chiamata esistenti che ignorano il nome passano `out _` (C# discard).

---

## 5. Compatibilità netstandard2.1

| Problema | Soluzione |
|---|---|
| `Interlocked.Read(ref long)` non esiste | Usare `Interlocked.CompareExchange(ref field, 0, 0)` — aggiungere extension in `Polyfills.cs` |
| `default interface methods` richiedono .NET Core 3.0+ runtime | Già garantito: netstandard2.1 non gira su .NET Framework |
| `System.Diagnostics.ActivitySource` non disponibile su netstandard2.1 | Wrappare con `#if NET5_0_OR_GREATER` o aggiungere pacchetto NuGet `System.Diagnostics.DiagnosticSource >= 8.0.0` |
| `readonly record struct` richiede C# 10+ | Già garantito: `LangVersion=latest` nel `.csproj` |

---

## 6. Test da scrivere

Aggiungere in `tests/BLite.Tests/` (già ha `InternalsVisibleTo`):

### Test Fase 1

```csharp
// AuditSinkTests.cs

[Fact]
public void Insert_ShouldInvokeAuditSink()
{
    var sink = new RecordingSink();
    var opts = new BLiteAuditOptions { Sink = sink };
    using var engine = new BLiteEngine(":memory:", opts);   // verificare se supporta path in-memory

    engine.Insert("test", engine.CreateDocument(/* ... */));
    engine.Commit();

    Assert.Single(sink.Inserts);
    Assert.True(sink.Inserts[0].Elapsed > TimeSpan.Zero);
    Assert.Equal("test", sink.Inserts[0].CollectionName);
}

[Fact]
public void Query_ShouldInvokeAuditSink_WithStrategy()
{
    // ...
    Assert.Equal(QueryStrategy.FullScan, sink.Queries[0].Strategy);
}

[Fact]
public void Metrics_TotalInserts_IncrementedByOne()
{
    var opts = new BLiteAuditOptions { EnableMetrics = true };
    using var engine = new BLiteEngine(tempPath, opts);

    engine.Insert("col", /* doc */);
    engine.Commit();

    Assert.Equal(1, engine.Metrics!.TotalInserts);
}

// Helper:
private sealed class RecordingSink : IBLiteAuditSink
{
    public List<InsertAuditEvent> Inserts { get; } = new();
    public List<QueryAuditEvent> Queries  { get; } = new();
    public List<CommitAuditEvent> Commits { get; } = new();
    public List<SlowOperationEvent> SlowOps { get; } = new();

    public void OnInsert(InsertAuditEvent e)       => Inserts.Add(e);
    public void OnQuery(QueryAuditEvent e)         => Queries.Add(e);
    public void OnCommit(CommitAuditEvent e)       => Commits.Add(e);
    public void OnSlowOperation(SlowOperationEvent e) => SlowOps.Add(e);
}
```

### Test Fase 2

```csharp
[Fact]
public void SlowQuery_ShouldTriggerSlowOperationEvent()
{
    var sink = new RecordingSink();
    var opts = new BLiteAuditOptions
    {
        Sink = sink,
        SlowQueryThreshold = TimeSpan.Zero  // threshold = 0 → qualsiasi query è "slow"
    };
    // ... inserire dati e fare query ...
    Assert.NotEmpty(sink.SlowOps);
    Assert.Equal(SlowOperationType.Query, sink.SlowOps[0].OperationType);
}

[Fact]
public void ActivitySource_ShouldEmitActivity_WhenListenerIsPresent()
{
    var activities = new List<Activity>();
    using var listener = new ActivityListener
    {
        ShouldListenTo = source => source.Name == "BLite",
        Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
        ActivityStopped = a => activities.Add(a)
    };
    ActivitySource.AddActivityListener(listener);

    var opts = new BLiteAuditOptions { EnableDiagnosticSource = true };
    // ... eseguire commit ...

    Assert.Contains(activities, a => a.OperationName == BLiteDiagnostics.CommitActivityName);
}
```

---

## Riepilogo delle modifiche al codebase

| File | Tipo | Note |
|---|---|---|
| `Audit/AuditEvents.cs` | NUOVO | Record types, zero dipendenze |
| `Audit/IBLiteAuditSink.cs` | NUOVO | Interfaccia pubblica con default impl |
| `Audit/BLiteAuditOptions.cs` | NUOVO | Configurazione |
| `Audit/BLiteMetrics.cs` | NUOVO | Contatori Interlocked |
| `Audit/BLiteDiagnostics.cs` | NUOVO (Fase 2) | ActivitySource static |
| `Storage/StorageEngine.cs` | MODIFICA | + campo `_auditOptions`, `ConfigureAudit()`, `AuditSink`, `AuditOptions`, `Metrics` |
| `Storage/StorageEngine.Transactions.cs` | MODIFICA | Hook Stopwatch in `CommitTransaction(ulong)` e `CommitTransactionAsync` |
| `Collections/DocumentCollection.cs` | MODIFICA | Hook in `InsertDataCore`, `CollectionName` internal, `_storage.AuditSink` |
| `Query/BTreeQueryProvider.cs` | MODIFICA | Hook in `Execute<TResult>`, strategy tracking |
| `Query/IndexOptimizer.cs` | MODIFICA (Fase 2) | `out string? selectedIndexName` param |
| `BLiteEngine.cs` | MODIFICA | Nuovi overload costruttore + `Metrics` property |
| `DocumentDbContext.cs` | MODIFICA | Nuovi overload costruttore + `Metrics` property |
| `Polyfills.cs` | MODIFICA | Polyfill `Interlocked.Read` per netstandard2.1 |
| `tests/BLite.Tests/AuditSinkTests.cs` | NUOVO | Test Fase 1 + 2 |

**Stima righe di codice:** ~350 nuove righe (Audit/), ~80 righe di modifica ai file esistenti.
