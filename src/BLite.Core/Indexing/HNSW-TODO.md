# HNSW Vector Index — Task List

File principale: `VectorSearchIndex.cs`, `VectorPage.cs`

---

## P1 — Bloccante (crash / perdita dati)

### 1. `AllocateNode` — buffer overflow su pagine piene

**File**: `VectorSearchIndex.cs`, metodo `AllocateNode`  
**Problema**: assegna sempre `pageId = _rootPageId` senza verificare se la pagina ha slot liberi.  
Con dim=1536 e M=16 ogni nodo occupa ~9.823 B; una pagina da 16 KB ne contiene **esattamente 1**.  
Il secondo insert scrive all'offset 19.706 su un buffer da 16.384 B → crash garantito.  
`VectorPage.GetMaxNodes()` esiste già ma non viene mai chiamato.

**Soluzione**: scorrere la catena `NextPageId` cercando `GetNodeCount < GetMaxNodes`.  
Se nessuna pagina ha spazio, allocarne una nuova e collegarla tramite `NextPageId`.

```
AllocateNode(...)
  └─ while (true)
      ├─ ReadPage(pageId)
      ├─ if GetNodeCount < GetMaxNodes → scrivi nodo, ritorna
      ├─ if header.NextPageId != 0  → pageId = NextPageId
      └─ else → crea nuova pagina, LinkPageChain(prev, new), pageId = new
```

---

### 2. `LinkPageChain` — mancante

**File**: `VectorSearchIndex.cs`  
**Problema**: `AllocateNode` (dopo il fix P1) deve collegare una nuova pagina alla catena impostando  
`header.NextPageId` sulla pagina precedente, ma questo helper non esiste.  
Senza di esso `CollectAllPages` non trova le pagine oltre la root → perdita di nodi al restart.

**Soluzione**: aggiungere un metodo privato:

```csharp
private void LinkPageChain(uint fromPageId, uint toPageId, ITransaction? transaction)
{
    var buffer = RentPageBuffer();
    try
    {
        _storage.ReadPage(fromPageId, transaction?.TransactionId, buffer);
        var header = PageHeader.ReadFrom(buffer);
        header.NextPageId = toPageId;
        header.WriteTo(buffer);
        if (transaction != null)
            _storage.WritePage(fromPageId, transaction.TransactionId, buffer);
        else
            _storage.WritePageImmediate(fromPageId, buffer);
    }
    finally { ReturnPageBuffer(buffer); }
}
```

---

## P2 — Correttezza (recall degradata / loop infiniti)

### 3. `NodeReference` — equality sbagliata in `HashSet`

**File**: `VectorSearchIndex.cs`, struct `NodeReference`  
**Problema**: il metodo `SearchLayer` usa `HashSet<NodeReference>` per tracciare i nodi visitati.  
La struct usa l'equality di default che confronta **tutti e tre** i campi: `PageId`, `NodeIndex`, `MaxLevel`.  
`GetNeighbors` restituisce riferimenti con `MaxLevel = 0` (campo non impostato).  
Se l'entry point ha `MaxLevel > 0`, `visited.Contains(neighborRef)` **non lo trova** → loop infinito  
nei grafi densi quando un nodo viene raggiunto da più percorsi.

**Soluzione**: implementare `IEquatable<NodeReference>` confrontando solo `PageId + NodeIndex`:

```csharp
private struct NodeReference : IEquatable<NodeReference>
{
    public uint PageId;
    public int NodeIndex;
    public int MaxLevel;

    public bool Equals(NodeReference other) =>
        PageId == other.PageId && NodeIndex == other.NodeIndex;

    public override bool Equals(object? obj) =>
        obj is NodeReference n && Equals(n);

    public override int GetHashCode() =>
        HashCode.Combine(PageId, NodeIndex);
}
```

---

### 4. `Link()` — skip silenzioso quando gli slot sono pieni

**File**: `VectorSearchIndex.cs`, metodo `Link`  
**Problema**: se tutti gli slot del livello sono occupati il collegamento viene silenziosamente ignorato.  
I nodi inseriti quando il grafo è denso restano con meno connessioni del previsto → recall degrada  
drasticamente su indici grandi (scenario reale oltre ~1.000 vettori con M=16).

**Soluzione**: implementare il **neighbor shrinking** dell'HNSW paper.  
Se gli slot sono pieni, calcolare la distanza di ogni link esistente; se il nuovo vicino è più vicino  
del peggiore tra quelli presenti, sostituirlo:

```
Link(from, to, level, tx):
  carica links di `from` a `level`
  if slot vuoto trovato → inserisci
  else:
    calcola distanza(from, to)
    trova il link con distanza massima
    if dist(from, to) < dist(from, worst) → sostituisci worst con to
    else → skip (to è più lontano di tutti i vicini attuali)
```

---

## P3 — Qualità (recall sub-ottimale)

### 5. `SelectNeighbors` — manca l'euristica di diversità

**File**: `VectorSearchIndex.cs`, metodo `SelectNeighbors`  
**Problema**: usa `.Take(m)` — prende i primi M candidati per distanza senza considerare la diversità.  
Può selezionare M nodi tutti nello stesso cluster → pochi "ponti" verso altre regioni del grafo →  
recall bassa su query cross-cluster.

**Soluzione**: implementare **Algorithm 4** del paper HNSW originale (Malkov & Yashunin, 2018).  
Un candidato `e` viene aggiunto ai selezionati solo se è più vicino alla query `q`  
di quanto non lo sia a qualsiasi vicino già selezionato `r`:

```
selectNeighbors_heuristic(query, candidates, M):
  result = []
  for e in candidates (ordinati per dist crescente da query):
    if result.IsEmpty OR dist(e, query) < min(dist(e, r) for r in result):
      result.Add(e)
    if result.Count == M: break
  return result
```

Richiede `LoadVector` per ogni coppia (e, r) — costo aggiuntivo accettabile perché eseguito  
solo durante l'inserimento.

---

### 6. `GetRandomLevel` — distribuzione sub-ottimale

**File**: `VectorSearchIndex.cs`, metodo `GetRandomLevel`  
**Problema**: usa `p = 1.0 / M` (per M=16 → p≈0.063) che produce una distribuzione molto ripida.  
L'HNSW paper raccomanda `mL = 1/ln(M)` → per M=16 → mL≈0.36 → livelli più alti sono  
significativamente più frequenti → grafo più connesso e scalabile.

**Soluzione**:

```csharp
private int GetRandomLevel()
{
    double mL = 1.0 / Math.Log(_options.M);
    return (int)Math.Floor(-Math.Log(Random.Shared.NextDouble()) * mL);
    // Cap a un massimo ragionevole (es. 15) per sicurezza
}
```

Note: usa `Random.Shared` (thread-safe da .NET 6+) invece del campo `_random = new(42)` privato  
che non è thread-safe né produce distribuzione diversa tra run (seed fisso 42).

---

## P4 — Performance

### 7. `CreateNewPage` — bypassa la transazione

**File**: `VectorSearchIndex.cs`, metodo `CreateNewPage`  
**Problema**: usa `WritePageImmediate` anche quando una transazione è attiva → la nuova pagina  
viene scritta direttamente su disco invece che nel WAL cache. Se la transazione fa rollback,  
la pagina rimane allocata → leak di pagine.

**Soluzione**: passare il `transaction` a `CreateNewPage` e usare `WritePage(pageId, txId, buffer)`  
quando la transazione è presente.

---

### 8. `Search` — doppio `LoadVector` per risultato

**File**: `VectorSearchIndex.cs`, metodo `Search`  
**Problema**: `SearchLayer` chiama `LoadVector` internamente per ogni candidato durante la ricerca,  
poi il loop di output chiama nuovamente `LoadVector(node, transaction)` per ottenere la distanza  
finale — ogni risultato viene letto due volte.

**Soluzione**: far restituire a `SearchLayer` una struttura `(NodeReference, float distance)` già pronta,  
eliminando la seconda lettura nel loop di output.

---

### 9. `LoadDocumentLocation` — alloca un vettore vuoto inutilmente

**File**: `VectorSearchIndex.cs`, metodo `LoadDocumentLocation`  
**Problema**: chiama `VectorPage.ReadNodeData(buffer, nodeIndex, out loc, out _, new float[0])`  
passando un array vuoto. `ReadNodeData` esegue comunque `vectorSource.CopyTo(vector)` — con  
destinazione vuota è una no-op, ma il cast `MemoryMarshal.Cast<byte, float>` viene eseguito.  
Allocazione `new float[0]` per ogni nodo nei top-k risultati (piccola ma evitabile).

**Soluzione**: aggiungere un overload di `ReadNodeData` che legga solo `loc` e `maxLevel`  
senza toccare il buffer del vettore.

---

## P5 — Funzionalità mancanti

### 10. Cancellazione di un nodo dal grafo

**File**: `VectorSearchIndex.cs`  
**Problema**: non esiste un metodo `Delete(DocumentLocation)`.  
Attualmente `BTreeIndex` ha una remove che aggiorna il B+Tree; `VectorSearchIndex` no.  
I nodi cancellati rimangono nel grafo → risultati ghost nelle ricerche.

**Soluzione minima (soft delete)**:  
Usare un campo bit/flag nel nodo (es. il byte `MaxLevel` → `0xFF` come tombstone) e filtrare  
i risultati in `Search` e in `GetNeighbors`. Nessuna riallocazione richiesta.

**Soluzione corretta (hard delete)**:  
Ricollegare i vicini del nodo rimosso tra loro (HNSW repair), più complessa ma necessaria  
per evitare che la recall degradi nel tempo su indici con molte cancellazioni.

---

### 11. Rebuild / reindex

**File**: `VectorSearchIndex.cs`  
**Problema**: dopo molte cancellazioni (soft delete) il grafo si frammenta.  
Manca un'API `Rebuild()` che reinserisca tutti i nodi non-tombstone in un indice fresco.

---

## Riepilogo priorità

| # | Issue | Impatto | Complessità |
|---|-------|---------|-------------|
| 1 | `AllocateNode` overflow | **crash** | Media |
| 2 | `LinkPageChain` mancante | perdita dati | Bassa |
| 3 | `NodeReference` equality | loop infinito | Bassa |
| 4 | `Link()` skip silenzioso | recall degrada | Media |
| 5 | `SelectNeighbors` euristica | recall sub-ottimale | Media |
| 6 | `GetRandomLevel` distribuzione | qualità grafo | Bassa |
| 7 | `CreateNewPage` bypass tx | page leak | Bassa |
| 8 | Doppio `LoadVector` | -perf query | Bassa |
| 9 | Alloca `float[0]` | micro-perf | Minima |
| 10 | Delete mancante | funzionalità | Alta |
| 11 | Rebuild mancante | manutenzione | Alta |
