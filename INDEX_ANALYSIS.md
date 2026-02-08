# ?? Analisi Sistema di Indicizzazione Esistente

## ?? Componenti Esistenti

### 1. **IndexOptions.cs** ? Già Implementato
```csharp
public enum IndexType : byte
{
    BTree = 1,
    Hash = 2,
    Unique = 3  // ?? Non è un tipo, è un flag!
}

public readonly struct IndexOptions
{
    public IndexType Type { get; init; }
    public bool Unique { get; init; }
    public string[] Fields { get; init; }  // ? Supporta già compound indexes!
    
    // Factory methods
    public static IndexOptions CreateBTree(params string[] fields);
    public static IndexOptions CreateUnique(params string[] fields);
    public static IndexOptions CreateHash(params string[] fields);
}
```

**Problema**: `IndexType.Unique = 3` è confuso - non è un tipo di indice ma un flag!

### 2. **IndexKey.cs** ? Già Implementato
```csharp
public struct IndexKey : IEquatable<IndexKey>, IComparable<IndexKey>
{
    private readonly byte[] _data;
    
    // Constructors per vari tipi
    public IndexKey(ObjectId objectId);
    public IndexKey(int value);
    public IndexKey(long value);
    public IndexKey(string value);
    public IndexKey(ReadOnlySpan<byte> data);
    
    // Comparisons
    public int CompareTo(IndexKey other);
    public bool Equals(IndexKey other);
}
```

**Ottimo**: Supporta già comparazione ordinata, essenziale per BTree!

### 3. **BTreeIndex.cs** ? Già Implementato

#### Metodi Pubblici Esistenti:
```csharp
public sealed class BTreeIndex
{
    // ? Constructor
    public BTreeIndex(PageFile pageFile, IndexOptions options, uint rootPageId = 0);
    
    // ? Insert
    public void Insert(IndexKey key, ObjectId documentId, ITransaction? transaction = null);
    
    // ? Search (TryFind)
    public bool TryFind(IndexKey key, out ObjectId documentId);
    
    // ? Range Scan
    public IEnumerable<IndexEntry> Range(IndexKey minKey, IndexKey maxKey);
    
    // ? Delete
    public bool Delete(IndexKey key, ObjectId documentId, ITransaction? transaction = null);
    
    // ? Properties
    public uint RootPageId { get; }
}
```

**Mancanti**:
- ? `Search(IndexKey)` che ritorna `ObjectId?` (esiste solo TryFind)
- ? `RangeScan(IndexKey?, IndexKey?)` con null per unbounded
- ? Metodo per ottenere statistiche (count, size)

### 4. **HashIndex.cs** ? Implementato

Non lo analizziamo in dettaglio ora, ma esiste per lookup O(1).

### 5. **IndexEntry e InternalEntry** ? Strutture Dati

```csharp
// Per leaf nodes
public readonly struct IndexEntry
{
    public IndexKey Key { get; }
    public ObjectId DocumentId { get; }
}

// Per internal nodes
public readonly struct InternalEntry
{
    public IndexKey Key { get; }
    public uint PageId { get; }
}
```

---

## ? Componenti Mancanti

### 1. **IndexDefinition<T>** ? DA IMPLEMENTARE
**Scopo**: Metadata e configurazione indici custom

**Differenza con IndexOptions**:
- `IndexOptions`: Configurazione basso livello per BTreeIndex (struct leggero)
- `IndexDefinition<T>`: High-level metadata con Expression<Func<T, object>> per estrarre chiavi

**Necessario per**:
- Creare indici su proprietà del documento: `collection.CreateIndex(p => p.Age)`
- Supporto compound indexes: `collection.CreateIndex(p => new { p.City, p.Age })`
- Metadata persistente degli indici

### 2. **SecondaryIndex<T>** ?? FILE ESISTE MA NON È QUELLO GIUSTO!

**Situazione**: Ho creato `SecondaryIndex.cs` ma potrebbe essere un duplicato!
Dobbiamo verificare se esiste un'altra implementazione.

### 3. **IndexManager<T>** ? DA IMPLEMENTARE
**Scopo**: Gestisce collezione di indici e selezione automatica

**Necessario per**:
- Mantenere Dictionary di tutti gli indici
- Automatic index selection per query
- Manutenzione sync su insert/update/delete

### 4. **Integration con DocumentCollection** ? DA IMPLEMENTARE

**API Pubblica Necessaria**:
```csharp
public class DocumentCollection<T>
{
    // Metodi da aggiungere:
    public void CreateIndex<TKey>(Expression<Func<T, TKey>> keySelector, bool unique = false);
    public void CreateIndex<TKey>(string name, Expression<Func<T, TKey>> keySelector, bool unique = false);
    public void DropIndex(string name);
    public IEnumerable<IndexInfo> GetIndexes();
    
    // Interno: manutenzione automatica
    private void InsertIntoAllIndexes(T document, ITransaction? transaction);
    private void UpdateAllIndexes(T oldDoc, T newDoc, ITransaction? transaction);
    private void DeleteFromAllIndexes(T document, ITransaction? transaction);
}
```

---

## ?? Piano di Azione Rivisto

### ? Usare l'Esistente
1. **IndexOptions** - Mantenere per configurazione BTreeIndex
2. **IndexKey** - Perfetto per chiavi tipizzate
3. **BTreeIndex** - Core già funzionante

### ??? Da Aggiungere

#### Phase 1: IndexDefinition<T> (NON conflitto con IndexOptions)
```csharp
public sealed class IndexDefinition<T> where T : class
{
    public string Name { get; }
    public string[] PropertyPaths { get; }  // ["Age"] o ["City", "Age"]
    public Expression<Func<T, object>> KeySelectorExpression { get; }
    public Func<T, object> KeySelector { get; }  // Compiled
    public bool IsUnique { get; }
    public IndexType IndexType { get; }
    
    // Converte a IndexOptions per BTreeIndex
    public IndexOptions ToIndexOptions() => new()
    {
        Type = IndexType,
        Unique = IsUnique,
        Fields = PropertyPaths
    };
}
```

#### Phase 2: SecondaryIndex<T> - Wrapper High-Level
```csharp
public sealed class SecondaryIndex<T> where T : class
{
    private readonly IndexDefinition<T> _definition;
    private readonly BTreeIndex _btreeIndex;  // ? Usa BTreeIndex esistente!
    private readonly IDocumentMapper<T> _mapper;
    
    public void Insert(T document, ITransaction? transaction = null)
    {
        var key = _definition.KeySelector(document);
        var indexKey = ConvertToIndexKey(key);  // CLR object ? IndexKey
        var docId = _mapper.GetId(document);
        _btreeIndex.Insert(indexKey, docId, transaction);
    }
    
    // Update, Delete, Seek, Range...
}
```

#### Phase 3: IndexManager<T>
```csharp
public sealed class IndexManager<T> where T : class
{
    private readonly Dictionary<string, SecondaryIndex<T>> _indexes;
    private readonly PageFile _pageFile;
    private readonly IDocumentMapper<T> _mapper;
    
    public SecondaryIndex<T> CreateIndex(IndexDefinition<T> definition)
    {
        var btreeIndex = new BTreeIndex(_pageFile, definition.ToIndexOptions());
        var secondaryIndex = new SecondaryIndex<T>(definition, btreeIndex, _mapper);
        _indexes[definition.Name] = secondaryIndex;
        return secondaryIndex;
    }
    
    public SecondaryIndex<T>? FindBestIndex(string propertyPath) { ... }
    
    public void InsertIntoAll(T document, ITransaction? transaction)
    {
        foreach (var index in _indexes.Values)
            index.Insert(document, transaction);
    }
}
```

#### Phase 4: DocumentCollection Integration
```csharp
public class DocumentCollection<T>
{
    private readonly IndexManager<T> _indexManager;
    
    public void CreateIndex<TKey>(Expression<Func<T, TKey>> keySelector, bool unique = false)
    {
        var propertyPaths = ExtractPropertyPaths(keySelector);
        var name = GenerateIndexName(propertyPaths);
        
        var definition = new IndexDefinition<T>(
            name, propertyPaths, keySelector, unique);
        
        _indexManager.CreateIndex(definition);
        
        // TODO: Rebuild index for existing documents
    }
    
    public ObjectId Insert(T entity, ITransaction? transaction = null)
    {
        // ... existing code ...
        
        // NEW: Insert into all secondary indexes
        _indexManager.InsertIntoAll(entity, transaction);
        
        return id;
    }
}
```

---

## ?? Conflitti Rilevati

### 1. IndexType Enum - CONFLITTO!
**Problema**: Ho creato nuovo `IndexType` enum in `IndexDefinition.cs`
**Esistente**: C'è già `IndexType` in `IndexOptions.cs` (con valori BTree=1, Hash=2, Unique=3)

**Soluzione**: 
- ? NON creare nuovo IndexType
- ? Usare quello esistente ma FIX: rimuovere `Unique=3` (non è un tipo!)
- ? Unique dovrebbe essere solo un bool flag

### 2. SecondaryIndex.cs - POSSIBILE DUPLICATO
**Situazione**: Ho creato file ma non sono sicuro se ne esiste già uno

**Soluzione**: Verificare con file_search se esiste già

---

## ?? Prossimi Step

1. ? **Analisi completata**
2. ?? **Fix IndexType enum** - rimuovere Unique=3, è solo un flag
3. ?? **Verificare SecondaryIndex esistente** - potrebbe già esserci
4. ? **Creare IndexDefinition<T>** - NON conflitto, è high-level
5. ? **Creare/Aggiornare SecondaryIndex<T>** - wrapper su BTreeIndex esistente
6. ? **Creare IndexManager<T>**
7. ? **Integrare in DocumentCollection**

---

## ?? Architettura Finale

```
DocumentCollection<T>
    ?
IndexManager<T>
    ?
SecondaryIndex<T> (wrapper)
    ?
BTreeIndex (existing, low-level)
    ?
PageFile (storage)
```

**Separation of Concerns**:
- `IndexOptions` + `BTreeIndex`: Low-level, gestione B+Tree su disco
- `IndexDefinition<T>` + `SecondaryIndex<T>`: High-level, typed, expression-based
- `IndexManager<T>`: Orchestrazione, selezione automatica indici

Procediamo?
