# ?? Piano di Ottimizzazione Allocazioni - Zero-Alloc Target

## ?? Allocazioni Identificate (127.98 KB per insert singolo)

### ?? CRITICHE (Alta Priorità)

#### 1. WriteAheadLog.WriteDataRecord
**Problema**: Alloca `new byte[totalSize]` per ogni record (può essere 16KB+)
**Impatto**: ~16KB per insert
**Soluzione**: Usare ArrayPool<byte>.Shared.Rent()

#### 2. DocumentCollection.InsertIntoPage - ToArray()
**Problema**: Linea 412 - `buffer.AsSpan(0, _pageFile.PageSize).ToArray()`
**Impatto**: ~16KB per insert
**Soluzione**: WriteOperation dovrebbe accettare ReadOnlyMemory o lavorare con buffer pooled

#### 3. DocumentCollection Insert - ArrayBufferWriter
**Problema**: Linea 645-646 - Serializzazione alloca
**Impatto**: Variabile (dipende da dimensione documento)
**Soluzione**: Pooled ArrayBufferWriter o buffer riutilizzabili

#### 4. InsertBulk - ToArray() per batch
**Problema**: Linea 748 - `.ToArray()` per ogni documento
**Impatto**: 16KB * batch size
**Soluzione**: Mantenere ReadOnlyMemory invece di byte[]

#### 5. SaveIdMap - ToString() in loop
**Problema**: Linea 232 - `index.ToString()` alloca string
**Impatto**: Piccolo ma ripetuto
**Soluzione**: Usare StringBuilder o format su stack

### ?? MEDIE (Priorità Media)

#### 6. Transaction._writeSet Dictionary
**Problema**: Dictionary che si ridimensiona
**Soluzione**: Pre-size con capacità iniziale

#### 7. String allocations varie
**Problema**: String interpolation, concatenation
**Soluzione**: Usare spans, stackalloc per stringhe temporanee

### ?? BASSE (Ottimizzazioni Finali)

#### 8. BSON Serialization overhead
**Problema**: BsonSpanWriter/Reader potrebbero allocare internamente
**Soluzione**: Audit completo del path di serializzazione

## ?? Implementazione Step-by-Step

### Step 1: WriteAheadLog - ArrayPool per WriteDataRecord ?
- Stimato risparmio: ~16KB per insert
- Complessità: Bassa
- Rischio: Basso

### Step 2: WriteOperation - Passare ownership del buffer ?
- Eliminare ToArray(), passare buffer pooled direttamente
- Stimato risparmio: ~16KB per insert
- Complessità: Media (richiede refactoring)
- Rischio: Medio

### Step 3: DocumentCollection - Riusare buffer di serializzazione ?
- ArrayBufferWriter pooled o buffer riutilizzabile
- Stimato risparmio: ~variabile (50-80%)
- Complessità: Media
- Rischio: Medio

### Step 4: InsertBulk - Eliminare ToArray() ?
- Passare ReadOnlyMemory invece di byte[]
- Stimato risparmio: Significativo per batch
- Complessità: Media
- Rischio: Medio

### Step 5: Micro-ottimizzazioni ?
- Pre-size collections
- Eliminare string allocations
- Stimato risparmio: ~5-10KB
- Complessità: Bassa
- Rischio: Basso

## ?? Target Finale

**Obiettivo**: < 5KB allocazioni per insert singolo (95% riduzione)
**Strategia**: Pool tutti i buffer, zero ToArray(), riuso massivo

## ?? Note Tecniche

- ArrayPool<byte>.Shared.Rent() è thread-safe
- Ricordare sempre di Return() i buffer
- WriteOperation potrebbe aver bisogno di "owns buffer" flag
- Transaction._writeSet potrebbe contenere riferimenti a buffer pooled
- Necessario coordinamento lifetime tra transaction e buffer pool
