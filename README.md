# DocumentDb - BSON Database Engine

Un engine di database ad alte prestazioni basato su documenti BSON per .NET 10.

## 🎯 Design Goals

- **Zero Reflection**: Nessun uso di reflection - tutto risolto a compile-time con source generators
- **Zero Allocation**: Minimizzazione allocazioni heap con `Span<T>`, `ArrayPool<T>`, e `stackalloc`  
- **Alta Performance**: Memory-mapped files, operazioni lock-free, ottimizzazioni CPU cache

## 🏗️ Architecture

### DocumentDb.Bson
Serializzazione BSON zero-allocation con:
- `BsonSpanReader` - Parsing BSON su `ReadOnlySpan<byte>`
- `BsonSpanWriter` - Scrittura BSON su `Span<byte>`
- Strutture dati BSON come `readonly struct` per performance

### DocumentDb.Core  
Storage engine e gestione documenti con:
- `PageFile` - Gestione file basata su pagine con memory-mapped I/O
- `DocumentCollection` - CRUD operations su collezioni
- `Index` - B+Tree e hash indexes
- `Transaction` - ACID transactions con WAL

### DocumentDb.Generators
Source generators per mappatura tipi:
- `BsonMapperGenerator` - Genera implementazioni `IBsonMapper<T>` 
- Attributi: `[BsonDocument]`, `[BsonId]`, `[BsonIgnore]`, `[BsonElement]`

## 🚀 Quick Start

```csharp
// TODO: Examples coming soon
```

## 📊 Performance

Performance benchmarks in arrivo...

## 📝 License

TBD
