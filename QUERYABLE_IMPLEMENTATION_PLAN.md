# ?? IQueryable Implementation Plan - High-Performance Query Engine

## ?? Obiettivi

1. **Zero Allocation** per query semplici (quanto possibile)
2. **Index-Aware** - Usa automaticamente indici quando disponibili
3. **Streaming** - Evita materializzazione completa quando non necessaria
4. **Type-Safe** - Sfrutta type safety di LINQ

## ??? Architettura

### Layer 1: Query Primitives (Foundation)
Operazioni atomiche ad alte performance.

#### Core Primitives
```csharp
// Sequential scan (fallback quando nessun indice)
IScanOperator<T> : IEnumerable<T>
  - FullScan(pageId, predicate?) 
  - Yield documenti uno alla volta
  - Zero allocation se usa ref struct enumerator

// Index operations (O(log n) seek, O(k) range scan)
IIndexSeekOperator<T>
  - Seek(key) -> T?
  - Range(min, max) -> IEnumerable<T>
  - Prefix(key) -> IEnumerable<T>

// Filtering
IFilterOperator<T>
  - Apply(source, predicate) -> IEnumerable<T>
  - Push-down quando possibile

// Projection (select)
IProjectOperator<T, TResult>
  - Project(source, selector) -> IEnumerable<TResult>
  - Evita materializzazione intermedia

// Sorting
ISortOperator<T>
  - Sort(source, keySelector, descending) -> IEnumerable<T>
  - Usa indici quando disponibili
  
// Pagination
IPaginationOperator<T>
  - Skip(n), Take(n)
  - Ottimizzato per indici
```

### Layer 2: Query Plan (Logical)
Rappresentazione intermedia della query.

```csharp
QueryPlan
  - Source: TableScan | IndexScan | IndexSeek
  - Filters: List<FilterNode>
  - Projections: List<ProjectionNode>
  - OrderBy: OrderByNode?
  - Skip: int?
  - Take: int?
```

### Layer 3: Expression Visitor
Traduce Expression Tree ? Query Plan.

```csharp
DocumentQueryExpressionVisitor<T>
  - Visit(Expression) -> QueryPlan
  - Supporta:
    * Where()
    * Select()
    * OrderBy() / OrderByDescending()
    * ThenBy()
    * Skip() / Take()
    * First() / FirstOrDefault()
    * Single() / SingleOrDefault()
    * Any() / Count()
```

### Layer 4: Query Optimizer
Ottimizza il query plan.

```csharp
QueryOptimizer
  - ChooseIndex(filters) -> IndexInfo?
  - PushDownPredicates(plan) -> OptimizedPlan
  - EliminateRedundantSorts(plan) -> OptimizedPlan
```

### Layer 5: Query Executor
Esegue il piano ottimizzato.

```csharp
QueryExecutor<T>
  - Execute(QueryPlan) -> IEnumerable<T>
  - ExecuteScalar(QueryPlan) -> T?
  - ExecuteCount(QueryPlan) -> int
```

### Layer 6: Query Provider
Entry point per LINQ.

```csharp
DocumentQueryProvider<T> : IQueryProvider
  - CreateQuery<T>(Expression) -> IQueryable<T>
  - Execute<T>(Expression) -> T

DocumentQueryable<T> : IQueryable<T>
  - Provider: DocumentQueryProvider
  - Expression: Expression
```

## ?? Use Cases Supportati

### Query Base
```csharp
// Full scan with filter
var results = collection.AsQueryable()
    .Where(p => p.Age > 18)
    .ToList();
```

### Query con Indice
```csharp
// Index seek (O(log n))
var person = collection.AsQueryable()
    .Where(p => p.Id == someId)
    .FirstOrDefault();
```

### Query Complessa
```csharp
// Multiple predicates + projection + ordering + pagination
var results = collection.AsQueryable()
    .Where(p => p.Age > 18 && p.City == "NYC")
    .OrderBy(p => p.LastName)
    .ThenBy(p => p.FirstName)
    .Select(p => new { p.Name, p.Email })
    .Skip(10)
    .Take(20)
    .ToList();
```

### Aggregazioni
```csharp
// Count optimized
int count = collection.AsQueryable()
    .Where(p => p.Age > 18)
    .Count();

// Any optimized (short-circuit)
bool hasAdults = collection.AsQueryable()
    .Any(p => p.Age >= 18);
```

## ?? Implementazione Step-by-Step

### Phase 1: Foundation (Alta Priorità)
- [x] Design architettura
- [ ] Implementare Query Primitives
  - [ ] FullScanOperator
  - [ ] IndexSeekOperator
  - [ ] FilterOperator
  - [ ] ProjectOperator
- [ ] Test primitives in isolamento

### Phase 2: Query Plan (Alta Priorità)
- [ ] Definire QueryPlan structs
- [ ] Implementare QueryPlanBuilder
- [ ] Test plan building

### Phase 3: Expression Visitor (Alta Priorità)
- [ ] Implementare DocumentQueryExpressionVisitor
- [ ] Supportare Where()
- [ ] Supportare Select()
- [ ] Supportare OrderBy()
- [ ] Test visitor

### Phase 4: Optimizer (Media Priorità)
- [ ] Index selection logic
- [ ] Predicate push-down
- [ ] Sort elimination
- [ ] Test optimizer

### Phase 5: Executor (Alta Priorità)
- [ ] QueryExecutor implementation
- [ ] Streaming execution
- [ ] Test executor

### Phase 6: Query Provider (Alta Priorità)
- [ ] DocumentQueryProvider
- [ ] DocumentQueryable
- [ ] Integration tests
- [ ] Benchmarks

### Phase 7: Advanced Features (Bassa Priorità)
- [ ] Compiled queries (caching)
- [ ] Query hints
- [ ] Parallel execution
- [ ] Complex joins

## ?? Performance Targets

| Operation | Target | Notes |
|-----------|--------|-------|
| Index Seek | < 50 ?s | O(log n) BTree lookup |
| Full Scan (1K docs) | < 5 ms | Sequential read, zero-alloc |
| Filter (1K docs) | < 2 ms | Predicate evaluation |
| OrderBy (1K docs) | < 10 ms | Use index when possible |
| Count | < 1 ms | Optimized, no materialization |
| Complex Query | < 20 ms | Multiple operations |

## ?? File Structure

```
src/DocumentDb.Core/
??? Query/
?   ??? Primitives/
?   ?   ??? IScanOperator.cs
?   ?   ??? FullScanOperator.cs
?   ?   ??? IIndexSeekOperator.cs
?   ?   ??? IndexSeekOperator.cs
?   ?   ??? FilterOperator.cs
?   ?   ??? ProjectOperator.cs
?   ?   ??? SortOperator.cs
?   ??? Planning/
?   ?   ??? QueryPlan.cs
?   ?   ??? QueryPlanBuilder.cs
?   ?   ??? FilterNode.cs
?   ?   ??? ProjectionNode.cs
?   ??? Visitors/
?   ?   ??? DocumentQueryExpressionVisitor.cs
?   ?   ??? ExpressionAnalyzer.cs
?   ??? Optimization/
?   ?   ??? QueryOptimizer.cs
?   ?   ??? IndexSelector.cs
?   ??? Execution/
?   ?   ??? QueryExecutor.cs
?   ?   ??? StreamingEnumerator.cs
?   ??? Providers/
?       ??? DocumentQueryProvider.cs
?       ??? DocumentQueryable.cs
??? Collections/
    ??? DocumentCollection.cs (add AsQueryable() method)
```

## ?? Design Decisions

### 1. Streaming vs Materialization
**Decisione**: Default streaming, materialize solo quando necessario
**Motivazione**: Riduce allocazioni e migliora performance per large datasets

### 2. Index Selection
**Decisione**: Automatic index selection basata su query predicates
**Motivazione**: Zero configuration, max performance

### 3. Expression Tree Caching
**Decisione**: Cache compiled query plans per query ripetute
**Motivazione**: Parsing Expression Tree è costoso

### 4. Predicate Evaluation
**Decisione**: Usa Func<T, bool> compilate invece di interpretare Expression
**Motivazione**: 10-100x più veloce

## ?? Limitazioni Note (v1)

1. **No Joins**: Solo single-table queries
2. **No Group By**: Da implementare in futuro
3. **Limited Functions**: Solo operatori base supportati
4. **No Async**: Queries sono sincrone (async futuro)

## ?? Test Strategy

### Unit Tests
- Ogni primitive in isolamento
- Expression visitor per ogni LINQ operator
- Optimizer rules

### Integration Tests
- End-to-end query execution
- Index usage verification
- Complex query combinations

### Performance Tests
- Benchmarks vs raw loops
- Memory allocation verification
- Index vs full scan comparison

## ?? References

- [LINQ Provider Implementation](https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/ef/language-reference/how-to-implement-a-linq-provider)
- [Expression Trees Best Practices](https://docs.microsoft.com/en-us/dotnet/csharp/programming-guide/concepts/expression-trees/)
- [MongoDB .NET Driver Query Implementation](https://github.com/mongodb/mongo-csharp-driver)
- [LiteDB LINQ Provider](https://github.com/mbdavid/LiteDB)

## ?? Next Steps

Iniziamo con **Phase 1**: Implementare le Query Primitives base.
Queste sono i building blocks fondamentali su cui costruiremo tutto il resto.

Procediamo?
