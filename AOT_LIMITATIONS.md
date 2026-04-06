# AOT and Trim Compatibility

## Overview

BLite.Core 4.x includes `IsAotCompatible` (enabled for `net10.0` target) and properly annotates
all APIs that require dynamic code or untrimmable reflection with `[RequiresDynamicCode]` and
`[RequiresUnreferencedCode]`. This ensures that:

1. When publishing a trimmed or NativeAOT application, users receive **specific, actionable
   warnings at their call sites** rather than the vague rollup warnings
   `IL2104: Assembly 'BLite.Core' produced trim warnings` and
   `IL3053: Assembly 'BLite.Core' produced AOT analysis warnings`.
2. BLite.Core's own build validates annotations via the `EnableAotAnalyzer` and
   `EnableTrimAnalyzer` properties that `IsAotCompatible=true` enables.

---

## Annotated (AOT-unfriendly) APIs

The following public APIs are annotated and will produce warnings at call sites in AOT/trimmed
publish scenarios. This is intentional and expected — the warnings direct you to the specific
lines that need attention.

### LINQ queries (`AsQueryable`, `FindAsync`, `FindOneAsync`)

```csharp
[RequiresDynamicCode("...")]
[RequiresUnreferencedCode("...")]
public IBLiteQueryable<T> AsQueryable();

[RequiresDynamicCode("...")]
[RequiresUnreferencedCode("...")]
public IAsyncEnumerable<T> FindAsync(Expression<Func<T, bool>> predicate, ...);

[RequiresDynamicCode("...")]
[RequiresUnreferencedCode("...")]
public Task<T?> FindOneAsync(Expression<Func<T, bool>> predicate, ...);
```

**Why:** The LINQ provider (`BTreeQueryProvider`) uses `Expression.Lambda.Compile()`,
`MakeGenericMethod`, and reflection to resolve `Enumerable` methods at runtime. These
operations require dynamic code generation and untrimmable type metadata.

**Workaround for AOT:** Use non-LINQ APIs:
- `FindByIdAsync(id)` — direct primary-key lookup; fully AOT-safe
- `FindAllAsync()` — full collection scan; fully AOT-safe
- `ScanAsync(predicate)` — raw BSON predicate scan; fully AOT-safe if `BsonReaderPredicate` is
  a static delegate (no captures that require reflection)
- `QueryIndexAsync(...)` — direct index lookup; fully AOT-safe

### Index creation (`CreateIndexAsync`, `CreateVectorIndexAsync`, `EnsureIndexAsync`)

```csharp
[RequiresDynamicCode("...")]
public Task<ICollectionIndex<TId, T>> CreateIndexAsync<TKey>(...);

[RequiresDynamicCode("...")]
public Task<ICollectionIndex<TId, T>> CreateVectorIndexAsync<TKey>(...);

[RequiresDynamicCode("...")]
public Task<ICollectionIndex<TId, T>> EnsureIndexAsync<TKey>(...);
```

**Why:** Index creation compiles the key-selector expression (`Expression<Func<T, TKey>>`) into a
delegate via `Expression.Compile()`.

**Workaround for AOT:** Pre-create indexes using direct index management APIs if they exist, or
accept the warning and suppress it if you are certain your types are preserved.

### Schema generation (`GetSchema`, `BsonSchemaGenerator.FromType`)

```csharp
[RequiresUnreferencedCode("...")]
public virtual BsonSchema GetSchema();

[RequiresUnreferencedCode("...")]
public static BsonSchema FromType<T>();
```

**Why:** Schema generation walks the CLR type hierarchy using `Type.GetProperties()`,
`Type.GetFields()`, and `Type.GetInterfaces()` — all of which require type metadata that may be
trimmed.

### Value converter registration (`ValueConverterRegistry`)

```csharp
[RequiresDynamicCode("...")]
[RequiresUnreferencedCode("...")]
public ValueConverterRegistry(Dictionary<string, Type> converterTypes);
```

**Why:** The registry discovers the `ConvertToProvider` method via `Type.GetMethod()` and
compiles an expression-tree delegate using `Expression.Compile()`.

**Workaround for AOT:** Instead of using runtime-discovered converters, register compile-time
known converters by implementing a custom `IDocumentMapper<TId, T>` that performs the conversion
directly in `Serialize`/`Deserialize`.

---

## Private-setter fallback in generated mapper code

The source generator emits a `CreateSetter<TObj, TVal>` helper for entity types that have
**private or init-only setters** on properties from a **referenced assembly**. This path:

1. First attempts `Expression.Lambda.Compile()` to create a typed setter delegate.
2. Falls back to walking the type hierarchy via `Type.GetProperty()` and invoking the setter
   via `MethodInfo.Invoke` if the expression tree path fails (which can happen for setters
   declared in a referenced assembly).

**Example scenario that triggers the fallback:**

```csharp
// In AssemblyA
public class InitOnlyModel
{
    public string Name { get; private set; }
}

// In AssemblyB (your project using BLite)
public class Container
{
    public InitOnlyModel Item { get; set; }
}
```

When the source generator processes `Container`, it detects that `InitOnlyModel.Name` has a
private setter. The generated mapper for `Container` will include the `CreateSetter` helper
with pragma suppressions and `[RequiresDynamicCode]`/`[RequiresUnreferencedCode]` attributes.

**Impact:** If your model uses such types, the generated code will include a `CreateSetter`
method annotated with AOT-unfriendly attributes. You will see warnings at the point where the
generated static field initialiser calls `CreateSetter(...)`.

**Planned fix:** A future release (targeting 4.2.0) will introduce a source-generator path that
avoids the `MethodInfo.Invoke` fallback for these scenarios, making the generated mapper
fully AOT-safe.

**Immediate workaround:**
- Avoid using entity types (from referenced assemblies) with private setters in your document
  models.
- Or add `[UnconditionalSuppressMessage("AOT", "IL3050")]` at the specific call site if you
  are sure the types involved are preserved.

---

## Summary table

| API / Scenario                             | AOT-safe? | Workaround available?                     |
|--------------------------------------------|-----------|-------------------------------------------|
| `FindByIdAsync`, `FindAllAsync`, `ScanAsync` | ✅ Yes    | N/A — use these directly                  |
| `QueryIndexAsync`                           | ✅ Yes    | N/A — use this directly                   |
| `FindAsync`, `FindOneAsync`, `AsQueryable`  | ❌ No     | Use scan/index APIs above                 |
| `CreateIndexAsync`, `EnsureIndexAsync`      | ❌ No     | Pre-create indexes at startup             |
| `BsonSchemaGenerator.FromType`              | ❌ No     | Implement `IDocumentMapper` manually      |
| `ValueConverterRegistry` constructor        | ❌ No     | Convert in custom mapper `Serialize`      |
| Generated mapper (no private setters)       | ✅ Yes    | N/A — fully safe path                     |
| Generated mapper (private/init setter from external assembly) | ❌ No | Avoid external private setters or suppress |

---

## Tracking issue

See the [BLite GitHub issues](https://github.com/EntglDb/BLite/issues) tracker for the original
IL2104/IL3053 report and planned improvements for AOT compatibility in 4.2.0.
