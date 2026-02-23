<template>
  <div class="doc-page">
    <h1>🔍 Querying</h1>
    <p class="lead">Master BLite's powerful LINQ-based query engine with automatic index optimization.</p>

    <section>
      <h2>Basic Queries</h2>
      <pre><code><span class="keyword">var</span> results = users.AsQueryable()
    .Where(u => u.Age > <span class="number">25</span>)
    .AsEnumerable();

<span class="keyword">foreach</span> (<span class="keyword">var</span> user <span class="keyword">in</span> results)
{
    Console.WriteLine(user.Name);
}</code></pre>
    </section>

    <section>
      <h2>Filtering</h2>
      
      <h3>Comparison Operators</h3>
      <pre><code><span class="comment">// Equality</span>
.Where(u => u.Name == <span class="string">"Alice"</span>)

<span class="comment">// Greater than / Less than</span>
.Where(u => u.Age > <span class="number">30</span>)
.Where(u => u.Age <= <span class="number">50</span>)

<span class="comment">// Range</span>
.Where(u => u.Age >= <span class="number">18</span> && u.Age <= <span class="number">65</span>)</code></pre>

      <h3>String Operations</h3>
      <pre><code><span class="comment">// StartsWith (optimized with B-Tree index)</span>
.Where(u => u.Name.StartsWith(<span class="string">"A"</span>))

<span class="comment">// Contains (full scan)</span>
.Where(u => u.Email.Contains(<span class="string">"@example.com"</span>))

<span class="comment">// Case-insensitive</span>
.Where(u => u.Name.ToLower() == <span class="string">"alice"</span>)</code></pre>

      <h3>Logical Operators</h3>
      <pre><code><span class="comment">// AND</span>
.Where(u => u.Age > <span class="number">25</span> && u.Name.StartsWith(<span class="string">"A"</span>))

<span class="comment">// OR</span>
.Where(u => u.Age < <span class="number">18</span> || u.Age > <span class="number">65</span>)

<span class="comment">// NOT</span>
.Where(u => !(u.Age >= <span class="number">18</span> && u.Age <= <span class="number">65</span>))</code></pre>
    </section>

    <section>
      <h2>Sorting</h2>
      <pre><code><span class="comment">// Ascending</span>
.OrderBy(u => u.Age)

<span class="comment">// Descending</span>
.OrderByDescending(u => u.Name)

<span class="comment">// Multiple sort keys</span>
.OrderBy(u => u.Age)
.ThenBy(u => u.Name)</code></pre>
    </section>

    <section>
      <h2>Pagination</h2>
      <pre><code><span class="keyword">int</span> page = <span class="number">2</span>;
<span class="keyword">int</span> pageSize = <span class="number">10</span>;

<span class="keyword">var</span> results = users.AsQueryable()
    .OrderBy(u => u.Id)
    .Skip((page - <span class="number">1</span>) * pageSize)
    .Take(pageSize)
    .AsEnumerable();</code></pre>
    </section>

    <section>
      <h2>Projection</h2>
      
      <h3>Select Specific Fields</h3>
      <pre><code><span class="keyword">var</span> names = users.AsQueryable()
    .Select(u => u.Name)
    .AsEnumerable();

<span class="comment">// Anonymous types</span>
<span class="keyword">var</span> summary = users.AsQueryable()
    .Select(u => <span class="keyword">new</span> { u.Name, u.Age })
    .AsEnumerable();</code></pre>

      <h3>Push-down Projection <span style="font-size:0.75em;color:#06b6d4;font-weight:600;">NEW in v1.5.0</span></h3>
      <p>When a query contains only scalar-field projections, BLite compiles the SELECT lambda into a single-pass raw-BSON reader. The source type <code>T</code> is <strong>never instantiated</strong> — only the requested bytes are read off the page.</p>
      <pre><code><span class="comment">// Only Name and Age bytes are read — User is never allocated</span>
<span class="keyword">var</span> dtos = users.AsQueryable()
    .Select(u => <span class="keyword">new</span> UserDto(u.Name, u.Age))
    .ToList();

<span class="comment">// WHERE + SELECT: both evaluated in a single BSON pass</span>
<span class="keyword">var</span> adults = users.AsQueryable()
    .Where(u => u.Age >= <span class="number">18</span>)
    .Select(u => <span class="keyword">new</span> { u.Name, u.Age })
    .ToList();</code></pre>
      <div class="info-box">
        <strong>⚡ When push-down fires:</strong> SELECT with scalar fields only, with or without a WHERE clause. Queries that include <code>OrderBy</code> / <code>Take</code> / <code>Skip</code> <em>after</em> the projection still benefit — the ordering/paging is applied on the already-projected <code>IEnumerable&lt;TResult&gt;</code>. Complex/nested fields (e.g. <code>x.Address</code>) automatically fall back to the standard path.
      </div>
    </section>

    <section>
      <h2>Async LINQ <span style="font-size:0.75em;color:#06b6d4;font-weight:600;">v1.4.0+</span></h2>
      <p><code>AsQueryable()</code> returns <code>IBLiteQueryable&lt;T&gt;</code>, which preserves the async chain across all LINQ operators.</p>
      <pre><code><span class="comment">// Async terminal operators</span>
<span class="keyword">var</span> list  = <span class="keyword">await</span> users.AsQueryable().Where(u => u.Age > <span class="number">25</span>).ToListAsync();
<span class="keyword">var</span> count = <span class="keyword">await</span> users.AsQueryable().CountAsync();
<span class="keyword">var</span> any   = <span class="keyword">await</span> users.AsQueryable().AnyAsync(u => u.Active);

<span class="comment">// Async streaming via IAsyncEnumerable&lt;T&gt;</span>
<span class="keyword">await foreach</span> (<span class="keyword">var</span> user <span class="keyword">in</span> users.AsQueryable().Where(u => u.Age > <span class="number">25</span>).AsAsyncEnumerable())
{
    Console.WriteLine(user.Name);
}</code></pre>
    </section>

    <section>
      <h2>Aggregations</h2>
      <pre><code><span class="comment">// Count</span>
<span class="keyword">var</span> total = users.AsQueryable().Count();
<span class="keyword">var</span> adults = users.AsQueryable().Count(u => u.Age >= <span class="number">18</span>);

<span class="comment">// Sum, Min, Max, Average</span>
<span class="keyword">var</span> totalAge = users.AsQueryable().Sum(u => u.Age);
<span class="keyword">var</span> youngest = users.AsQueryable().Min(u => u.Age);
<span class="keyword">var</span> oldest = users.AsQueryable().Max(u => u.Age);
<span class="keyword">var</span> avgAge = users.AsQueryable().Average(u => u.Age);</code></pre>
    </section>

    <section>
      <h2>GroupBy</h2>
      <pre><code><span class="keyword">var</span> byAge = users.AsQueryable()
    .GroupBy(u => u.Age)
    .Select(g => <span class="keyword">new</span>
    {
        Age = g.Key,
        Count = g.Count(),
        Names = g.Select(u => u.Name).ToList()
    })
    .AsEnumerable();</code></pre>
    </section>

    <section>
      <h2>Index Optimization</h2>
      <p>BLite automatically uses B-Tree indexes when available:</p>
      <pre><code><span class="comment">// Create an index on Age</span>
users.EnsureIndex(u => u.Age);

<span class="comment">// This query now uses the index</span>
<span class="keyword">var</span> results = users.AsQueryable()
    .Where(u => u.Age > <span class="number">30</span>) <span class="comment">// ← Index hit!</span>
    .AsEnumerable();

<span class="comment">// Enum properties can also be indexed (v1.7.0)</span>
orders.EnsureIndex(o => o.Status);
<span class="keyword">var</span> shipped = orders.AsQueryable()
    .Where(o => o.Status == OrderStatus.Shipped)
    .AsEnumerable();</code></pre>

      <div class="info-box">
        <strong>💡 Tip:</strong> Use <code>.Explain()</code> to see query execution plans and verify index usage. Enum index keys are stored as their underlying integer value (<code>int</code>, <code>byte</code>, <code>long</code>) — ordering and range queries work correctly.
      </div>
    </section>

    <section>
      <h2>Advanced Examples</h2>
      
      <h3>Complex Filter</h3>
      <pre><code><span class="keyword">var</span> results = users.AsQueryable()
    .Where(u => 
        u.Age >= <span class="number">25</span> && 
        u.Age <= <span class="number">50</span> && 
        u.Name.StartsWith(<span class="string">"A"</span>) &&
        u.Email.Contains(<span class="string">"@company.com"</span>)
    )
    .OrderBy(u => u.Age)
    .ThenBy(u => u.Name)
    .Take(<span class="number">100</span>)
    .AsEnumerable();</code></pre>
    </section>
  </div>
</template>

<style scoped>
.doc-page { max-width: 800px; }
h1 { font-size: 2.5rem; font-weight: 800; margin-bottom: 16px; }
.lead { font-size: 1.2rem; color: var(--text-secondary); margin-bottom: 48px; line-height: 1.7; }
section { margin-bottom: 48px; }
h2 { font-size: 1.8rem; font-weight: 700; margin-bottom: 16px; color: var(--blite-red); border-bottom: 2px solid rgba(231, 76, 60, 0.2); padding-bottom: 8px; }
h3 { font-size: 1.3rem; font-weight: 600; margin: 24px 0 12px; }
p { margin-bottom: 16px; line-height: 1.7; color: var(--text-secondary); }
code { font-family: var(--font-mono); font-size: 0.9rem; background: rgba(231, 76, 60, 0.1); padding: 2px 6px; border-radius: 4px; color: var(--blite-red); }
pre { background: rgba(10, 10, 10, 0.6); border: 1px solid rgba(231, 76, 60, 0.2); border-radius: 8px; padding: 20px; overflow-x: auto; margin: 16px 0; }
pre code { background: none; padding: 0; color: var(--text-secondary); }
.keyword { color: var(--blite-red); }
.type { color: #06b6d4; }
.string { color: #a1a1aa; }
.number { color: #06b6d4; }
.comment { color: #52525b; font-style: italic; }
.info-box { padding: 16px 20px; border-radius: 8px; margin: 24px 0; border-left: 4px solid #06b6d4; background: rgba(6, 182, 212, 0.05); }
</style>
