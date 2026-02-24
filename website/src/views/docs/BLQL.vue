<template>
  <div class="doc-page">
    <h1>🔎 <span class="title-gradient">BLQL — BLite Query Language</span></h1>
    <p class="lead">
      BLQL is the <strong>BLite Query Language</strong> for schema-less scenarios. Inspired by MQL (MongoDB Query Language),
      it lets you filter, sort, project, and page <code>BsonDocument</code> results from a
      <code>DynamicCollection</code> using either a <strong>JSON string</strong> or a <strong>fluent C# API</strong> —
      no compile-time types required.
    </p>

    <div class="info-box">
      <div class="info-header">📌 BLQL vs LINQ</div>
      <table>
        <thead>
          <tr><th></th><th>LINQ (<code>DocumentDbContext</code>)</th><th>BLQL (<code>DynamicCollection</code>)</th></tr>
        </thead>
        <tbody>
          <tr><td><strong>Entry point</strong></td><td><code>.AsQueryable()</code></td><td><code>.Query()</code> / <code>.Query(filterJson)</code></td></tr>
          <tr><td><strong>Type safety</strong></td><td>✅ Compile-time</td><td>Runtime <code>BsonDocument</code></td></tr>
          <tr><td><strong>JSON string queries</strong></td><td>—</td><td>✅ MQL-style</td></tr>
          <tr><td><strong>Fluent C# filter API</strong></td><td>Lambda expressions</td><td><code>BlqlFilter.*</code> factories</td></tr>
          <tr><td><strong>Projection</strong></td><td>LINQ <code>Select</code></td><td><code>BlqlProjection.Include/Exclude</code></td></tr>
          <tr><td><strong>Injection protection</strong></td><td>✅ Type-safe</td><td>✅ Unknown <code>$</code> operators rejected</td></tr>
        </tbody>
      </table>
    </div>

    <!-- ═══════════════════════════════════════════════════════ Getting Started -->
    <section>
      <h2>Getting Started</h2>
      <p>Import the namespace and call <code>.Query()</code> on any <code>DynamicCollection</code>:</p>
      <pre><code><span class="keyword">using</span> BLite.Core.Query.Blql;

<span class="keyword">using var</span> engine = <span class="keyword">new</span> <span class="type">BLiteEngine</span>(<span class="string">"data.db"</span>);
<span class="keyword">var</span> col = engine.GetOrCreateCollection(<span class="string">"users"</span>);

<span class="comment">// ── Option A: JSON string (MQL-style) ────────────────────────</span>
<span class="keyword">var</span> docs = col.Query(<span class="string">"""{ "status": "active", "age": { "$gt": 18 } }"""</span>)
              .Sort(<span class="string">"""{ "name": 1 }"""</span>)
              .Skip(<span class="number">0</span>).Take(<span class="number">20</span>)
              .ToList();

<span class="comment">// ── Option B: Fluent C# API ───────────────────────────────────</span>
<span class="keyword">var</span> docs = col.Query()
              .Filter(<span class="type">BlqlFilter</span>.And(
                  <span class="type">BlqlFilter</span>.Eq(<span class="string">"status"</span>, <span class="string">"active"</span>),
                  <span class="type">BlqlFilter</span>.Gt(<span class="string">"age"</span>, <span class="number">18</span>)))
              .OrderBy(<span class="string">"name"</span>)
              .Project(<span class="type">BlqlProjection</span>.Include(<span class="string">"name"</span>, <span class="string">"email"</span>))
              .ToList();</code></pre>
    </section>

    <!-- ═══════════════════════════════════════════════════════ Filter Operators -->
    <section>
      <h2>Filter Operators</h2>

      <h3>Comparison</h3>
      <pre><code><span class="comment">// JSON                               C# equivalent</span>
{ <span class="string">"age"</span>: <span class="number">30</span> }                         <span class="type">BlqlFilter</span>.Eq(<span class="string">"age"</span>, <span class="number">30</span>)
{ <span class="string">"age"</span>: { <span class="string">"$ne"</span>:  <span class="number">30</span> } }            <span class="type">BlqlFilter</span>.Ne(<span class="string">"age"</span>, <span class="number">30</span>)
{ <span class="string">"age"</span>: { <span class="string">"$gt"</span>:  <span class="number">18</span> } }            <span class="type">BlqlFilter</span>.Gt(<span class="string">"age"</span>, <span class="number">18</span>)
{ <span class="string">"age"</span>: { <span class="string">"$gte"</span>: <span class="number">18</span> } }            <span class="type">BlqlFilter</span>.Gte(<span class="string">"age"</span>, <span class="number">18</span>)
{ <span class="string">"age"</span>: { <span class="string">"$lt"</span>:  <span class="number">65</span> } }            <span class="type">BlqlFilter</span>.Lt(<span class="string">"age"</span>, <span class="number">65</span>)
{ <span class="string">"age"</span>: { <span class="string">"$lte"</span>: <span class="number">65</span> } }            <span class="type">BlqlFilter</span>.Lte(<span class="string">"age"</span>, <span class="number">65</span>)

<span class="comment">// Range: combine $gte + $lte in one object</span>
{ <span class="string">"age"</span>: { <span class="string">"$gte"</span>: <span class="number">18</span>, <span class="string">"$lte"</span>: <span class="number">65</span> } }  <span class="type">BlqlFilter</span>.Between(<span class="string">"age"</span>, <span class="number">18</span>, <span class="number">65</span>)</code></pre>

      <h3>Set Operators</h3>
      <pre><code>{ <span class="string">"role"</span>: { <span class="string">"$in"</span>:  [<span class="string">"admin"</span>, <span class="string">"mod"</span>] } }   <span class="type">BlqlFilter</span>.In(<span class="string">"role"</span>, <span class="string">"admin"</span>, <span class="string">"mod"</span>)
{ <span class="string">"role"</span>: { <span class="string">"$nin"</span>: [<span class="string">"banned"</span>] } }        <span class="type">BlqlFilter</span>.Nin(<span class="string">"role"</span>, <span class="string">"banned"</span>)</code></pre>

      <h3>Field Tests</h3>
      <pre><code>{ <span class="string">"email"</span>: { <span class="string">"$exists"</span>: <span class="literal">true</span>  } }       <span class="type">BlqlFilter</span>.Exists(<span class="string">"email"</span>)
{ <span class="string">"email"</span>: { <span class="string">"$exists"</span>: <span class="literal">false</span> } }       <span class="type">BlqlFilter</span>.Exists(<span class="string">"email"</span>, exists: <span class="literal">false</span>)
{ <span class="string">"email"</span>: <span class="literal">null</span> }                        <span class="type">BlqlFilter</span>.IsNull(<span class="string">"email"</span>)
{ <span class="string">"age"</span>:   { <span class="string">"$type"</span>: <span class="number">16</span> } }             <span class="type">BlqlFilter</span>.Type(<span class="string">"age"</span>, <span class="type">BsonType</span>.Int32)</code></pre>

      <h3>Regex</h3>
      <pre><code>{ <span class="string">"name"</span>: { <span class="string">"$regex"</span>: <span class="string">"^Al"</span> } }         <span class="type">BlqlFilter</span>.Regex(<span class="string">"name"</span>, <span class="string">"^Al"</span>)</code></pre>
      <div class="tip-box">
        <strong>🛡️ ReDoS-safe</strong> — the JSON parser always compiles regex patterns with
        <code>RegexOptions.NonBacktracking</code>, eliminating catastrophic backtracking.
      </div>

      <h3>Logical Operators</h3>
      <pre><code><span class="comment">// $and</span>
{ <span class="string">"$and"</span>: [ { <span class="string">"age"</span>: { <span class="string">"$gt"</span>: <span class="number">18</span> } }, { <span class="string">"status"</span>: <span class="string">"active"</span> } ] }
<span class="type">BlqlFilter</span>.And(<span class="type">BlqlFilter</span>.Gt(<span class="string">"age"</span>, <span class="number">18</span>), <span class="type">BlqlFilter</span>.Eq(<span class="string">"status"</span>, <span class="string">"active"</span>))

<span class="comment">// $or</span>
{ <span class="string">"$or"</span>: [ { <span class="string">"role"</span>: <span class="string">"admin"</span> }, { <span class="string">"role"</span>: <span class="string">"superadmin"</span> } ] }
<span class="type">BlqlFilter</span>.Or(<span class="type">BlqlFilter</span>.Eq(<span class="string">"role"</span>, <span class="string">"admin"</span>), <span class="type">BlqlFilter</span>.Eq(<span class="string">"role"</span>, <span class="string">"superadmin"</span>))

<span class="comment">// $nor — negated OR</span>
{ <span class="string">"$nor"</span>: [ { <span class="string">"deleted"</span>: <span class="literal">true</span> } ] }
<span class="type">BlqlFilter</span>.Nor(<span class="type">BlqlFilter</span>.Eq(<span class="string">"deleted"</span>, <span class="literal">true</span>))

<span class="comment">// $not — wraps any filter</span>
{ <span class="string">"$not"</span>: { <span class="string">"status"</span>: <span class="string">"banned"</span> } }
<span class="type">BlqlFilter</span>.Not(<span class="type">BlqlFilter</span>.Eq(<span class="string">"status"</span>, <span class="string">"banned"</span>))

<span class="comment">// Implicit AND — multiple top-level fields</span>
{ <span class="string">"status"</span>: <span class="string">"active"</span>, <span class="string">"age"</span>: { <span class="string">"$gt"</span>: <span class="number">18</span> } }</code></pre>

      <h3>Fluent Combinators</h3>
      <p>Combine filters programmatically after construction:</p>
      <pre><code><span class="keyword">var</span> f = <span class="type">BlqlFilter</span>.Gt(<span class="string">"age"</span>, <span class="number">18</span>)
               .AndAlso(<span class="type">BlqlFilter</span>.Eq(<span class="string">"status"</span>, <span class="string">"active"</span>))
               .AndAlso(<span class="type">BlqlFilter</span>.Exists(<span class="string">"email"</span>));</code></pre>
    </section>

    <!-- ═══════════════════════════════════════════════════════ Sort -->
    <section>
      <h2>Sort</h2>
      <pre><code><span class="comment">// Single field — ascending</span>
col.Query(filter).OrderBy(<span class="string">"name"</span>).ToList();

<span class="comment">// Single field — descending</span>
col.Query(filter).OrderByDescending(<span class="string">"createdAt"</span>).ToList();

<span class="comment">// Multi-key JSON sort (1 = asc, -1 = desc)</span>
col.Query(filter).Sort(<span class="string">"""{ "lastName": 1, "age": -1 }"""</span>).ToList();

<span class="comment">// Programmatic multi-key</span>
col.Query(filter)
   .Sort(<span class="type">BlqlSort</span>.By(<span class="string">"lastName"</span>, <span class="type">SortDirection</span>.Ascending)
                .ThenBy(<span class="string">"age"</span>,       <span class="type">SortDirection</span>.Descending))
   .ToList();</code></pre>
    </section>

    <!-- ═══════════════════════════════════════════════════════ Projection -->
    <section>
      <h2>Projection</h2>
      <pre><code><span class="comment">// Include only specific fields</span>
col.Query(filter)
   .Project(<span class="type">BlqlProjection</span>.Include(<span class="string">"name"</span>, <span class="string">"email"</span>, <span class="string">"createdAt"</span>))
   .ToList();

<span class="comment">// Exclude specific fields (return everything else)</span>
col.Query(filter)
   .Project(<span class="type">BlqlProjection</span>.Exclude(<span class="string">"password"</span>, <span class="string">"__internal"</span>))
   .ToList();

<span class="comment">// No projection — returns whole document (default)</span>
col.Query(filter).ToList();</code></pre>
      <div class="tip-box">
        Field projections rebuild the <code>BsonDocument</code> in a single pass using the engine's
        shared key map, so <strong>field IDs are preserved exactly</strong> — no re-registration needed.
      </div>
    </section>

    <!-- ═══════════════════════════════════════════════════════ Paging -->
    <section>
      <h2>Paging</h2>
      <pre><code><span class="comment">// Offset-based paging</span>
<span class="keyword">int</span> pageSize = <span class="number">20</span>;
<span class="keyword">int</span> pageNo   = <span class="number">3</span>;   <span class="comment">// 1-based</span>

<span class="keyword">var</span> page = col.Query(filter)
              .OrderBy(<span class="string">"createdAt"</span>)
              .Skip((pageNo - <span class="number">1</span>) * pageSize)
              .Take(pageSize)     <span class="comment">// alias: .Limit(pageSize)</span>
              .ToList();</code></pre>
    </section>

    <!-- ═══════════════════════════════════════════════════════ Terminal Methods -->
    <section>
      <h2>Terminal Methods</h2>
      <table>
        <thead>
          <tr><th>Method</th><th>Returns</th><th>Description</th></tr>
        </thead>
        <tbody>
          <tr><td><code>ToList()</code></td><td><code>List&lt;BsonDocument&gt;</code></td><td>Materialise all results</td></tr>
          <tr><td><code>AsEnumerable()</code></td><td><code>IEnumerable&lt;BsonDocument&gt;</code></td><td>Lazy streaming</td></tr>
          <tr><td><code>AsAsyncEnumerable(ct)</code></td><td><code>IAsyncEnumerable&lt;BsonDocument&gt;</code></td><td>Async streaming with cancellation</td></tr>
          <tr><td><code>Count()</code></td><td><code>int</code></td><td>Count matching documents (ignores skip/take)</td></tr>
          <tr><td><code>FirstOrDefault()</code></td><td><code>BsonDocument?</code></td><td>First match or <code>null</code></td></tr>
          <tr><td><code>First()</code></td><td><code>BsonDocument</code></td><td>First match or throws <code>InvalidOperationException</code></td></tr>
          <tr><td><code>Any()</code></td><td><code>bool</code></td><td>Short-circuits on first match</td></tr>
          <tr><td><code>None()</code></td><td><code>bool</code></td><td>True if no document matches</td></tr>
        </tbody>
      </table>

      <h3>Async Streaming</h3>
      <pre><code><span class="keyword">await foreach</span> (<span class="keyword">var</span> doc <span class="keyword">in</span> col.Query(filter)
                                  .OrderBy(<span class="string">"createdAt"</span>)
                                  .AsAsyncEnumerable(ct))
{
    Process(doc);
}</code></pre>
    </section>

    <!-- ═══════════════════════════════════════════════════════ Building Documents -->
    <section>
      <h2>Creating Documents</h2>
      <p>
        Keys live at <strong>database scope</strong> — <code>BLiteEngine.CreateDocument</code> registers
        field names in the engine's shared key map and builds the <code>BsonDocument</code> in a single call:
      </p>
      <pre><code><span class="comment">// Keys registered on the engine, not the collection</span>
<span class="keyword">var</span> doc = engine.CreateDocument(
    [<span class="string">"name"</span>, <span class="string">"age"</span>, <span class="string">"status"</span>, <span class="string">"role"</span>],
    b => b.AddString(<span class="string">"name"</span>,   <span class="string">"Alice"</span>)
          .AddInt32(<span class="string">"age"</span>,    <span class="number">30</span>)
          .AddString(<span class="string">"status"</span>, <span class="string">"active"</span>)
          .AddString(<span class="string">"role"</span>,   <span class="string">"admin"</span>));

col.Insert(doc);
engine.Commit();</code></pre>
    </section>

    <!-- ═══════════════════════════════════════════════════════ Security -->
    <section>
      <h2>Security — Injection Protection</h2>
      <p>
        The JSON filter parser is hardened against <strong>BLQL-injection attacks</strong>
        (analogous to NoSQL injection):
      </p>
      <table>
        <thead>
          <tr><th>Attack vector</th><th>Protection</th></tr>
        </thead>
        <tbody>
          <tr>
            <td>Unknown <code>$</code> operators at root (<code>$where</code>, <code>$expr</code>, <code>$function</code>, …)</td>
            <td>→ <code>FormatException</code> — never silently passed through</td>
          </tr>
          <tr>
            <td>Unknown field-level operators (<code>$lookup</code>, <code>$match</code>, …)</td>
            <td>→ <code>FormatException</code></td>
          </tr>
          <tr>
            <td>Type confusion (<code>"$exists": 1</code>, <code>"$regex": 42</code>)</td>
            <td>→ <code>FormatException</code></td>
          </tr>
          <tr>
            <td>ReDoS via <code>$regex</code></td>
            <td>→ <code>RegexOptions.NonBacktracking</code> eliminates catastrophic backtracking</td>
          </tr>
          <tr>
            <td>Deeply nested DoS (JSON depth &gt; 64)</td>
            <td>→ <code>JsonReaderException</code> from <code>System.Text.Json</code> before evaluation</td>
          </tr>
        </tbody>
      </table>

      <pre><code><span class="comment">// These all throw FormatException — they are never evaluated</span>
<span class="type">BlqlFilterParser</span>.Parse(<span class="string">"""{ "$where":    "this.age > 0" }"""</span>);
<span class="type">BlqlFilterParser</span>.Parse(<span class="string">"""{ "$function": { "body": "return true;" } }"""</span>);
<span class="type">BlqlFilterParser</span>.Parse(<span class="string">"""{ "$expr":    { "$gt": ["$age", 0] } }"""</span>);
<span class="type">BlqlFilterParser</span>.Parse(<span class="string">"""{ "name": { "$exists": 1 } }"""</span>);</code></pre>
    </section>

    <!-- ═══════════════════════════════════════════════════════ Full Example -->
    <section>
      <h2>Full Example</h2>
      <pre><code><span class="keyword">using</span> BLite.Core.Query.Blql;

<span class="keyword">using var</span> engine = <span class="keyword">new</span> <span class="type">BLiteEngine</span>(<span class="string">"shop.db"</span>);
<span class="keyword">var</span> orders = engine.GetOrCreateCollection(<span class="string">"orders"</span>);

<span class="comment">// Insert some documents</span>
<span class="keyword">foreach</span> (<span class="keyword">var</span> status <span class="keyword">in</span> <span class="keyword">new</span>[] { <span class="string">"pending"</span>, <span class="string">"shipped"</span>, <span class="string">"pending"</span> })
{
    <span class="keyword">var</span> doc = engine.CreateDocument(
        [<span class="string">"status"</span>, <span class="string">"total"</span>, <span class="string">"currency"</span>],
        b => b.AddString(<span class="string">"status"</span>,   status)
              .AddDouble(<span class="string">"total"</span>,    <span class="number">199.99</span>)
              .AddString(<span class="string">"currency"</span>, <span class="string">"EUR"</span>));
    orders.Insert(doc);
}
engine.Commit();

<span class="comment">// Query with JSON string filter + sort + paging</span>
<span class="type">List</span>&lt;<span class="type">BsonDocument</span>&gt; page = orders
    .Query(<span class="string">"""{ "status": "pending", "total": { "$gte": 100 } }"""</span>)
    .Sort(<span class="string">"""{ "total": -1 }"""</span>)
    .Skip(<span class="number">0</span>).Take(<span class="number">10</span>)
    .Project(<span class="type">BlqlProjection</span>.Include(<span class="string">"status"</span>, <span class="string">"total"</span>))
    .ToList();

<span class="keyword">foreach</span> (<span class="keyword">var</span> doc <span class="keyword">in</span> page)
    Console.WriteLine(<span class="string">$"status={doc.GetString("status")} total={doc.GetDouble("total")}"</span>);

<span class="comment">// Aggregate</span>
<span class="keyword">int</span> pendingCount = orders.Query(<span class="string">"""{ "status": "pending" }"""</span>).Count();
Console.WriteLine(<span class="string">$"Pending orders: {pendingCount}"</span>);</code></pre>
    </section>

    <!-- ═══════════════════════════════════════════════════════ Next Steps -->
    <section>
      <h2>Next Steps</h2>
      <div class="next-links">
        <router-link to="/docs/dynamic-api" class="next-card">
          <span class="next-icon">🔌</span>
          <div>
            <h4>Schema-less API</h4>
            <p>Explore the full BLiteEngine and DynamicCollection surface</p>
          </div>
        </router-link>
        <router-link to="/docs/querying" class="next-card">
          <span class="next-icon">🔍</span>
          <div>
            <h4>LINQ Querying</h4>
            <p>Type-safe queries with IQueryable for DocumentDbContext</p>
          </div>
        </router-link>
      </div>
    </section>
  </div>
</template>

<script setup lang="ts">
// Static documentation page — no reactive logic needed
</script>

<style scoped>
.doc-page {
  max-width: 900px;
}

h1 {
  font-size: 2.5rem;
  font-weight: 800;
  margin-bottom: 16px;
}

.lead {
  font-size: 1.2rem;
  color: var(--text-secondary);
  margin-bottom: 48px;
  line-height: 1.7;
}

section {
  margin-bottom: 56px;
}

h2 {
  font-size: 1.8rem;
  font-weight: 700;
  margin-bottom: 16px;
  color: var(--blite-red);
  border-bottom: 2px solid rgba(231, 76, 60, 0.2);
  padding-bottom: 8px;
}

h3 {
  font-size: 1.2rem;
  font-weight: 600;
  margin: 28px 0 12px;
  color: var(--text-primary);
}

p {
  margin-bottom: 16px;
  line-height: 1.7;
  color: var(--text-secondary);
}

ul {
  margin: 16px 0;
  padding-left: 24px;
}

li {
  margin-bottom: 8px;
  color: var(--text-secondary);
  line-height: 1.6;
}

/* ── Inline code ─────────────────────────────────────────────── */
code {
  font-family: var(--font-mono);
  font-size: 0.88rem;
  background: rgba(231, 76, 60, 0.1);
  padding: 2px 6px;
  border-radius: 4px;
  color: var(--blite-red);
}

/* ── Code blocks ─────────────────────────────────────────────── */
pre {
  background: rgba(10, 10, 10, 0.7);
  border: 1px solid rgba(231, 76, 60, 0.2);
  border-radius: 8px;
  padding: 20px 24px;
  overflow-x: auto;
  margin: 16px 0;
}

pre code {
  background: none;
  padding: 0;
  color: var(--text-secondary);
  font-size: 0.9rem;
  line-height: 1.65;
}

/* ── Syntax highlighting ─────────────────────────────────────── */
.keyword  { color: var(--blite-red); }
.type     { color: #06b6d4; }
.string   { color: #a3e635; }
.number   { color: #f59e0b; }
.comment  { color: #52525b; font-style: italic; }
.literal  { color: #f59e0b; }   /* true / false / null */

/* ── Tables ──────────────────────────────────────────────────── */
table {
  width: 100%;
  border-collapse: collapse;
  margin: 16px 0;
  font-size: 0.95rem;
}

th {
  text-align: left;
  padding: 10px 14px;
  color: var(--blite-red);
  font-weight: 600;
  border-bottom: 2px solid rgba(231, 76, 60, 0.25);
  background: rgba(231, 76, 60, 0.06);
}

td {
  padding: 10px 14px;
  color: var(--text-secondary);
  border-bottom: 1px solid rgba(231, 76, 60, 0.1);
  vertical-align: top;
}

tr:last-child td {
  border-bottom: none;
}

/* ── Info box ────────────────────────────────────────────────── */
.info-box {
  background: rgba(231, 76, 60, 0.05);
  border: 1px solid rgba(231, 76, 60, 0.2);
  border-radius: 8px;
  padding: 20px 24px;
  margin: 24px 0 32px;
}

.info-header {
  font-weight: 700;
  color: var(--blite-red);
  margin-bottom: 14px;
  font-size: 1.05rem;
}

/* ── Tip / warning box ───────────────────────────────────────── */
.tip-box {
  background: rgba(6, 182, 212, 0.06);
  border: 1px solid rgba(6, 182, 212, 0.25);
  border-left: 3px solid #06b6d4;
  border-radius: 6px;
  padding: 12px 16px;
  margin: 12px 0 20px;
  color: var(--text-secondary);
  font-size: 0.92rem;
  line-height: 1.6;
}

.tip-box strong {
  color: #06b6d4;
}

/* ── Next-step cards ─────────────────────────────────────────── */
.next-links {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
  gap: 16px;
  margin-top: 24px;
}

.next-card {
  display: flex;
  gap: 16px;
  align-items: flex-start;
  padding: 20px;
  background: rgba(231, 76, 60, 0.05);
  border: 1px solid rgba(231, 76, 60, 0.2);
  border-radius: 8px;
  transition: all 0.2s ease;
  text-decoration: none;
}

.next-card:hover {
  background: rgba(231, 76, 60, 0.1);
  transform: translateY(-2px);
  box-shadow: 0 4px 16px rgba(231, 76, 60, 0.15);
}

.next-icon {
  font-size: 2rem;
  flex-shrink: 0;
}

.next-card h4 {
  font-size: 1.05rem;
  font-weight: 600;
  margin-bottom: 4px;
  color: var(--blite-red);
}

.next-card p {
  font-size: 0.88rem;
  margin: 0;
  color: var(--text-muted);
}
</style>
