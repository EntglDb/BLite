<template>
  <div class="doc-page">
    <h1>🔌 <span class="title-gradient">Schema-less API</span></h1>
    <p class="lead">
      When compile-time types are unavailable — server-side processing, migrations, scripting, or interop scenarios —
      BLite exposes a fully schema-less BSON API via <code>BLiteEngine</code> and <code>DynamicCollection</code>.
      Both paths share the same kernel: StorageEngine, B-Tree, WAL, Vector and Spatial indexes.
    </p>

    <div class="info-box">
      <div class="info-header">📌 Two Modes, One Kernel</div>
      <table>
        <thead>
          <tr><th></th><th>DocumentDbContext</th><th>BLiteEngine</th></tr>
        </thead>
        <tbody>
          <tr><td><strong>Type safety</strong></td><td>✅ Compile-time</td><td>Runtime <code>BsonDocument</code></td></tr>
          <tr><td><strong>Source generators</strong></td><td>✅ Zero reflection</td><td>—</td></tr>
          <tr><td><strong>LINQ IQueryable</strong></td><td>✅ Full</td><td>—</td></tr>
          <tr><td><strong>Schema-less / dynamic</strong></td><td>—</td><td>✅</td></tr>
          <tr><td><strong>Server / scripting mode</strong></td><td>—</td><td>✅</td></tr>
          <tr><td><strong>Performance</strong></td><td>✅ Max (generated mappers)</td><td>✅ Near-identical</td></tr>
          <tr><td><strong>Shared storage file</strong></td><td>✅</td><td>✅</td></tr>
        </tbody>
      </table>
    </div>

    <section>
      <h2>Entry Point</h2>
      <pre><code><span class="keyword">using</span> <span class="keyword">var</span> engine = <span class="keyword">new</span> <span class="type">BLiteEngine</span>(<span class="string">"data.db"</span>);

<span class="comment">// Open or create a schema-less collection</span>
<span class="keyword">var</span> orders = engine.GetOrCreateCollection(<span class="string">"orders"</span>, <span class="type">BsonIdType</span>.ObjectId);

<span class="comment">// List all collections</span>
<span class="type">IReadOnlyList</span>&lt;<span class="keyword">string</span>&gt; names = engine.ListCollections();

<span class="comment">// Drop a collection</span>
engine.DropCollection(<span class="string">"orders"</span>);</code></pre>
    </section>

    <section>
      <h2>Insert</h2>
      <h3>Sync</h3>
      <pre><code><span class="comment">// Build a BsonDocument using the engine's field-name dictionary</span>
<span class="keyword">var</span> doc = orders.CreateDocument(
    [<span class="string">"status"</span>, <span class="string">"total"</span>, <span class="string">"currency"</span>],
    b => b
        .Set(<span class="string">"status"</span>,   <span class="string">"pending"</span>)
        .Set(<span class="string">"total"</span>,    <span class="number">199.99</span>)
        .Set(<span class="string">"currency"</span>, <span class="string">"EUR"</span>));

<span class="type">BsonId</span> id = orders.Insert(doc);</code></pre>

      <h3>Async</h3>
      <pre><code><span class="comment">// Async single insert</span>
<span class="type">BsonId</span> id = <span class="keyword">await</span> orders.InsertAsync(doc, ct);

<span class="comment">// Bulk insert (single transaction)</span>
<span class="type">List</span>&lt;<span class="type">BsonId</span>&gt; ids = orders.InsertBulk([doc1, doc2, doc3]);
<span class="type">List</span>&lt;<span class="type">BsonId</span>&gt; ids = <span class="keyword">await</span> orders.InsertBulkAsync([doc1, doc2, doc3], ct);</code></pre>
    </section>

    <section>
      <h2>Read</h2>

      <h3>Primary-key lookup</h3>
      <pre><code><span class="type">BsonDocument</span>? doc = orders.FindById(id);
<span class="type">BsonDocument</span>? doc = <span class="keyword">await</span> orders.FindByIdAsync(id, ct);</code></pre>

      <h3>Full collection scan</h3>
      <pre><code><span class="keyword">foreach</span> (<span class="keyword">var</span> d <span class="keyword">in</span> orders.FindAll()) { ... }

<span class="keyword">await foreach</span> (<span class="keyword">var</span> d <span class="keyword">in</span> orders.FindAllAsync(ct)) { ... }</code></pre>

      <h3>Predicate filter</h3>
      <pre><code><span class="comment">// Sync predicate filter</span>
<span class="keyword">var</span> pending = orders.Find(d => d.GetString(<span class="string">"status"</span>) == <span class="string">"pending"</span>);

<span class="comment">// Async predicate filter</span>
<span class="keyword">await foreach</span> (<span class="keyword">var</span> d <span class="keyword">in</span> orders.FindAsync(d => d.GetDouble(<span class="string">"total"</span>) > <span class="number">100</span>, ct)) { ... }</code></pre>

      <h3>Zero-copy predicate scan</h3>
      <p>Use <code>BsonSpanReader</code> to filter directly on the raw bytes — no heap allocation per document:</p>
      <pre><code><span class="keyword">var</span> shipped = orders.Scan(reader =>
{
    <span class="keyword">if</span> (reader.TryReadString(<span class="string">"status"</span>, <span class="keyword">out var</span> status))
        <span class="keyword">return</span> status == <span class="string">"shipped"</span>;
    <span class="keyword">return false</span>;
});</code></pre>

      <h3>B-Tree range query</h3>
      <pre><code><span class="keyword">var</span> recent = orders.QueryIndex(<span class="string">"idx_placed_at"</span>, minDate, maxDate);</code></pre>

      <h3>Count</h3>
      <pre><code><span class="keyword">int</span> total = orders.Count();</code></pre>

      <h3>Read fields from a BsonDocument</h3>
      <pre><code><span class="keyword">if</span> (doc <span class="keyword">is not null</span>)
{
    <span class="keyword">string</span>  status   = doc.GetString(<span class="string">"status"</span>);
    <span class="keyword">double</span>  total    = doc.GetDouble(<span class="string">"total"</span>);
    <span class="type">BsonId</span>  docId    = doc.Id;
}</code></pre>
    </section>

    <section>
      <h2>Vector &amp; Spatial Search</h2>
      <pre><code><span class="comment">// Vector similarity search (HNSW)</span>
<span class="keyword">var</span> similar = orders.VectorSearch(<span class="string">"idx_embedding"</span>, queryVector, k: <span class="number">10</span>);

<span class="comment">// Geospatial proximity (Milan, 5 km radius)</span>
<span class="keyword">var</span> nearby = orders.Near(<span class="string">"idx_location"</span>, (<span class="number">45.46</span>, <span class="number">9.18</span>), radiusKm: <span class="number">5.0</span>);

<span class="comment">// Bounding box</span>
<span class="keyword">var</span> inArea = orders.Within(<span class="string">"idx_location"</span>, (<span class="number">45.0</span>, <span class="number">9.0</span>), (<span class="number">46.0</span>, <span class="number">10.0</span>));</code></pre>
    </section>

    <section>
      <h2>Update &amp; Delete</h2>
      <pre><code><span class="comment">// Sync</span>
<span class="keyword">bool</span> updated = orders.Update(id, newDoc);
<span class="keyword">bool</span> deleted = orders.Delete(id);

<span class="comment">// Async (collection-level)</span>
<span class="keyword">bool</span> updated = <span class="keyword">await</span> orders.UpdateAsync(id, newDoc, ct);
<span class="keyword">bool</span> deleted = <span class="keyword">await</span> orders.DeleteAsync(id, ct);

<span class="comment">// Bulk (single transaction)</span>
<span class="keyword">int</span> updatedCount = orders.UpdateBulk([(id1, doc1), (id2, doc2)]);
<span class="keyword">int</span> deletedCount = orders.DeleteBulk([id1, id2, id3]);

<span class="comment">// Bulk async</span>
<span class="keyword">int</span> updatedCount = <span class="keyword">await</span> orders.UpdateBulkAsync([(id1, doc1), (id2, doc2)], ct);
<span class="keyword">int</span> deletedCount = <span class="keyword">await</span> orders.DeleteBulkAsync([id1, id2, id3], ct);</code></pre>
    </section>

    <section>
      <h2>Index Management</h2>
      <pre><code><span class="comment">// B-Tree secondary index</span>
orders.CreateIndex(<span class="string">"status"</span>);
orders.CreateIndex(<span class="string">"placed_at"</span>, unique: <span class="keyword">false</span>);
orders.CreateIndex(<span class="string">"order_number"</span>, unique: <span class="keyword">true</span>);

<span class="comment">// Vector index (HNSW)</span>
orders.CreateVectorIndex(<span class="string">"embedding"</span>, dimensions: <span class="number">1536</span>, metric: <span class="type">VectorMetric</span>.Cosine);

<span class="comment">// Spatial index (R-Tree)</span>
orders.CreateSpatialIndex(<span class="string">"location"</span>);

<span class="comment">// Introspect / drop</span>
<span class="type">IReadOnlyList</span>&lt;<span class="keyword">string</span>&gt; indexes = orders.ListIndexes();
orders.DropIndex(<span class="string">"idx_status"</span>);</code></pre>
    </section>

    <section>
      <h2>API Reference</h2>
      <h3>BLiteEngine</h3>
      <table>
        <thead>
          <tr><th>Method</th><th>Description</th></tr>
        </thead>
        <tbody>
          <tr><td><code>GetOrCreateCollection(name, idType)</code></td><td>Open or create a <code>DynamicCollection</code></td></tr>
          <tr><td><code>GetCollection(name)</code></td><td>Get existing collection or <code>null</code></td></tr>
          <tr><td><code>ListCollections()</code></td><td>List all collection names</td></tr>
          <tr><td><code>DropCollection(name)</code></td><td>Delete a collection and all its data</td></tr>
          <tr><td><code>Insert(collection, doc)</code></td><td>Sync insert, returns <code>BsonId</code></td></tr>
          <tr><td><code>InsertAsync(collection, doc, ct)</code></td><td>Async insert, returns <code>Task&lt;BsonId&gt;</code></td></tr>
          <tr><td><code>InsertBulk(collection, docs)</code></td><td>Bulk insert (one transaction), returns <code>List&lt;BsonId&gt;</code></td></tr>
          <tr><td><code>InsertBulkAsync(collection, docs, ct)</code></td><td>Async bulk insert, returns <code>Task&lt;List&lt;BsonId&gt;&gt;</code></td></tr>
          <tr><td><code>FindById(collection, id)</code></td><td>Sync primary-key lookup</td></tr>
          <tr><td><code>FindByIdAsync(collection, id, ct)</code></td><td>Async primary-key lookup</td></tr>
          <tr><td><code>FindAll(collection)</code></td><td>Sync full-collection scan</td></tr>
          <tr><td><code>FindAllAsync(collection, ct)</code></td><td>Async streaming <code>IAsyncEnumerable&lt;BsonDocument&gt;</code></td></tr>
          <tr><td><code>Find(collection, predicate)</code></td><td>Sync predicate filter</td></tr>
          <tr><td><code>FindAsync(collection, predicate, ct)</code></td><td>Async predicate filter as <code>IAsyncEnumerable</code></td></tr>
          <tr><td><code>Update(collection, id, doc)</code></td><td>Sync update</td></tr>
          <tr><td><code>UpdateAsync(collection, id, doc, ct)</code></td><td>Async update</td></tr>
          <tr><td><code>UpdateBulk(collection, updates)</code></td><td>Bulk update (one transaction), returns <code>int</code></td></tr>
          <tr><td><code>UpdateBulkAsync(collection, updates, ct)</code></td><td>Async bulk update, returns <code>Task&lt;int&gt;</code></td></tr>
          <tr><td><code>Delete(collection, id)</code></td><td>Sync delete</td></tr>
          <tr><td><code>DeleteAsync(collection, id, ct)</code></td><td>Async delete</td></tr>
          <tr><td><code>DeleteBulk(collection, ids)</code></td><td>Bulk delete (one transaction), returns <code>int</code></td></tr>
          <tr><td><code>DeleteBulkAsync(collection, ids, ct)</code></td><td>Async bulk delete, returns <code>Task&lt;int&gt;</code></td></tr>
        </tbody>
      </table>

      <h3>DynamicCollection</h3>
      <table>
        <thead>
          <tr><th>Method</th><th>Returns</th></tr>
        </thead>
        <tbody>
          <tr><td><code>CreateDocument(fields, builder)</code></td><td><code>BsonDocument</code></td></tr>
          <tr><td><code>Insert(doc)</code></td><td><code>BsonId</code></td></tr>
          <tr><td><code>InsertAsync(doc, ct)</code></td><td><code>Task&lt;BsonId&gt;</code></td></tr>
          <tr><td><code>InsertBulk(docs)</code></td><td><code>List&lt;BsonId&gt;</code></td></tr>
          <tr><td><code>InsertBulkAsync(docs, ct)</code></td><td><code>Task&lt;List&lt;BsonId&gt;&gt;</code></td></tr>
          <tr><td><code>FindById(id)</code></td><td><code>BsonDocument?</code></td></tr>
          <tr><td><code>FindByIdAsync(id, ct)</code></td><td><code>ValueTask&lt;BsonDocument?&gt;</code></td></tr>
          <tr><td><code>FindAll()</code></td><td><code>IEnumerable&lt;BsonDocument&gt;</code></td></tr>
          <tr><td><code>FindAllAsync(ct)</code></td><td><code>IAsyncEnumerable&lt;BsonDocument&gt;</code></td></tr>
          <tr><td><code>Find(predicate)</code></td><td><code>IEnumerable&lt;BsonDocument&gt;</code></td></tr>
          <tr><td><code>FindAsync(predicate, ct)</code></td><td><code>IAsyncEnumerable&lt;BsonDocument&gt;</code></td></tr>
          <tr><td><code>Scan(predicate)</code></td><td><code>IEnumerable&lt;BsonDocument&gt;</code> (zero-copy)</td></tr>
          <tr><td><code>QueryIndex(name, min, max)</code></td><td><code>IEnumerable&lt;BsonDocument&gt;</code></td></tr>
          <tr><td><code>VectorSearch(name, vector, k)</code></td><td><code>IEnumerable&lt;BsonDocument&gt;</code></td></tr>
          <tr><td><code>Near(name, center, radiusKm)</code></td><td><code>IEnumerable&lt;BsonDocument&gt;</code></td></tr>
          <tr><td><code>Within(name, min, max)</code></td><td><code>IEnumerable&lt;BsonDocument&gt;</code></td></tr>
          <tr><td><code>Count()</code></td><td><code>int</code></td></tr>
          <tr><td><code>Update(id, doc)</code></td><td><code>bool</code></td></tr>
          <tr><td><code>UpdateAsync(id, doc, ct)</code></td><td><code>Task&lt;bool&gt;</code></td></tr>
          <tr><td><code>UpdateBulk(updates)</code></td><td><code>int</code></td></tr>
          <tr><td><code>UpdateBulkAsync(updates, ct)</code></td><td><code>Task&lt;int&gt;</code></td></tr>
          <tr><td><code>Delete(id)</code></td><td><code>bool</code></td></tr>
          <tr><td><code>DeleteAsync(id, ct)</code></td><td><code>Task&lt;bool&gt;</code></td></tr>
          <tr><td><code>DeleteBulk(ids)</code></td><td><code>int</code></td></tr>
          <tr><td><code>DeleteBulkAsync(ids, ct)</code></td><td><code>Task&lt;int&gt;</code></td></tr>
          <tr><td><code>CreateIndex / CreateVectorIndex / CreateSpatialIndex</code></td><td><code>void</code></td></tr>
          <tr><td><code>DropIndex(name)</code></td><td><code>bool</code></td></tr>
          <tr><td><code>ListIndexes()</code></td><td><code>IReadOnlyList&lt;string&gt;</code></td></tr>
        </tbody>
      </table>
    </section>
  </div>
</template>

<style scoped>
.doc-page { max-width: 900px; }
h1 { font-size: 2.5rem; font-weight: 800; margin-bottom: 16px; }
.lead { font-size: 1.2rem; color: var(--text-secondary); margin-bottom: 48px; line-height: 1.7; }
section { margin-bottom: 48px; }
h2 { font-size: 1.8rem; font-weight: 700; margin-bottom: 16px; color: var(--blite-red); border-bottom: 2px solid rgba(231, 76, 60, 0.2); padding-bottom: 8px; }
h3 { font-size: 1.3rem; font-weight: 600; margin: 24px 0 12px; }
p { margin-bottom: 16px; line-height: 1.7; color: var(--text-secondary); }
ul { margin: 16px 0; padding-left: 24px; }
li { margin-bottom: 12px; color: var(--text-secondary); line-height: 1.6; }
code { font-family: var(--font-mono); font-size: 0.9rem; background: rgba(231, 76, 60, 0.1); padding: 2px 6px; border-radius: 4px; color: var(--blite-red); }
pre { background: rgba(10, 10, 10, 0.6); border: 1px solid rgba(231, 76, 60, 0.2); border-radius: 8px; padding: 20px; overflow-x: auto; margin: 16px 0; }
pre code { background: none; padding: 0; color: var(--text-secondary); }
.keyword { color: var(--blite-red); }
.type { color: #06b6d4; }
.string { color: #a1a1aa; }
.number { color: #06b6d4; }
.comment { color: #52525b; font-style: italic; }
.info-box { background: rgba(231, 76, 60, 0.05); border: 1px solid rgba(231, 76, 60, 0.2); border-radius: 8px; padding: 20px; margin: 24px 0; }
.info-header { font-weight: 600; color: var(--blite-red); margin-bottom: 12px; font-size: 1.1rem; }
table { width: 100%; border-collapse: collapse; margin: 16px 0; }
th, td { padding: 12px; text-align: left; border-bottom: 1px solid rgba(231, 76, 60, 0.2); }
th { color: var(--blite-red); font-weight: 600; }
td { color: var(--text-secondary); font-size: 0.95rem; }
</style>
