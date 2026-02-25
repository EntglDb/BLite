<template>
  <div class="doc-page">
    <h1>📈 <span class="title-gradient">TimeSeries</span></h1>
    <p class="lead">
      BLite 1.12 introduces a native <strong>TimeSeries page type</strong> — a dedicated, append-only storage format
      optimised for high-throughput time-ordered data with automatic retention-based pruning.
      No background threads, no daemons: pruning is triggered transparently on insertion.
    </p>

    <div class="info-box">
      <div class="info-header">📌 How it works</div>
      <p>
        When a collection is marked as TimeSeries, every new insert goes to a chain of
        <strong>TimeSeries pages</strong> instead of slotted data pages.
        Each page packs BSON documents sequentially and tracks the latest timestamp in its header.
        When the retention threshold is reached, entire expired pages are freed in a single pass —
        no per-document scanning needed.
      </p>
    </div>

    <section>
      <h2>Page Layout</h2>
      <table>
        <thead><tr><th>Offset</th><th>Size</th><th>Field</th></tr></thead>
        <tbody>
          <tr><td><code>0–31</code></td><td>32 B</td><td>Standard <code>PageHeader</code> (<code>PageType.TimeSeries = 12</code>)</td></tr>
          <tr><td><code>32–39</code></td><td>8 B</td><td><code>LastTimestamp</code> — ticks of the newest entry on this page</td></tr>
          <tr><td><code>40–43</code></td><td>4 B</td><td><code>EntryCount</code> — number of BSON documents stored</td></tr>
          <tr><td><code>44–47</code></td><td>4 B</td><td>Reserved</td></tr>
          <tr><td><code>48+</code></td><td>variable</td><td>BSON documents packed sequentially (no slots, no gaps)</td></tr>
        </tbody>
      </table>
    </section>

    <section>
      <h2>Enabling TimeSeries on a Collection</h2>
      <p>Call <code>SetTimeSeries()</code> on any <code>DynamicCollection</code>:</p>
      <pre><code><span class="keyword">using</span> <span class="keyword">var</span> engine = <span class="keyword">new</span> <span class="type">BLiteEngine</span>(<span class="string">"iot.db"</span>);
<span class="keyword">var</span> sensors = engine.GetOrCreateCollection(<span class="string">"sensors"</span>);

<span class="comment">// Enable TimeSeries with a 7-day retention window</span>
<span class="comment">// The second argument names the timestamp field in each document.</span>
<span class="comment">// Leave it empty ("") to use insertion time automatically.</span>
sensors.SetTimeSeries(<span class="string">"timestamp"</span>, <span class="type">TimeSpan</span>.FromDays(<span class="number">7</span>));

engine.Commit();</code></pre>
    </section>

    <section>
      <h2>Inserting Time-Stamped Documents</h2>
      <p>
        After enabling TimeSeries, use the standard <code>Insert</code> / <code>InsertAsync</code> API.
        BLite routes inserts to TimeSeries pages automatically.
        The timestamp is read from the field named in <code>SetTimeSeries</code>;
        if the field is absent or empty, the current UTC time is used.
      </p>
      <pre><code><span class="keyword">var</span> doc = sensors.CreateDocument(
    [<span class="string">"deviceId"</span>, <span class="string">"temperature"</span>, <span class="string">"timestamp"</span>],
    b => b
        .Set(<span class="string">"deviceId"</span>,    <span class="string">"sensor-42"</span>)
        .Set(<span class="string">"temperature"</span>, <span class="number">23.5</span>)
        .Set(<span class="string">"timestamp"</span>,   <span class="type">DateTime</span>.UtcNow));

sensors.Insert(doc);</code></pre>

      <div class="info-box warning">
        <div class="info-header">⚠️ Timestamp field types</div>
        <p>
          The TTL field must contain either a <strong>BSON DateTime</strong> or an <strong>Int64</strong> (ticks).
          Any other type is ignored and insertion time is used as fallback.
        </p>
      </div>
    </section>

    <section>
      <h2>Automatic Pruning</h2>
      <p>
        Pruning is triggered automatically on insert when either condition is true:
      </p>
      <ul>
        <li><strong>1 000 documents</strong> have been inserted since the last prune, <em>or</em></li>
        <li><strong>5 minutes</strong> have elapsed since the last prune.</li>
      </ul>
      <p>
        The engine walks the page chain from the oldest end and frees every page whose
        <code>LastTimestamp</code> is older than <code>UtcNow − RetentionPolicy</code>.
        It stops at the first non-expired page, so the scan is <strong>O(freed pages)</strong>, not O(all pages).
      </p>
    </section>

    <section>
      <h2>Reading Data</h2>
      <p>All standard read methods work on TimeSeries collections:</p>
      <pre><code><span class="comment">// Full scan — returns all documents on all active TS pages</span>
<span class="keyword">foreach</span> (<span class="keyword">var</span> d <span class="keyword">in</span> sensors.FindAll()) { ... }

<span class="comment">// Lookup by _id via primary B-Tree</span>
<span class="keyword">var</span> doc = sensors.FindById(id);

<span class="comment">// BLQL query (filter, sort, project …)</span>
<span class="keyword">var</span> recent = sensors.Query(<span class="string">"""{ "deviceId": "sensor-42" }"""</span>)
    .Sort(<span class="string">"""{ "timestamp": -1 }"""</span>)
    .Take(<span class="number">100</span>)
    .ToList();</code></pre>
    </section>

    <section>
      <h2>Force Prune (Testing / Admin)</h2>
      <p>
        To trigger pruning immediately — useful in tests or admin tooling — call <code>ForcePrune()</code>:
      </p>
      <pre><code>sensors.ForcePrune();
engine.Commit();</code></pre>
      <p>
        In <strong>BLite Studio</strong>, the TimeSeries tab exposes a <em>⚡ Force Prune</em> button
        alongside the configuration form.
      </p>
    </section>

    <section>
      <h2>Checking the Configuration</h2>
      <pre><code><span class="keyword">bool</span> isTs = sensors.IsTimeSeries;  <span class="comment">// true</span>

<span class="keyword">var</span> (retentionMs, ttlField) = sensors.GetTimeSeriesConfig();
<span class="comment">// retentionMs = 604800000  (7 days in ms)</span>
<span class="comment">// ttlField    = "timestamp"</span></code></pre>
    </section>

    <section>
      <h2>Important Caveats</h2>

      <div class="info-box warning">
        <div class="info-header">⚠️ Converting an existing collection</div>
        <ul>
          <li><strong>Existing documents are NOT migrated.</strong> They stay in their original slotted pages and are never pruned by the retention policy.</li>
          <li><strong>All indexes remain valid</strong> — primary B-Tree and secondary indexes continue to serve both old and new documents.</li>
          <li><strong>Only documents inserted after enabling TimeSeries</strong> are written to TS pages and are subject to pruning.</li>
          <li><strong>One-way operation</strong> — there is no built-in way to revert to standard mode once enabled.</li>
        </ul>
      </div>

      <div class="info-box">
        <div class="info-header">📌 v1 known limitation — stale B-Tree entries after pruning</div>
        <p>
          When pages are freed by pruning, the primary B-Tree index retains stale entries pointing to the freed pages.
          <code>FindAll()</code> silently skips documents whose page has been freed (returning <code>null</code> guards are in place),
          so reads remain safe. A future release will clean up stale index entries during the prune pass.
        </p>
      </div>
    </section>

    <section>
      <h2>BLite Studio</h2>
      <p>
        The <strong>TimeSeries</strong> tab in BLite Studio lets you configure and monitor any collection:
      </p>
      <ul>
        <li>Status badge — <span style="color:#55DD88;font-weight:600">TIME SERIES</span> (green) or STANDARD (grey)</li>
        <li>TTL field name input</li>
        <li>Retention period (days) via a numeric picker</li>
        <li><em>Save TimeSeries Configuration</em> button</li>
        <li><em>⚡ Force Prune</em> button — visible only when TimeSeries is already enabled</li>
        <li>Sidebar TS badge — TimeSeries collections show a small green <strong>TS</strong> chip next to their name</li>
      </ul>
    </section>

    <section>
      <h2>Quick Reference</h2>
      <table>
        <thead><tr><th>API</th><th>Description</th></tr></thead>
        <tbody>
          <tr><td><code>SetTimeSeries(field, retention)</code></td><td>Enable TimeSeries mode. <code>field</code> can be empty to use insertion time.</td></tr>
          <tr><td><code>IsTimeSeries</code></td><td>Returns <code>true</code> if the collection is in TimeSeries mode.</td></tr>
          <tr><td><code>GetTimeSeriesConfig()</code></td><td>Returns <code>(RetentionPolicyMs, TtlFieldName)</code>.</td></tr>
          <tr><td><code>ForcePrune()</code></td><td>Runs the prune pass immediately, ignoring insert-count and time thresholds.</td></tr>
          <tr><td><code>Insert(doc)</code> / <code>InsertAsync(doc)</code></td><td>Standard insert — routed to TS pages automatically when in TS mode.</td></tr>
          <tr><td><code>FindAll()</code> / BLQL <code>Query()</code></td><td>Standard reads — operate transparently across TS and slotted pages.</td></tr>
        </tbody>
      </table>
    </section>

  </div>
</template>

<style scoped>
.doc-page {
  max-width: 800px;
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
  margin-bottom: 48px;
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
  font-size: 1.3rem;
  font-weight: 600;
  margin: 24px 0 12px;
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

li strong {
  color: var(--blite-red);
}

pre {
  background: rgba(10, 10, 10, 0.6);
  border: 1px solid rgba(231, 76, 60, 0.2);
  border-radius: 8px;
  padding: 20px;
  overflow-x: auto;
  margin: 16px 0;
}

code {
  font-family: var(--font-mono);
  font-size: 0.9rem;
  line-height: 1.6;
  color: var(--text-secondary);
}

.keyword { color: var(--blite-red); }
.type    { color: #06b6d4; }
.string  { color: #a1a1aa; }
.number  { color: #06b6d4; }
.comment { color: #52525b; font-style: italic; }

table {
  width: 100%;
  border-collapse: collapse;
  margin: 16px 0;
}

th, td {
  padding: 12px;
  text-align: left;
  border-bottom: 1px solid rgba(231, 76, 60, 0.2);
}

th {
  color: var(--blite-red);
  font-weight: 600;
}

td {
  color: var(--text-secondary);
  font-size: 0.95rem;
}

.info-box {
  background: rgba(231, 76, 60, 0.05);
  border: 1px solid rgba(231, 76, 60, 0.2);
  border-left: 4px solid var(--blite-red);
  border-radius: 8px;
  padding: 20px;
  margin: 24px 0;
}

.info-header {
  font-weight: 600;
  color: var(--blite-red);
  margin-bottom: 12px;
  font-size: 1.1rem;
}

.info-box.warning {
  background: rgba(232, 192, 64, 0.05);
  border-color: rgba(232, 192, 64, 0.3);
  border-left-color: #e8c040;
}

.info-box.warning .info-header {
  color: #e8c040;
}

.info-box.warning p,
.info-box.warning li {
  color: #D0C090;
}
</style>
