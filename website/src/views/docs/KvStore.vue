<template>
  <div class="doc-page">
    <h1>🗝️ <span class="title-gradient">Embedded Key-Value Store</span></h1>
    <p class="lead">
      BLite 1.13 ships a persistent <strong>key-value store co-located in the same database file</strong> — no extra
      process, no extra file, no separate daemon. Access it via <code>IBLiteKvStore</code> on any
      <code>BLiteEngine</code> or <code>DocumentDbContext</code>.
    </p>

    <div class="info-box">
      <div class="info-header">📌 How it works</div>
      <p>
        The KV engine uses a dedicated <code>PageType.KeyValue = 13</code> page chain stored alongside your
        document collections. A compact in-memory index (<code>ConcurrentDictionary&lt;string, KvEntryLocation&gt;</code>)
        is rebuilt at startup and keeps all lookups O(1). Writes, deletes, and batch operations are serialized
        through a single write lock, ensuring ACID-safe updates without blocking document reads.
      </p>
    </div>

    <section>
      <h2>Page Layout</h2>
      <table>
        <thead><tr><th>Offset</th><th>Size</th><th>Field</th></tr></thead>
        <tbody>
          <tr><td><code>0–31</code></td><td>32 B</td><td>Standard <code>PageHeader</code> (<code>PageType.KeyValue = 13</code>)</td></tr>
          <tr><td><code>32–33</code></td><td>2 B</td><td><code>EntryCount</code> — live slots on this page</td></tr>
          <tr><td><code>34–35</code></td><td>2 B</td><td><code>FreeSpaceEnd</code> — offset of the next free byte in the data area</td></tr>
          <tr><td><code>36+</code></td><td>10 B × N</td><td>Slot array — one per entry, growing forward from <code>36</code></td></tr>
          <tr><td><em>data area</em></td><td>variable</td><td>Key + value bytes, growing <strong>backward</strong> from the page end</td></tr>
        </tbody>
      </table>
      <h3>Slot Format (10 bytes)</h3>
      <table>
        <thead><tr><th>Offset</th><th>Size</th><th>Field</th></tr></thead>
        <tbody>
          <tr><td><code>0–1</code></td><td>2 B</td><td><code>DataOffset</code> — byte offset of the key inside the data area</td></tr>
          <tr><td><code>2</code></td><td>1 B</td><td><code>KeyLen</code> — UTF-8 key length (max 255 bytes)</td></tr>
          <tr><td><code>3</code></td><td>1 B</td><td><code>Flags</code> — <code>0x01</code> = deleted, <code>0x02</code> = has expiry ticks</td></tr>
          <tr><td><code>4–7</code></td><td>4 B</td><td><code>ValueLen</code> — value byte length</td></tr>
          <tr><td><code>8–9</code></td><td>2 B</td><td>Reserved</td></tr>
        </tbody>
      </table>
    </section>

    <section>
      <h2>Installation</h2>
      <p>The KV store is part of <code>BLite.Core</code> — no extra NuGet package needed.</p>
      <pre><code>dotnet add package BLite</code></pre>
    </section>

    <section>
      <h2>Basic Usage</h2>
      <pre><code>using BLite.Core;
using BLite.Core.KeyValue;

using var engine = new BLiteEngine("data.db");
IBLiteKvStore kv = engine.KvStore;

// Write (optional TTL)
kv.Set("session:abc", Encoding.UTF8.GetBytes("payload"), TimeSpan.FromHours(1));

// Read — returns null if absent or expired
byte[]? value = kv.Get("session:abc");

// Check existence
bool exists = kv.Exists("session:abc");

// Delete
kv.Delete("session:abc");

// Refresh the TTL without rewriting the value
kv.Refresh("session:abc", TimeSpan.FromHours(2));</code></pre>
    </section>

    <section>
      <h2>Prefix Scan</h2>
      <pre><code>// Enumerate all keys that start with "session:"
IEnumerable&lt;string&gt; sessionKeys = kv.ScanKeys("session:");

// Enumerate ALL keys (empty prefix)
IEnumerable&lt;string&gt; allKeys = kv.ScanKeys();</code></pre>
    </section>

    <section>
      <h2>Atomic Batches</h2>
      <p>
        Use <code>Batch()</code> to set and delete multiple keys under a <strong>single lock acquisition</strong> —
        all operations succeed or none do.
      </p>
      <pre><code>int applied = kv.Batch()
    .Set("k1", data1)
    .Set("k2", data2, TimeSpan.FromMinutes(30))
    .Delete("k3")
    .Execute();   // returns number of operations applied</code></pre>
    </section>

    <section>
      <h2>Options</h2>
      <p>Pass <code>BLiteKvOptions</code> to the engine / context constructor:</p>
      <pre><code>var kvOptions = new BLiteKvOptions
{
    DefaultTtl         = TimeSpan.FromDays(1),   // applied when no explicit TTL is given
    PurgeExpiredOnOpen = true                    // run PurgeExpired() automatically on startup
};

// BLiteEngine
using var engine = new BLiteEngine("data.db", kvOptions);

// DocumentDbContext
using var db = new MyDbContext("app.db", kvOptions);</code></pre>
    </section>

    <section>
      <h2>Purge Expired Entries</h2>
      <pre><code>// Manually sweep all pages and remove expired entries
int removed = kv.PurgeExpired();</code></pre>
      <p>
        Entries are <strong>soft-deleted</strong> in-place when first accessed after TTL expiry, so reads never
        return stale data. <code>PurgeExpired()</code> does a full-page sweep and reclaims wasted space.
      </p>
    </section>

    <section>
      <h2>Using with DocumentDbContext</h2>
      <pre><code>public partial class MyDbContext : DocumentDbContext
{
    public DocumentCollection&lt;ObjectId, User&gt; Users { get; set; } = null!;

    public MyDbContext(string path) : base(path) =&gt; InitializeCollections();
    public MyDbContext(string path, BLiteKvOptions kv) : base(path, kv) =&gt; InitializeCollections();
}

using var db = new MyDbContext("app.db");
db.KvStore.Set("last-sync", BitConverter.GetBytes(DateTime.UtcNow.Ticks));</code></pre>
    </section>

    <div class="info-box">
      <div class="info-header">📦 BLite.Caching</div>
      <p>
        Want a drop-in <code>IDistributedCache</code> backed by the KV store — with typed get/set, sliding expiry,
        and thundering-herd protection?
        Install the <strong>BLite.Caching</strong> package and call
        <code>builder.Services.AddBLiteDistributedCache("cache.db")</code>.
        See the <router-link to="/docs/getting-started">Getting Started</router-link> guide for details.
      </p>
    </div>

    <section>
      <h2>API Reference</h2>
      <table>
        <thead><tr><th>Method</th><th>Description</th></tr></thead>
        <tbody>
          <tr><td><code>Get(key)</code></td><td>Returns the raw bytes for <em>key</em>, or <code>null</code> if absent/expired.</td></tr>
          <tr><td><code>Set(key, value, ttl?)</code></td><td>Writes a value. Optional <code>TimeSpan</code> TTL overrides <code>BLiteKvOptions.DefaultTtl</code>.</td></tr>
          <tr><td><code>Delete(key)</code></td><td>Soft-deletes the entry. Returns <code>true</code> if found.</td></tr>
          <tr><td><code>Exists(key)</code></td><td>Returns <code>true</code> if the key exists and has not expired.</td></tr>
          <tr><td><code>Refresh(key, ttl)</code></td><td>Extends the expiry of an existing entry without rewriting its value.</td></tr>
          <tr><td><code>ScanKeys(prefix="")</code></td><td>Enumerates all live keys with the given prefix.</td></tr>
          <tr><td><code>PurgeExpired()</code></td><td>Reclaims storage used by expired entries. Returns count removed.</td></tr>
          <tr><td><code>Batch()</code></td><td>Returns a <code>KvBatch</code> builder for atomic multi-key operations.</td></tr>
        </tbody>
      </table>
    </section>
  </div>
</template>

<style scoped>
.doc-page {
  padding: 40px 0;
}

h1 {
  font-size: 2.5rem;
  font-weight: 800;
  margin-bottom: 16px;
}

.lead {
  font-size: 1.15rem;
  color: var(--text-secondary);
  margin-bottom: 40px;
  line-height: 1.7;
}

.info-box {
  background: rgba(231, 76, 60, 0.08);
  border: 1px solid rgba(231, 76, 60, 0.25);
  border-radius: 12px;
  padding: 20px 24px;
  margin: 32px 0;
}

.info-header {
  font-weight: 700;
  color: var(--blite-red);
  margin-bottom: 8px;
  font-size: 1rem;
}

section {
  margin: 40px 0;
}

h2 {
  font-size: 1.6rem;
  font-weight: 700;
  margin-bottom: 16px;
  color: var(--text-primary);
}

h3 {
  font-size: 1.15rem;
  font-weight: 600;
  margin: 24px 0 12px;
  color: var(--text-secondary);
}

p {
  color: var(--text-secondary);
  line-height: 1.7;
  margin-bottom: 12px;
}

pre {
  background: rgba(0, 0, 0, 0.4);
  border: 1px solid rgba(255, 255, 255, 0.07);
  border-radius: 10px;
  padding: 20px 24px;
  overflow-x: auto;
  font-size: 0.88rem;
  line-height: 1.6;
  margin: 16px 0;
}

code {
  font-family: 'JetBrains Mono', 'Fira Code', monospace;
  font-size: 0.88rem;
}

table {
  width: 100%;
  border-collapse: collapse;
  margin: 16px 0;
  font-size: 0.9rem;
}

th, td {
  text-align: left;
  padding: 10px 14px;
  border-bottom: 1px solid rgba(255, 255, 255, 0.06);
}

th {
  color: var(--blite-red);
  font-weight: 600;
  background: rgba(231, 76, 60, 0.05);
}

td code {
  background: rgba(255, 255, 255, 0.07);
  padding: 2px 6px;
  border-radius: 4px;
}

.title-gradient {
  background: linear-gradient(135deg, var(--blite-red), #ff8a65);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
}
</style>
