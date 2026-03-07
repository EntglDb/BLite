<template>
    <div class="doc-page">
        <h1>🗝️ <span class="title-gradient">Embedded Key-Value Store</span></h1>
        <p class="lead">
            BLite 3.2.0 ships a persistent
            <strong>key-value store co-located in the same database file</strong>
            — no extra process, no extra file, no separate daemon. Access it via
            <code>IBLiteKvStore</code> on any <code>BLiteEngine</code> or
            <code>DocumentDbContext</code>.
        </p>

        <div class="info-box">
            <div class="info-header">📌 How it works</div>
            <p>
                The KV engine uses a dedicated <code>PageType.KeyValue = 13</code> page chain stored alongside
                your document collections. A compact in-memory index is rebuilt
                at startup and keeps all lookups O(1). Writes, deletes, and
                batch operations are serialized through a single write lock —
                ACID-safe without blocking concurrent document reads.
            </p>
        </div>

        <section>
            <h2>Installation</h2>
            <p>
                The KV store is built into <code>BLite.Core</code> — no extra
                NuGet package required.
            </p>
            <pre><code>dotnet add package BLite</code></pre>
        </section>

        <section>
            <h2>Basic Usage</h2>
            <pre><code><span class="keyword">using</span> BLite.Core;
<span class="keyword">using</span> BLite.Core.KeyValue;

<span class="keyword">using var</span> engine = <span class="keyword">new</span> <span class="type">BLiteEngine</span>(<span class="string">"data.db"</span>);
<span class="type">IBLiteKvStore</span> kv = engine.KvStore;

<span class="comment">// Write — optional TTL per entry</span>
kv.Set(<span class="string">"session:abc"</span>, Encoding.UTF8.GetBytes(<span class="string">"payload"</span>), <span class="type">TimeSpan</span>.FromHours(<span class="number">1</span>));

<span class="comment">// Read — returns null if absent or expired</span>
<span class="keyword">byte</span>[]? value = kv.Get(<span class="string">"session:abc"</span>);

<span class="comment">// Check / Delete</span>
<span class="keyword">bool</span> exists = kv.Exists(<span class="string">"session:abc"</span>);
kv.Delete(<span class="string">"session:abc"</span>);

<span class="comment">// Extend the expiry without rewriting the value</span>
kv.Refresh(<span class="string">"session:abc"</span>, <span class="type">TimeSpan</span>.FromHours(<span class="number">2</span>));</code></pre>
        </section>

        <section>
            <h2>Prefix Scan</h2>
            <pre><code><span class="comment">// All keys starting with "session:"</span>
<span class="type">IEnumerable</span>&lt;<span class="keyword">string</span>&gt; sessionKeys = kv.ScanKeys(<span class="string">"session:"</span>);

<span class="comment">// Every key in the store</span>
<span class="type">IEnumerable</span>&lt;<span class="keyword">string</span>&gt; allKeys = kv.ScanKeys();</code></pre>
        </section>

        <section>
            <h2>Atomic Batches</h2>
            <p>
                <code>Batch()</code> groups multiple set/delete operations under
                a <strong>single lock acquisition</strong>. All operations
                succeed or none do.
            </p>
            <pre><code><span class="keyword">int</span> applied = kv.Batch()
    .Set(<span class="string">"k1"</span>, data1)
    .Set(<span class="string">"k2"</span>, data2, <span class="type">TimeSpan</span>.FromMinutes(<span class="number">30</span>))
    .Delete(<span class="string">"k3"</span>)
    .Execute();   <span class="comment">// returns number of operations applied</span></code></pre>
        </section>

        <section>
            <h2>Options</h2>
            <p>
                Pass <code>BLiteKvOptions</code> to the engine or context
                constructor:
            </p>
            <pre><code><span class="keyword">var</span> kvOptions = <span class="keyword">new</span> <span class="type">BLiteKvOptions</span>
{
    DefaultTtl         = <span class="type">TimeSpan</span>.FromDays(<span class="number">1</span>),  <span class="comment">// applied when no explicit TTL is given</span>
    PurgeExpiredOnOpen = <span class="keyword">true</span>               <span class="comment">// sweep on startup</span>
};

<span class="comment">// With BLiteEngine</span>
<span class="keyword">using var</span> engine = <span class="keyword">new</span> <span class="type">BLiteEngine</span>(<span class="string">"data.db"</span>, kvOptions);

<span class="comment">// With DocumentDbContext</span>
<span class="keyword">using var</span> db = <span class="keyword">new</span> <span class="type">MyDbContext</span>(<span class="string">"app.db"</span>, kvOptions);</code></pre>
        </section>

        <section>
            <h2>Using with DocumentDbContext</h2>
            <pre><code><span class="keyword">public partial class</span> <span class="type">MyDbContext</span> : <span class="type">DocumentDbContext</span>
{
    <span class="keyword">public</span> <span class="type">DocumentCollection</span>&lt;<span class="type">ObjectId</span>, <span class="type">User</span>&gt; Users { <span class="keyword">get</span>; <span class="keyword">set</span>; } = <span class="keyword">null</span>!;

    <span class="keyword">public</span> MyDbContext(<span class="keyword">string</span> path) : <span class="keyword">base</span>(path) =&gt; InitializeCollections();
    <span class="keyword">public</span> MyDbContext(<span class="keyword">string</span> path, <span class="type">BLiteKvOptions</span> kv) : <span class="keyword">base</span>(path, kv) =&gt; InitializeCollections();
}

<span class="keyword">using var</span> db = <span class="keyword">new</span> <span class="type">MyDbContext</span>(<span class="string">"app.db"</span>);
db.KvStore.Set(<span class="string">"last-sync"</span>, BitConverter.GetBytes(<span class="type">DateTime</span>.UtcNow.Ticks));</code></pre>
        </section>

        <section>
            <h2>Purge Expired Entries</h2>
            <p>
                Expired entries are soft-deleted on first access, so reads never
                return stale data.
                <code>PurgeExpired()</code> does a full-page sweep and reclaims
                wasted disk space.
            </p>
            <pre><code><span class="keyword">int</span> removed = kv.PurgeExpired();</code></pre>
        </section>

        <section>
            <h2>BLite.Caching — IDistributedCache</h2>
            <p>
                <code>BLite.Caching</code> wraps the embedded KV store as a
                fully compliant <code>IDistributedCache</code>. Drop it in
                anywhere you would use Redis or SQL Server cache — zero external
                dependencies.
            </p>
            <pre><code>dotnet add package BLite.Caching</code></pre>

            <h3>Register in ASP.NET Core</h3>
            <pre><code><span class="comment">// Program.cs</span>
builder.Services.AddBLiteDistributedCache(<span class="string">"cache.db"</span>);

<span class="comment">// With options</span>
builder.Services.AddBLiteDistributedCache(<span class="string">"cache.db"</span>, <span class="keyword">new</span> <span class="type">BLiteKvOptions</span>
{
    DefaultTtl         = <span class="type">TimeSpan</span>.FromMinutes(<span class="number">30</span>),
    PurgeExpiredOnOpen = <span class="keyword">true</span>
});</code></pre>

            <h3>Standard IDistributedCache</h3>
            <pre><code><span class="comment">// Inject IDistributedCache — works with any existing caching code</span>
<span class="keyword">await</span> cache.SetStringAsync(<span class="string">"key"</span>, <span class="string">"value"</span>, <span class="keyword">new</span> <span class="type">DistributedCacheEntryOptions</span>
{
    SlidingExpiration = <span class="type">TimeSpan</span>.FromMinutes(<span class="number">20</span>)
});
<span class="keyword">string</span>? value = <span class="keyword">await</span> cache.GetStringAsync(<span class="string">"key"</span>);</code></pre>

            <h3>Typed helpers (IBLiteCache)</h3>
            <p>
                Inject <code>IBLiteCache</code> for typed get/set via
                <code>System.Text.Json</code> and a thundering-herd-safe
                <code>GetOrSetAsync</code> backed by per-key
                <code>SemaphoreSlim</code>.
            </p>
            <pre><code><span class="comment">// Typed set</span>
<span class="keyword">await</span> cache.SetAsync(<span class="string">"user:42"</span>, myUser, <span class="keyword">new</span> <span class="type">DistributedCacheEntryOptions</span>
{
    AbsoluteExpirationRelativeToNow = <span class="type">TimeSpan</span>.FromHours(<span class="number">1</span>)
});

<span class="comment">// Typed get</span>
<span class="type">User</span>? user = <span class="keyword">await</span> cache.GetAsync&lt;<span class="type">User</span>&gt;(<span class="string">"user:42"</span>);

<span class="comment">// GetOrSet — stampede-safe factory</span>
<span class="type">User</span> user = <span class="keyword">await</span> cache.GetOrSetAsync&lt;<span class="type">User</span>&gt;(<span class="string">"user:42"</span>,
    factory: <span class="keyword">async</span> ct =&gt; <span class="keyword">await</span> db.LoadUserAsync(<span class="number">42</span>, ct),
    options: <span class="keyword">new</span> <span class="type">DistributedCacheEntryOptions</span>
    {
        SlidingExpiration = <span class="type">TimeSpan</span>.FromMinutes(<span class="number">20</span>)
    });</code></pre>

            <div class="info-box">
                <div class="info-header">📌 Note</div>
                <p>
                    <code>IBLiteCache</code> is a superset of
                    <code>IDistributedCache</code>. The same
                    <code>AddBLiteDistributedCache()</code> registration
                    satisfies both interfaces — inject whichever fits your code.
                </p>
            </div>
        </section>

        <section>
            <h2>BLite Studio</h2>
            <p>
                The <strong>Key-Value Store</strong> panel in BLite Studio lets
                you browse, inspect, create, and delete entries without writing
                code. A <em>Purge Expired</em> button reclaims disk space on
                demand.
            </p>
        </section>

        <section>
            <h2>API Reference</h2>
            <table>
                <thead>
                    <tr>
                        <th>Method</th>
                        <th>Description</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td><code>Get(key)</code></td>
                        <td>Returns the raw bytes for <em>key</em>, or <code>null</code> if absent or expired.</td>
                    </tr>
                    <tr>
                        <td><code>Set(key, value, ttl?)</code></td>
                        <td>Writes a value. Optional TTL overrides <code>BLiteKvOptions.DefaultTtl</code>.</td>
                    </tr>
                    <tr>
                        <td><code>Delete(key)</code></td>
                        <td>Removes the entry. Returns <code>true</code> if found.</td>
                    </tr>
                    <tr>
                        <td><code>Exists(key)</code></td>
                        <td>Returns <code>true</code> if the key exists and has not expired.</td>
                    </tr>
                    <tr>
                        <td><code>Refresh(key, ttl)</code></td>
                        <td>Extends the expiry without rewriting the value.</td>
                    </tr>
                    <tr>
                        <td><code>ScanKeys(prefix="")</code></td>
                        <td>Enumerates all live keys with the given prefix.</td>
                    </tr>
                    <tr>
                        <td><code>PurgeExpired()</code></td>
                        <td>Reclaims storage used by expired entries. Returns count removed.</td>
                    </tr>
                    <tr>
                        <td><code>Batch()</code></td>
                        <td>Returns a <code>KvBatch</code> builder for atomic multi-key operations.</td>
                    </tr>
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

.title-gradient {
    background: linear-gradient(135deg, var(--blite-red), #ff8a65);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
}
</style>