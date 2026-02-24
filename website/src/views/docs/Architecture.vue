<template>
  <div class="doc-page">
    <h1>🏗️ <span class="title-gradient">Architecture</span></h1>
    <p class="lead">Deep dive into BLite's internal architecture, storage engine, and BSON format.</p>

    <section>
      <h2>Storage Engine</h2>
      <p>BLite uses a <strong>page-based storage engine</strong> with memory-mapped files:</p>
      <ul>
        <li><strong>PageFile</strong> - Memory-mapped file manager with block-aligned growth</li>
        <li><strong>Write-Ahead Log (WAL)</strong> - Durability and crash recovery</li>
        <li><strong>SlottedPage</strong> - Variable-length record storage</li>
        <li><strong>B-Tree Index</strong> - Logarithmic key lookups</li>
      </ul>

      <h3>File Growth Strategy</h3>
      <p>The file grows in <strong>fixed-size blocks</strong> (aligned allocation), avoiding the exponential doubling of traditional databases. Waste is bounded to at most one block regardless of database size:</p>
      <pre><code><span class="comment">// PageFileConfig presets</span>
<span class="type">PageFileConfig</span>.Small   <span class="comment">// 8 KB pages,  grow by  512 KB</span>
<span class="type">PageFileConfig</span>.Default <span class="comment">// 16 KB pages, grow by    1 MB</span>
<span class="type">PageFileConfig</span>.Large   <span class="comment">// 32 KB pages, grow by    2 MB</span></code></pre>
    </section>

    <section>
      <h2>Page Structure</h2>
      <pre><code><span class="comment">// Page Header (32 bytes)</span>
<span class="keyword">struct</span> <span class="type">PageHeader</span>
{
    <span class="keyword">uint</span> PageId;
    <span class="keyword">ushort</span> PageType;  <span class="comment">// Data, Index, Free</span>
    <span class="keyword">ushort</span> FreeSpace;
    <span class="keyword">uint</span> NextPageId;
    <span class="comment">// ...</span>
}</code></pre>
    </section>

    <section>
      <h2>C-BSON Format</h2>
      <p>BLite uses <strong>C-BSON (Compressed BSON)</strong> - a space-efficient variant that achieves 30-60% size reduction through field name compression:</p>
      
      <h3>Key Compression</h3>
      <p>Standard BSON stores field names as null-terminated strings in every document. C-BSON replaces them with <strong>2-byte numeric field IDs</strong>:</p>
      <pre><code><span class="comment">// Standard BSON element</span>
[<span class="number">1</span> byte: type][N bytes: <span class="string">"field_name\0"</span>][value]

<span class="comment">// C-BSON element (compressed)</span>
[<span class="number">1</span> byte: type][<span class="number">2</span> bytes: field_id][value]</code></pre>

      <h3>Schema-Based Mapping</h3>
      <p>Field IDs are assigned via a schema dictionary:</p>
      <ul>
        <li><code>"_id"</code> → Field ID <code>1</code> (2 bytes instead of 4)</li>
        <li><code>"email"</code> → Field ID <code>2</code> (2 bytes instead of 6)</li>
        <li><code>"created_at"</code> → Field ID <code>3</code> (2 bytes instead of 11)</li>
      </ul>

      <h3>Benefits</h3>
      <ul>
        <li><strong>30-60% smaller documents</strong> - Less storage and faster I/O</li>
        <li><strong>Zero allocation</strong> - Direct <code>Span&lt;byte&gt;</code> operations</li>
        <li><strong>BSON type compatible</strong> - Same type codes and value encoding</li>
        <li><strong>Schema evolution</strong> - Add/remove fields safely</li>
      </ul>
    </section>

    <section>
      <h2>Transaction Flow</h2>
      <pre><code><span class="number">1</span>. BeginTransaction()
   └─> Allocate transaction ID
   └─> Create WAL entry (BEGIN)

<span class="number">2</span>. Write Operations
   └─> Write to WAL <span class="comment">(uncommitted)</span>
   └─> Cache in memory

<span class="number">3</span>. Commit()
   └─> Write WAL commit marker
   └─> Flush to PageFile
   └─> Clear WAL cache

<span class="number">4</span>. Rollback() <span class="comment">(if needed)</span>
   └─> Discard WAL entries
   └─> Restore from checkpoint</code></pre>
    </section>

    <section>
      <h2>Indexing Strategies</h2>
      
      <h3>B-Tree Index</h3>
      <p>Used for:</p>
      <ul>
        <li>Primary key lookups</li>
        <li>Range queries (<code>&gt;</code>, <code>&lt;</code>, <code>BETWEEN</code>)</li>
        <li>Prefix searches (<code>StartsWith</code>)</li>
      </ul>

      <h3>R-Tree Index</h3>
      <p>Used for:</p>
      <ul>
        <li>Geospatial queries</li>
        <li>Bounding box searches</li>
        <li>Nearest neighbor (k-NN)</li>
      </ul>
    </section>

    <section>
      <h2>Memory Management</h2>
      <p>BLite minimizes garbage collection pressure:</p>
      <ul>
        <li>✅ <strong><code>stackalloc</code></strong> for temporary buffers</li>
        <li>✅ <strong><code>ArrayPool</code></strong> for reusable arrays</li>
        <li>✅ <strong>Memory-mapped files</strong> for OS-level caching</li>
        <li>✅ <strong>Span&lt;T&gt;</strong> for zero-copy operations</li>
      </ul>
    </section>

    <section>
      <h2>Concurrency Model</h2>
      <p>BLite supports <strong>multi-threading within a single process</strong>:</p>
      <ul>
        <li>✅ Multiple readers simultaneously</li>
        <li>✅ Single writer at a time (per transaction)</li>
        <li>✅ Snapshot isolation for consistency</li>
        <li>⚠️ <strong>Not designed for multi-process access</strong></li>
      </ul>
    </section>

    <section>
      <h2>Performance Characteristics</h2>
      <table>
        <thead>
          <tr>
            <th>Operation</th>
            <th>Complexity</th>
            <th>Notes</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>Insert</td>
            <td><code>O(log n)</code></td>
            <td>B-Tree index insert</td>
          </tr>
          <tr>
            <td>FindById</td>
            <td><code>O(log n)</code></td>
            <td>Primary key lookup</td>
          </tr>
          <tr>
            <td>Range Query</td>
            <td><code>O(log n + k)</code></td>
            <td>k = result size</td>
          </tr>
          <tr>
            <td>Spatial Query</td>
            <td><code>O(log n + k)</code></td>
            <td>R-Tree search</td>
          </tr>
          <tr>
            <td>Full Scan</td>
            <td><code>O(n)</code></td>
            <td>No index available</td>
          </tr>
        </tbody>
      </table>
    </section>

    <section>
      <h2>Crash Recovery</h2>
      <p>BLite uses WAL for durability:</p>
      <pre><code><span class="comment">// On database open</span>
<span class="keyword">if</span> (WAL.Exists())
{
    <span class="comment">// Replay uncommitted transactions</span>
    WAL.Recover();
    
    <span class="comment">// Apply committed writes</span>
    WAL.Flush();
    
    <span class="comment">// Clear WAL</span>
    WAL.Truncate();
}</code></pre>
    </section>

    <section>
      <h2>Hot Backup</h2>
      <p>BLite supports <strong>online, consistent backups</strong> without stopping the engine. Concurrent reads and writes remain safe during the copy:</p>
      <pre><code><span class="comment">// 1. Checkpoint WAL into PageFile (under commit lock)</span>
<span class="comment">// 2. Copy PageFile to destination (while holding the lock)</span>
<span class="comment">// 3. Release lock — normal writes resume</span>

<span class="comment">// The WAL is NOT copied — the destination is already consistent</span></code></pre>
      <p>See <strong>Transactions → Hot Backup</strong> for the user-facing API.</p>
    </section>

    <section>
      <h2>Design Philosophy</h2>
      <ul>
        <li>🎯 <strong>Performance First</strong> - Zero allocation wherever possible</li>
        <li>🛡️ <strong>ACID Compliance</strong> - Full transaction support</li>
        <li>🚀 <strong>Modern .NET</strong> - Leverage Span&lt;T&gt;, Memory&lt;T&gt;</li>
        <li>🔧 <strong>Developer Experience</strong> - LINQ, attributes, source generators</li>
        <li>📦 <strong>Embedded First</strong> - No external dependencies</li>
      </ul>
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
ul { margin: 16px 0; padding-left: 24px; }
li { margin-bottom: 12px; color: var(--text-secondary); line-height: 1.6; }
code { font-family: var(--font-mono); font-size: 0.9rem; background: rgba(231, 76, 60, 0.1); padding: 2px 6px; border-radius: 4px; color: var(--blite-red); }
pre { background: rgba(10, 10, 10, 0.6); border: 1px solid rgba(231, 76, 60, 0.2); border-radius: 8px; padding: 20px; overflow-x: auto; margin: 16px 0; }
pre code { background: none; padding: 0; color: var(--text-secondary); }
table { width: 100%; border-collapse: collapse; margin: 24px 0; }
th, td { padding: 12px; text-align: left; border-bottom: 1px solid rgba(231, 76, 60, 0.2); }
th { color: var(--blite-red); font-weight: 600; }
td { color: var(--text-secondary); }
.keyword { color: var(--blite-red); }
.type { color: #06b6d4; }
.string { color: #a1a1aa; }
.number { color: #06b6d4; }
.comment { color: #52525b; font-style: italic; }
</style>
