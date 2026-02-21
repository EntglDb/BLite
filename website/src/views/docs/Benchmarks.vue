<template>
  <div class="doc-page">
    <h1>⚡ <span class="title-gradient">Benchmarks</span></h1>
    <p class="lead">BLite performance benchmarks vs <strong>LiteDB 5.0.21</strong> and <strong>SQLite+JSON</strong> (Microsoft.Data.Sqlite + System.Text.Json) using BenchmarkDotNet on .NET 10.</p>

    <div class="info-box">
      <div class="info-header">📊 Benchmark Environment</div>
      <ul>
        <li><strong>OS:</strong> Windows 11 (10.0.22631.6495 / 23H2)</li>
        <li><strong>CPU:</strong> Intel Core i7-13800H — 14 physical cores, 20 logical @ 2.50GHz</li>
        <li><strong>Runtime:</strong> .NET 10.0.2 (X64 RyuJIT x86-64-v3)</li>
        <li><strong>BenchmarkDotNet:</strong> v0.15.8</li>
        <li><strong>Last Run:</strong> February 21, 2026</li>
      </ul>
      <p style="margin-top:12px"><strong>Engines:</strong> BLite (source-generated mappers) · LiteDB 5.0.21 (<code>Connection=direct</code>) · SQLite Microsoft.Data.Sqlite 10.0.2 + Dapper 2.1.66 (JSON blobs)</p>
    </div>

    <section>
      <h2>Document Schema</h2>
      <p>All benchmarks use a realistic e-commerce <code>CustomerOrder</code> document (~1–2 KB each):</p>
      <pre><code>CustomerOrder
├── Id, OrderNumber, PlacedAt, Status, Currency
├── Subtotal, TaxAmount, Total
├── Customer (CustomerContact)
│   ├── FullName, Email, Phone
│   └── BillingAddress (PostalAddress) → Street, City, ZipCode, Country
├── Shipping (ShippingInfo)
│   ├── Carrier, TrackingNumber, EstimatedDelivery
│   └── Destination (PostalAddress)
├── Lines (List&lt;OrderLine&gt; — 5 items)
│   └── each: Sku, ProductName, Qty, UnitPrice, Subtotal, Tags[3]
├── Notes (List&lt;OrderNote&gt; — 2 items)
└── Tags (List&lt;string&gt;)</code></pre>
      <p>1000 documents. Status distribution: ~25% each across <code>pending</code> · <code>confirmed</code> · <code>shipped</code> · <code>delivered</code>.</p>
    </section>

    <section>
      <h2>Summary</h2>
      <div class="highlights">
        <div class="highlight">
          <div class="highlight-value">5.6x</div>
          <div class="highlight-label">vs LiteDB Single Insert</div>
        </div>
        <div class="highlight">
          <div class="highlight-value">33x</div>
          <div class="highlight-label">vs SQLite Single Insert</div>
        </div>
        <div class="highlight">
          <div class="highlight-value">2.6x</div>
          <div class="highlight-label">vs LiteDB FindById</div>
        </div>
        <div class="highlight">
          <div class="highlight-value">3.5x</div>
          <div class="highlight-label">vs LiteDB Scan</div>
        </div>
      </div>
    </section>

    <section>
      <h2>Insert Performance</h2>

      <h3>Single Document Insert</h3>
      <table>
        <thead><tr><th>Engine</th><th>Mean</th><th>Allocated</th><th>Ratio</th></tr></thead>
        <tbody>
          <tr class="winner"><td><strong>BLite</strong></td><td><code>134.7 μs</code></td><td>114.53 KB</td><td><span class="badge-good">baseline</span></td></tr>
          <tr><td>LiteDB</td><td><code>704.3 μs</code></td><td>57.82 KB</td><td><span class="badge-slow">5.57x slower</span></td></tr>
          <tr><td>SQLite+JSON</td><td><code>4,209.1 μs</code></td><td>11.02 KB</td><td><span class="badge-slow">33.27x slower</span></td></tr>
        </tbody>
      </table>

      <h3>Batch Insert (1000 Documents, 1 Transaction)</h3>
      <table>
        <thead><tr><th>Engine</th><th>Mean</th><th>Allocated</th><th>Ratio</th></tr></thead>
        <tbody>
          <tr class="winner"><td><strong>BLite</strong></td><td><code>8,797 μs</code></td><td>52,930 KB</td><td><span class="badge-good">baseline</span></td></tr>
          <tr><td>LiteDB</td><td><code>27,376 μs</code></td><td>33,000 KB</td><td><span class="badge-slow">3.15x slower</span></td></tr>
          <tr><td>SQLite+JSON</td><td><code>25,128 μs</code></td><td>6,295 KB</td><td><span class="badge-slow">2.89x slower</span></td></tr>
        </tbody>
      </table>

      <div class="warning-box">
        <div class="warning-header">⚠️ Note on SQLite allocations</div>
        <p>SQLite reports minimal managed allocations because work is delegated to its native C library. Unmanaged memory is not captured by BenchmarkDotNet. BLite allocations are fully measured (100% managed code).</p>
      </div>
    </section>

    <section>
      <h2>Read Performance</h2>
      <p>Results from <code>DefaultJob</code> — standard BenchmarkDotNet multi-iteration run (most reliable).</p>

      <h3>FindById — Primary Key Lookup</h3>
      <table>
        <thead><tr><th>Engine</th><th>Mean</th><th>Allocated</th><th>Ratio</th></tr></thead>
        <tbody>
          <tr class="winner"><td><strong>BLite</strong></td><td><code>6.368 μs</code></td><td>6.60 KB</td><td><span class="badge-good">baseline</span></td></tr>
          <tr><td>LiteDB</td><td><code>16.466 μs</code></td><td>44.44 KB</td><td><span class="badge-slow">2.59x slower</span></td></tr>
          <tr><td>SQLite+JSON</td><td><code>62.807 μs</code></td><td>9.34 KB</td><td><span class="badge-slow">9.87x slower</span></td></tr>
        </tbody>
      </table>

      <h3>Scan — Filter by Status (<code>"shipped"</code>, ~250 / 1000 results)</h3>
      <table>
        <thead><tr><th>Engine</th><th>Mean</th><th>Allocated</th><th>Ratio</th></tr></thead>
        <tbody>
          <tr class="winner"><td><strong>BLite</strong></td><td><code>3,433 μs</code></td><td>5,090 KB</td><td><span class="badge-good">baseline</span></td></tr>
          <tr><td>LiteDB</td><td><code>11,953 μs</code></td><td>17,295 KB</td><td><span class="badge-slow">3.51x slower</span></td></tr>
          <tr><td>SQLite+JSON</td><td><code>9,677 μs</code></td><td>7,803 KB</td><td><span class="badge-slow">2.84x slower</span></td></tr>
        </tbody>
      </table>
    </section>

    <section>
      <h2>Serialization (BSON vs JSON)</h2>
      <p>Standalone serialization — no I/O, measured on a complex nested object.</p>

      <h3>Single Object</h3>
      <table>
        <thead><tr><th>Operation</th><th>BSON (BLite)</th><th>JSON (STJ)</th><th>Speedup</th></tr></thead>
        <tbody>
          <tr class="winner"><td>Serialize</td><td><code>1.42 μs</code></td><td><code>3.43 μs</code></td><td><span class="badge-good">2.4x</span></td></tr>
          <tr class="winner"><td>Deserialize</td><td><code>3.34 μs</code></td><td><code>7.01 μs</code></td><td><span class="badge-good">2.1x</span></td></tr>
          <tr class="winner"><td>Alloc (Serialize)</td><td><code>0 B</code> ✅</td><td><code>1,880 B</code></td><td><span class="badge-good">100%</span></td></tr>
        </tbody>
      </table>

      <h3>Bulk (10,000 Objects)</h3>
      <table>
        <thead><tr><th>Operation</th><th>BSON (BLite)</th><th>JSON (STJ)</th><th>Speedup</th></tr></thead>
        <tbody>
          <tr class="winner"><td>Serialize</td><td><code>14.99 ms</code></td><td><code>21.40 ms</code></td><td><span class="badge-good">1.43x</span></td></tr>
          <tr class="winner"><td>Deserialize</td><td><code>18.92 ms</code></td><td><code>42.96 ms</code></td><td><span class="badge-good">2.27x</span></td></tr>
          <tr class="winner"><td>Alloc (Serialize)</td><td><code>0 B</code> ✅</td><td><code>19.19 MB</code></td><td><span class="badge-good">100%</span></td></tr>
        </tbody>
      </table>
    </section>

    <section>
      <h2>Why BLite Is Faster</h2>
      <ul>
        <li>✅ <strong>C-BSON Format</strong> — Field name compression (2-byte IDs vs full string keys)</li>
        <li>✅ <strong>Zero-Copy I/O</strong> — Direct <code>Span&lt;byte&gt;</code> operations throughout the pipeline</li>
        <li>✅ <strong>Memory Pooling</strong> — <code>ArrayPool</code> for buffer reuse</li>
        <li>✅ <strong>Stack Allocation</strong> — <code>stackalloc</code> for temporary buffers</li>
        <li>✅ <strong>Source Generators</strong> — Compile-time serialization, no reflection at runtime</li>
        <li>✅ <strong>vs LiteDB</strong> — LiteDB uses reflection-based BSON and a B+ tree requiring page splits; BLite's generated mappers and append-only WAL eliminate per-document reflection overhead</li>
      </ul>
    </section>

    <section>
      <h2>Running Benchmarks</h2>
      <pre><code><span class="comment"># Full BenchmarkDotNet run (all engines, all categories)</span>
dotnet run -c Release --project src/BLite.Benchmark

<span class="comment"># Quick manual run (~10s, no BDN overhead)</span>
dotnet run -c Release --project src/BLite.Benchmark -- manual

<span class="comment"># Results:</span>
<span class="comment"># src/BLite.Benchmark/BenchmarkDotNet.Artifacts/results/</span></code></pre>
    </section>

    <div class="info-box">
      <div class="info-header">📄 Full Report</div>
      <p>Complete benchmark data with error margins, standard deviations, and GC metrics: <a href="https://github.com/EntglDb/BLite/blob/main/BENCHMARKS.md" target="_blank">BENCHMARKS.md</a></p>
    </div>
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

.info-box { background: rgba(231, 76, 60, 0.05); border: 1px solid rgba(231, 76, 60, 0.2); border-radius: 8px; padding: 20px; margin: 24px 0; }
.info-header { font-weight: 600; color: var(--blite-red); margin-bottom: 12px; font-size: 1.1rem; }
.info-box ul { margin-top: 12px; }
.info-box a { color: var(--blite-red); text-decoration: underline; }

.warning-box { background: rgba(251, 146, 60, 0.05); border: 1px solid rgba(251, 146, 60, 0.3); border-radius: 8px; padding: 20px; margin: 24px 0; }
.warning-header { font-weight: 600; color: #fb923c; margin-bottom: 12px; font-size: 1.1rem; }
.warning-box p { color: var(--text-secondary); }

.highlights { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 16px; margin: 24px 0; }
.highlight { background: rgba(231, 76, 60, 0.1); border: 1px solid rgba(231, 76, 60, 0.3); border-radius: 8px; padding: 20px; text-align: center; }
.highlight-value { font-size: 2rem; font-weight: 800; font-family: var(--font-mono); color: var(--blite-red); }
.highlight-label { font-size: 0.85rem; color: var(--text-muted); margin-top: 8px; text-transform: uppercase; letter-spacing: 1px; }

table { width: 100%; border-collapse: collapse; margin: 24px 0; }
th, td { padding: 12px; text-align: left; border-bottom: 1px solid rgba(231, 76, 60, 0.2); }
th { color: var(--blite-red); font-weight: 600; }
td { color: var(--text-secondary); }
tr.winner { background: rgba(231, 76, 60, 0.05); }

.badge-good { display: inline-block; padding: 4px 8px; background: rgba(34, 197, 94, 0.2); color: #22c55e; border-radius: 4px; font-size: 0.85rem; font-weight: 600; }
.badge-slow { display: inline-block; padding: 4px 8px; background: rgba(231, 76, 60, 0.15); color: #f87171; border-radius: 4px; font-size: 0.85rem; font-weight: 600; }

.comment { color: #52525b; font-style: italic; }
</style>
