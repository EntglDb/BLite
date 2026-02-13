<template>
  <div class="doc-page">
    <h1>⚡ Benchmarks</h1>
    <p class="lead">BLite performance benchmarks vs SQLite using BenchmarkDotNet on .NET 10.</p>

    <div class="info-box">
      <div class="info-header">📊 Benchmark Environment</div>
      <ul>
        <li><strong>OS:</strong> Windows 11 (23H2)</li>
        <li><strong>CPU:</strong> Intel Core i7-13800H (14 cores @ 2.50GHz)</li>
        <li><strong>Runtime:</strong> .NET 10.0.2 (X64 RyuJIT)</li>
        <li><strong>Last Run:</strong> February 13, 2026</li>
      </ul>
    </div>

    <section>
      <h2>Summary</h2>
      <div class="highlights">
        <div class="highlight">
          <div class="highlight-value">8.2x</div>
          <div class="highlight-label">Faster Single Insert</div>
        </div>
        <div class="highlight">
          <div class="highlight-value">2.4x</div>
          <div class="highlight-label">Faster Serialization</div>
        </div>
        <div class="highlight">
          <div class="highlight-value">0 B</div>
          <div class="highlight-label">BSON Serialize Alloc</div>
        </div>
        <div class="highlight">
          <div class="highlight-value">2.1x</div>
          <div class="highlight-label">Faster Deserialization</div>
        </div>
      </div>
    </section>

    <section>
      <h2>Insert Performance</h2>
      <h3>Single Document Insert</h3>
      <table>
        <thead>
          <tr>
            <th>Database</th>
            <th>Mean Time</th>
            <th>Allocated</th>
            <th>Speedup</th>
          </tr>
        </thead>
        <tbody>
          <tr class="winner">
            <td><strong>BLite</strong></td>
            <td><code>355.8 μs</code></td>
            <td>128.89 KB</td>
            <td><span class="badge-good">8.2x faster</span></td>
          </tr>
          <tr>
            <td>SQLite</td>
            <td><code>2,916.3 μs</code></td>
            <td>6.67 KB</td>
            <td>—</td>
          </tr>
        </tbody>
      </table>
    </section>

    <div class="warning-box">
      <div class="warning-header">⚠️ Important Note on SQLite Memory</div>
      <p>The "Allocated" metrics shown for SQLite only measure <strong>managed .NET allocations</strong>. SQLite's native C library allocates significant unmanaged memory that is <strong>not captured</strong> by BenchmarkDotNet.</p>
      <p>In reality, SQLite's total memory footprint is much higher than reported. BLite's allocations are fully measured since it's 100% managed code.</p>
    </div>

    <section>
      <h2>Serialization Performance</h2>
      <h3>Single Object</h3>
      <table>
        <thead>
          <tr>
            <th>Operation</th>
            <th>BSON (BLite)</th>
            <th>JSON (System.Text.Json)</th>
            <th>Speedup</th>
          </tr>
        </thead>
        <tbody>
          <tr class="winner">
            <td>Serialize</td>
            <td><code>1.42 μs</code></td>
            <td><code>3.43 μs</code></td>
            <td><span class="badge-good">2.4x</span></td>
          </tr>
          <tr class="winner">
            <td>Deserialize</td>
            <td><code>3.34 μs</code></td>
            <td><code>7.01 μs</code></td>
            <td><span class="badge-good">2.1x</span></td>
          </tr>
        </tbody>
      </table>

      <h3>Memory Allocations (Single Object)</h3>
      <table>
        <thead>
          <tr>
            <th>Operation</th>
            <th>BSON</th>
            <th>JSON</th>
          </tr>
        </thead>
        <tbody>
          <tr class="winner">
            <td>Serialize</td>
            <td><code>0 B</code> ✅</td>
            <td><code>1,880 B</code></td>
          </tr>
          <tr>
            <td>Deserialize</td>
            <td><code>5,704 B</code></td>
            <td><code>6,600 B</code></td>
          </tr>
        </tbody>
      </table>

      <h3>Bulk Operations (10,000 Objects)</h3>
      <table>
        <thead>
          <tr>
            <th>Operation</th>
            <th>BSON (BLite)</th>
            <th>JSON</th>
            <th>Speedup</th>
          </tr>
        </thead>
        <tbody>
          <tr class="winner">
            <td>Serialize</td>
            <td><code>14.99 ms</code></td>
            <td><code>21.40 ms</code></td>
            <td><span class="badge-good">1.43x</span></td>
          </tr>
          <tr class="winner">
            <td>Deserialize</td>
            <td><code>18.92 ms</code></td>
            <td><code>42.96 ms</code></td>
            <td><span class="badge-good">2.27x</span></td>
          </tr>
        </tbody>
      </table>

      <h3>Memory Allocations (10,000 Objects)</h3>
      <table>
        <thead>
          <tr>
            <th>Operation</th>
            <th>BSON</th>
            <th>JSON</th>
            <th>Savings</th>
          </tr>
        </thead>
        <tbody>
          <tr class="winner">
            <td>Serialize</td>
            <td><code>0 B</code> ✅</td>
            <td><code>19.19 MB</code></td>
            <td><span class="badge-good">100%</span></td>
          </tr>
          <tr class="winner">
            <td>Deserialize</td>
            <td><code>57.98 MB</code></td>
            <td><code>66.94 MB</code></td>
            <td><span class="badge-good">13%</span></td>
          </tr>
        </tbody>
      </table>
    </section>

    <section>
      <h2>Why BLite Is Faster</h2>
      <ul>
        <li>✅ <strong>C-BSON Format</strong> - Field name compression (2-byte IDs vs strings)</li>
        <li>✅ <strong>Zero-Copy I/O</strong> - Direct <code>Span&lt;byte&gt;</code> operations</li>
        <li>✅ <strong>Memory Pooling</strong> - <code>ArrayPool</code> for buffer reuse</li>
        <li>✅ <strong>Stack Allocation</strong> - <code>stackalloc</code> for temp buffers</li>
        <li>✅ <strong>Source Generators</strong> - Compile-time serialization code</li>
      </ul>
    </section>

    <section>
      <h2>Test Workload</h2>
      <p>Benchmarks use a complex <code>Person</code> document with:</p>
      <ul>
        <li>10 employment history entries</li>
        <li>Nested address object</li>
        <li>Lists of tags (5 strings per entry)</li>
        <li>ObjectId, DateTime, Decimal types</li>
        <li><strong>~150 fields total</strong> per document</li>
      </ul>
    </section>

    <section>
      <h2>Running Benchmarks</h2>
      <pre><code><span class="comment"># Clone repository</span>
git clone https://github.com/EntglDb/BLite.git
cd BLite

<span class="comment"># Run all benchmarks</span>
dotnet run -c Release --project src/BLite.Benchmark

<span class="comment"># Results will be in:</span>
<span class="comment"># BenchmarkDotNet.Artifacts/results/*.md</span></code></pre>
    </section>

    <div class="info-box">
      <div class="info-header">📄 Full Report</div>
      <p>For complete benchmark data including error margins, standard deviations, and GC metrics, see <a href="https://github.com/EntglDb/BLite/blob/main/BENCHMARKS.md" target="_blank">BENCHMARKS.md</a> in the repository.</p>
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

.comment { color: #52525b; font-style: italic; }
</style>
