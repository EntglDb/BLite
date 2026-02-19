<template>
  <div class="doc-page">
    <h1>⚖️ <span class="title-gradient">Comparisons & Positioning</span></h1>
    <p class="lead">How BLite fits in the .NET embedded database landscape — and why it's unlike anything else available today.</p>

    <section>
      <h2>The Embedded Database Landscape</h2>
      <p>The .NET ecosystem has several options for embedded or local data storage. Each one makes different trade-offs between performance, size, flexibility, and modernity. BLite was designed to fill a gap that none of the existing solutions cover.</p>
    </section>

    <section>
      <h2>Feature Comparison</h2>
      <div class="table-scroll">
        <table>
          <thead>
            <tr>
              <th>Feature</th>
              <th class="highlight-col">BLite</th>
              <th>LiteDB</th>
              <th>SQLite + EF Core</th>
              <th>Realm .NET</th>
              <th>RavenDB Embedded</th>
              <th>Akavache</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>Data Model</td>
              <td class="highlight-col"><strong>Document (BSON)</strong></td>
              <td>Document (BSON)</td>
              <td>Relational</td>
              <td>Object</td>
              <td>Document (JSON)</td>
              <td>Key-Value</td>
            </tr>
            <tr>
              <td>NativeAOT / Trimming</td>
              <td class="highlight-col"><span class="badge-good">✅ Full</span></td>
              <td><span class="badge-bad">❌</span></td>
              <td><span class="badge-mid">⚠️ Partial</span></td>
              <td><span class="badge-bad">❌</span></td>
              <td><span class="badge-bad">❌</span></td>
              <td><span class="badge-bad">❌</span></td>
            </tr>
            <tr>
              <td>Reflection-Free</td>
              <td class="highlight-col"><span class="badge-good">✅ Zero</span></td>
              <td><span class="badge-bad">❌ Heavy</span></td>
              <td><span class="badge-bad">❌ Heavy</span></td>
              <td><span class="badge-bad">❌</span></td>
              <td><span class="badge-bad">❌</span></td>
              <td><span class="badge-bad">❌</span></td>
            </tr>
            <tr>
              <td>Source Generators</td>
              <td class="highlight-col"><span class="badge-good">✅ Roslyn</span></td>
              <td><span class="badge-bad">❌</span></td>
              <td><span class="badge-bad">❌</span></td>
              <td>Fody weaver</td>
              <td><span class="badge-bad">❌</span></td>
              <td><span class="badge-bad">❌</span></td>
            </tr>
            <tr>
              <td>Dependencies</td>
              <td class="highlight-col"><span class="badge-good">0</span></td>
              <td>0</td>
              <td>Many (EF Core)</td>
              <td>Native (C++)</td>
              <td>Many</td>
              <td>Reactive Ext.</td>
            </tr>
            <tr>
              <td>Native Code</td>
              <td class="highlight-col"><span class="badge-good">None (pure C#)</span></td>
              <td>None</td>
              <td>SQLite native lib</td>
              <td>C++ core</td>
              <td>C++ Voron engine</td>
              <td>SQLite native lib</td>
            </tr>
            <tr>
              <td>Spatial Indexing</td>
              <td class="highlight-col"><span class="badge-good">✅ R-Tree</span></td>
              <td><span class="badge-bad">❌</span></td>
              <td>Extension</td>
              <td><span class="badge-bad">❌</span></td>
              <td><span class="badge-good">✅</span></td>
              <td><span class="badge-bad">❌</span></td>
            </tr>
            <tr>
              <td>Change Data Capture</td>
              <td class="highlight-col"><span class="badge-good">✅ Built-in</span></td>
              <td><span class="badge-bad">❌</span></td>
              <td><span class="badge-bad">❌</span></td>
              <td>Notifications</td>
              <td>Subscriptions</td>
              <td><span class="badge-bad">❌</span></td>
            </tr>
            <tr>
              <td>ACID Transactions</td>
              <td class="highlight-col"><span class="badge-good">✅</span></td>
              <td><span class="badge-good">✅</span></td>
              <td><span class="badge-good">✅</span></td>
              <td><span class="badge-good">✅</span></td>
              <td><span class="badge-good">✅</span></td>
              <td><span class="badge-bad">❌</span></td>
            </tr>
            <tr>
              <td>Async API</td>
              <td class="highlight-col"><span class="badge-good">✅ Full</span></td>
              <td><span class="badge-bad">❌</span></td>
              <td><span class="badge-good">✅</span></td>
              <td><span class="badge-good">✅</span></td>
              <td><span class="badge-good">✅</span></td>
              <td><span class="badge-good">✅</span></td>
            </tr>
            <tr>
              <td>P2P / Distributed</td>
              <td class="highlight-col"><span class="badge-good">✅ via EntglDb</span></td>
              <td><span class="badge-bad">❌</span></td>
              <td><span class="badge-bad">❌</span></td>
              <td>Atlas Sync</td>
              <td>Cluster mode</td>
              <td><span class="badge-bad">❌</span></td>
            </tr>
            <tr>
              <td>Platforms</td>
              <td class="highlight-col">Any .NET</td>
              <td>.NET Framework/Core</td>
              <td>Any .NET</td>
              <td>iOS, Android, .NET</td>
              <td>Server-side</td>
              <td>Xamarin / MAUI</td>
            </tr>
            <tr>
              <td>License</td>
              <td class="highlight-col">MIT</td>
              <td>MIT</td>
              <td>Mixed</td>
              <td>Apache 2.0</td>
              <td>AGPL / Commercial</td>
              <td>MIT</td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>

    <section>
      <h2>What Makes BLite Unique</h2>
      <div class="highlights">
        <div class="highlight">
          <div class="highlight-value">0</div>
          <div class="highlight-label">Reflection Calls</div>
        </div>
        <div class="highlight">
          <div class="highlight-value">0</div>
          <div class="highlight-label">Dependencies</div>
        </div>
        <div class="highlight">
          <div class="highlight-value">0</div>
          <div class="highlight-label">Native Binaries</div>
        </div>
        <div class="highlight">
          <div class="highlight-value">100%</div>
          <div class="highlight-label">AOT Compatible</div>
        </div>
      </div>
      <p>BLite is the only embedded document database for .NET that combines <strong>source-generated mappers</strong>, <strong>zero reflection</strong>, and <strong>full NativeAOT support</strong> in a pure managed library with zero external dependencies.</p>
      <p>This means:</p>
      <ul>
        <li><strong>Instant startup</strong> — no runtime type scanning or mapper initialization.</li>
        <li><strong>Trimmer-safe</strong> — publish tiny self-contained binaries without worrying about reflection metadata.</li>
        <li><strong>Cross-platform without friction</strong> — no native interop, no platform-specific builds.</li>
        <li><strong>Compile-time safety</strong> — mapping errors surface at build time, not at runtime.</li>
      </ul>
    </section>

    <section>
      <h2>🌐 A Cosmos DB for Embedded Applications</h2>
      <p>Azure Cosmos DB popularized the idea of a globally distributed, multi-model document database. BLite brings the same <strong>document-oriented philosophy</strong> to the embedded world:</p>
      <div class="table-scroll">
        <table>
          <thead>
            <tr>
              <th>Concept</th>
              <th>Azure Cosmos DB</th>
              <th class="highlight-col">BLite</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>Data Format</td>
              <td>JSON / BSON</td>
              <td class="highlight-col">BSON</td>
            </tr>
            <tr>
              <td>Data Model</td>
              <td>Document-oriented</td>
              <td class="highlight-col">Document-oriented</td>
            </tr>
            <tr>
              <td>Partitioning</td>
              <td>Partition keys</td>
              <td class="highlight-col">Collections</td>
            </tr>
            <tr>
              <td>Schema</td>
              <td>Schema-free</td>
              <td class="highlight-col">Schema-free</td>
            </tr>
            <tr>
              <td>Indexing</td>
              <td>Automatic + custom</td>
              <td class="highlight-col">Explicit indexes + R-Tree</td>
            </tr>
            <tr>
              <td>Transactions</td>
              <td>Scoped to partition</td>
              <td class="highlight-col">Full ACID per database</td>
            </tr>
            <tr>
              <td>Change Feed</td>
              <td>Change Feed</td>
              <td class="highlight-col">Change Data Capture</td>
            </tr>
            <tr>
              <td>Distribution</td>
              <td>Cloud-native global</td>
              <td class="highlight-col">Embedded P2P via EntglDb</td>
            </tr>
            <tr>
              <td>Deployment</td>
              <td>Cloud service</td>
              <td class="highlight-col">In-process library</td>
            </tr>
            <tr>
              <td>Cost</td>
              <td>RU-based billing</td>
              <td class="highlight-col">Free &amp; open source</td>
            </tr>
          </tbody>
        </table>
      </div>
      <p>If you think of Cosmos DB as <em>"a document database that scales globally in the cloud"</em>, then BLite is <em>"a document database that runs locally with cloud-like capabilities on any device."</em></p>
    </section>

    <section>
      <h2>🔗 BLite + EntglDb = Embedded Distributed Database</h2>
      <p>On its own, BLite is a powerful single-node embedded database. When paired with <a href="https://www.entgldb.com" target="_blank">EntglDb</a>, it becomes something entirely new in the .NET ecosystem: an <strong>embedded, peer-to-peer distributed document database</strong>.</p>

      <div class="info-box">
        <div class="info-header">🌐 EntglDb — Peer-to-Peer Data Sync for .NET</div>
        <p><a href="https://www.entgldb.com" target="_blank">EntglDb</a> is a distributed data synchronization framework that provides:</p>
        <ul>
          <li><strong>P2P Replication</strong> — nodes sync directly without a central server.</li>
          <li><strong>Conflict Resolution</strong> — automatic CRDT-based or custom resolution strategies.</li>
          <li><strong>Offline-First</strong> — each node works independently and syncs when connected.</li>
          <li><strong>Pluggable Persistence</strong> — BLite is a first-class persistence backend.</li>
        </ul>
      </div>

      <div class="architecture-diagram">
        <div class="arch-row">
          <div class="arch-node app">📱 App A<span class="node-sub">BLite + EntglDb</span></div>
          <div class="arch-connector">⟷</div>
          <div class="arch-node app">💻 App B<span class="node-sub">BLite + EntglDb</span></div>
          <div class="arch-connector">⟷</div>
          <div class="arch-node app">🖥️ App C<span class="node-sub">BLite + EntglDb</span></div>
        </div>
        <div class="arch-label">Each node runs a full BLite database locally — data syncs peer-to-peer via EntglDb</div>
      </div>

      <p>This combination enables scenarios that were previously only possible with cloud backends:</p>
      <ul>
        <li><strong>Multi-device sync</strong> — MAUI apps on phones, tablets, and desktops stay in sync without a server.</li>
        <li><strong>Edge computing</strong> — IoT devices replicate readings across a mesh network.</li>
        <li><strong>Collaborative apps</strong> — multiple users share data in real-time with conflict resolution.</li>
        <li><strong>Resilient architectures</strong> — no single point of failure; every node owns its data.</li>
      </ul>
    </section>

    <section>
      <h2>🎯 Target Scenarios</h2>
      <div class="scenarios">
        <div class="scenario">
          <div class="scenario-icon">📱</div>
          <h3>Mobile (MAUI)</h3>
          <p>AOT-compiled MAUI apps with instant data access, no native SQLite dependency, and optional P2P sync between devices.</p>
        </div>
        <div class="scenario">
          <div class="scenario-icon">🌡️</div>
          <h3>IoT & Edge</h3>
          <p>Lightweight data storage on resource-constrained devices. Spatial indexing for geo-enabled sensors. Mesh sync via EntglDb.</p>
        </div>
        <div class="scenario">
          <div class="scenario-icon">⚙️</div>
          <h3>CLI & Background Services</h3>
          <p>Trimmed, single-file executables with embedded persistence. Zero startup overhead from reflection-free design.</p>
        </div>
        <div class="scenario">
          <div class="scenario-icon">🖥️</div>
          <h3>Desktop Apps</h3>
          <p>WPF, WinUI, Avalonia — any .NET desktop framework. Local-first storage with CDC for reactive UI updates.</p>
        </div>
        <div class="scenario">
          <div class="scenario-icon">🧪</div>
          <h3>Testing & Prototyping</h3>
          <p>In-memory mode for fast unit tests. No external process, no cleanup. Perfect for integration testing.</p>
        </div>
        <div class="scenario">
          <div class="scenario-icon">🏗️</div>
          <h3>Microservices</h3>
          <p>Local caching or event sourcing with ACID guarantees and zero infrastructure. EntglDb adds state replication across instances.</p>
        </div>
      </div>
    </section>

    <section>
      <h2>When to Use What</h2>
      <div class="decision-grid">
        <div class="decision-card">
          <h3>Choose BLite when…</h3>
          <ul>
            <li>You need NativeAOT or trimming.</li>
            <li>You want zero dependencies and pure managed code.</li>
            <li>You prefer document-oriented data over relational tables.</li>
            <li>You want compile-time mapping safety.</li>
            <li>You need spatial indexing or CDC built-in.</li>
            <li>You want a fully async API.</li>
          </ul>
        </div>
        <div class="decision-card">
          <h3>Choose SQLite + EF Core when…</h3>
          <ul>
            <li>You need complex relational queries (joins, aggregates).</li>
            <li>You have an existing SQL schema you must reuse.</li>
            <li>You need full-text search.</li>
          </ul>
        </div>
        <div class="decision-card">
          <h3>Choose LiteDB when…</h3>
          <ul>
            <li>You don't need AOT and reflection is acceptable.</li>
            <li>You need BsonExpression-based LINQ queries.</li>
            <li>You are on .NET Framework and can't upgrade.</li>
          </ul>
        </div>
      </div>
    </section>

    <div class="info-box">
      <div class="info-header">📦 Get Started</div>
      <p>Ready to try BLite? Install it from NuGet:</p>
      <pre><code>dotnet add package BLite
dotnet add package BLite.SourceGenerators</code></pre>
      <p>Pair it with <a href="https://www.entgldb.com" target="_blank">EntglDb</a> for distributed scenarios:</p>
      <pre><code>dotnet add package EntglDb.Persistence.BLite</code></pre>
      <p>Check the <router-link to="/docs/getting-started">Getting Started</router-link> guide for a full walkthrough.</p>
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
.info-box a, .info-box .router-link-active { color: var(--blite-red); text-decoration: underline; }

.table-scroll { overflow-x: auto; margin: 24px 0; }
table { width: 100%; border-collapse: collapse; min-width: 700px; }
th, td { padding: 12px; text-align: left; border-bottom: 1px solid rgba(231, 76, 60, 0.2); }
th { color: var(--blite-red); font-weight: 600; font-size: 0.9rem; }
td { color: var(--text-secondary); font-size: 0.9rem; }
.highlight-col { background: rgba(231, 76, 60, 0.05); }

.badge-good { display: inline-block; padding: 4px 8px; background: rgba(34, 197, 94, 0.2); color: #22c55e; border-radius: 4px; font-size: 0.8rem; font-weight: 600; }
.badge-bad { display: inline-block; padding: 4px 8px; background: rgba(239, 68, 68, 0.15); color: #ef4444; border-radius: 4px; font-size: 0.8rem; font-weight: 600; }
.badge-mid { display: inline-block; padding: 4px 8px; background: rgba(251, 146, 60, 0.2); color: #fb923c; border-radius: 4px; font-size: 0.8rem; font-weight: 600; }

.highlights { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 16px; margin: 24px 0; }
.highlight { background: rgba(231, 76, 60, 0.1); border: 1px solid rgba(231, 76, 60, 0.3); border-radius: 8px; padding: 20px; text-align: center; }
.highlight-value { font-size: 2rem; font-weight: 800; font-family: var(--font-mono); color: var(--blite-red); }
.highlight-label { font-size: 0.85rem; color: var(--text-muted); margin-top: 8px; text-transform: uppercase; letter-spacing: 1px; }

.architecture-diagram { background: rgba(10, 10, 10, 0.6); border: 1px solid rgba(231, 76, 60, 0.2); border-radius: 12px; padding: 32px; margin: 24px 0; text-align: center; }
.arch-row { display: flex; align-items: center; justify-content: center; gap: 16px; flex-wrap: wrap; }
.arch-node { background: rgba(231, 76, 60, 0.1); border: 1px solid rgba(231, 76, 60, 0.3); border-radius: 8px; padding: 16px 24px; font-size: 1.1rem; font-weight: 600; display: flex; flex-direction: column; align-items: center; gap: 4px; }
.arch-node.app { color: var(--text-primary); }
.node-sub { font-size: 0.75rem; color: var(--text-muted); font-weight: 400; }
.arch-connector { font-size: 1.5rem; color: var(--blite-red); font-weight: 700; }
.arch-label { margin-top: 20px; font-size: 0.9rem; color: var(--text-muted); font-style: italic; }

.scenarios { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin: 24px 0; }
.scenario { background: rgba(15, 15, 15, 0.5); border: 1px solid rgba(231, 76, 60, 0.15); border-radius: 8px; padding: 24px; transition: border-color 0.2s ease; }
.scenario:hover { border-color: rgba(231, 76, 60, 0.4); }
.scenario-icon { font-size: 2rem; margin-bottom: 12px; }
.scenario h3 { margin-top: 0; color: var(--text-primary); }
.scenario p { font-size: 0.9rem; margin-bottom: 0; }

.decision-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin: 24px 0; }
.decision-card { background: rgba(15, 15, 15, 0.5); border: 1px solid rgba(231, 76, 60, 0.15); border-radius: 8px; padding: 24px; }
.decision-card:first-child { border-color: rgba(231, 76, 60, 0.4); background: rgba(231, 76, 60, 0.05); }
.decision-card h3 { margin-top: 0; color: var(--blite-red); font-size: 1.1rem; }
.decision-card ul { margin-bottom: 0; }
.decision-card li { font-size: 0.9rem; }
</style>
