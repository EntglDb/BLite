<template>
  <div class="doc-page">
    <h1>📦 <span class="title-gradient">Installation</span></h1>
    <p class="lead">Multiple ways to get BLite into your .NET project.</p>

    <section>
      <h2>NuGet Package (Recommended)</h2>
      <p>Install the all-in-one package using the .NET CLI:</p>
      <pre><code>dotnet add package BLite</code></pre>
      
      <p>Or using Package Manager Console in Visual Studio:</p>
      <pre><code>Install-Package BLite</code></pre>

      <p>Or add directly to your <code>.csproj</code>:</p>
      <pre><code>&lt;<span class="keyword">ItemGroup</span>&gt;
  &lt;<span class="keyword">PackageReference</span> <span class="type">Include</span>=<span class="string">"BLite"</span> <span class="type">Version</span>=<span class="string">"1.4.0"</span> /&gt;
&lt;/<span class="keyword">ItemGroup</span>&gt;</code></pre>
    </section>

    <section>
      <h2>What's Included</h2>
      <p>The <code>BLite</code> metapackage installs all necessary components:</p>
      <ul>
        <li><strong>BLite.Core</strong> - Storage engine, transactions, indexing</li>
        <li><strong>BLite.Bson</strong> - Zero-allocation BSON serializer</li>
        <li><strong>BLite.SourceGenerators</strong> - Compile-time code generation</li>
      </ul>
      <div class="info-box">
        <strong>✨ Automatic Setup:</strong> Source generators are automatically activated when you add the package. No additional configuration needed!
      </div>
    </section>

    <section>
      <h2>Build from Source</h2>
      <p>Clone the repository and build locally:</p>
      <pre><code>git clone https://github.com/EntglDb/BLite.git
cd BLite
dotnet build -c Release</code></pre>

      <p>Then reference the projects directly in your solution:</p>
      <pre><code>&lt;<span class="keyword">ItemGroup</span>&gt;
  &lt;<span class="keyword">ProjectReference</span> <span class="type">Include</span>=<span class="string">"..\BLite\src\BLite.Core\BLite.Core.csproj"</span> /&gt;
  &lt;<span class="keyword">ProjectReference</span> <span class="type">Include</span>=<span class="string">"..\BLite\src\BLite.Bson\BLite.Bson.csproj"</span> /&gt;
&lt;/<span class="keyword">ItemGroup</span>&gt;</code></pre>
    </section>

    <section>
      <h2>Requirements</h2>
      <ul>
        <li><strong>.NET 10 SDK</strong> or later</li>
        <li><strong>Windows, Linux, or macOS</strong></li>
        <li><strong>C# 13</strong> language features</li>
      </ul>

      <div class="warning-box">
        <strong>⚠️ Note:</strong> BLite currently targets .NET 10 to leverage modern performance features like <code>Span&lt;T&gt;</code> optimizations. Support for .NET Standard 2.1 is under consideration.
      </div>
    </section>

    <section>
      <h2>Next Steps</h2>
      <router-link to="/docs/getting-started" class="cta-button">
        Get Started →
      </router-link>
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
}

li strong {
  color: var(--blite-red);
}

code {
  font-family: var(--font-mono);
  font-size: 0.9rem;
  background: rgba(231, 76, 60, 0.1);
  padding: 2px 6px;
  border-radius: 4px;
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

pre code {
  background: none;
  padding: 0;
  color: var(--text-secondary);
}

.keyword { color: var(--blite-red); }
.type { color: #06b6d4; }
.string { color: #a1a1aa; }

.info-box, .warning-box {
  padding: 16px 20px;
  border-radius: 8px;
  margin: 24px 0;
  border-left: 4px solid;
}

.info-box {
  background: rgba(6, 182, 212, 0.05);
  border-color: #06b6d4;
}

.warning-box {
  background: rgba(231, 76, 60, 0.05);
  border-color: var(--blite-red);
}

.cta-button {
  display: inline-block;
  padding: 12px 32px;
  background: var(--blite-red);
  color: white;
  font-weight: 600;
  border-radius: 8px;
  transition: all 0.2s ease;
}

.cta-button:hover {
  background: var(--blite-red-glow);
  transform: translateY(-1px);
  box-shadow: 0 4px 12px rgba(231, 76, 60, 0.3);
}
</style>
