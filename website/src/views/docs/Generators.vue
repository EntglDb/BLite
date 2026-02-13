<template>
  <div class="doc-page">
    <h1>🤖 Source Generators</h1>
    <p class="lead">Zero-allocation, compile-time BSON mappers with no reflection overhead.</p>

    <section>
      <h2>How It Works</h2>
      <p>BLite uses Roslyn Source Generators to create optimized serialization code at compile-time. This eliminates reflection overhead and enables zero-allocation I/O.</p>
      
      <div class="info-box">
        <strong>✨ Automatic:</strong> Generators activate automatically when you add the BLite package. No configuration needed!
      </div>
    </section>

    <section>
      <h2>Supported Attributes</h2>
      
      <h3>Mapping Attributes</h3>
      <pre><code><span class="keyword">using</span> System.ComponentModel.DataAnnotations;
<span class="keyword">using</span> System.ComponentModel.DataAnnotations.Schema;
<span class="keyword">using</span> System.Text.Json.Serialization;

[<span class="type">Table</span>(<span class="string">"users"</span>)]  <span class="comment">// Collection name</span>
<span class="keyword">public class</span> <span class="type">User</span>
{
    [<span class="type">Key</span>]  <span class="comment">// Primary key</span>
    <span class="keyword">public</span> <span class="type">ObjectId</span> Id { <span class="keyword">get</span>; <span class="keyword">set</span>; }
    
    [<span class="type">Column</span>(<span class="string">"full_name"</span>)]  <span class="comment">// Custom BSON field name</span>
    <span class="keyword">public string</span> Name { <span class="keyword">get</span>; <span class="keyword">set</span>; }
    
    [<span class="type">JsonPropertyName</span>(<span class="string">"email_address"</span>)]  <span class="comment">// Also supported</span>
    <span class="keyword">public string</span> Email { <span class="keyword">get</span>; <span class="keyword">set</span>; }
    
    [<span class="type">JsonIgnore</span>]  <span class="comment">// Exclude from BSON</span>
    <span class="keyword">public string</span> TemporaryData { <span class="keyword">get</span>; <span class="keyword">set</span>; }
}</code></pre>

      <h3>Validation Attributes</h3>
      <pre><code><span class="keyword">public class</span> <span class="type">Product</span>
{
    [<span class="type">Required</span>]
    [<span class="type">StringLength</span>(<span class="number">200</span>, MinimumLength = <span class="number">3</span>)]
    <span class="keyword">public string</span> Name { <span class="keyword">get</span>; <span class="keyword">set</span>; }
    
    [<span class="type">Range</span>(<span class="number">0.01</span>, <span class="number">999999.99</span>)]
    <span class="keyword">public decimal</span> Price { <span class="keyword">get</span>; <span class="keyword">set</span>; }
    
    [<span class="type">EmailAddress</span>]
    <span class="keyword">public string</span> ContactEmail { <span class="keyword">get</span>; <span class="keyword">set</span>; }
    
    [<span class="type">Url</span>]
    <span class="keyword">public string</span> Website { <span class="keyword">get</span>; <span class="keyword">set</span>; }
}</code></pre>
    </section>

    <section>
      <h2>Generated Code</h2>
      <p>For each model, BLite generates a mapper class:</p>
      <pre><code><span class="comment">// Generated automatically</span>
<span class="keyword">internal class</span> <span class="type">UserMapper</span> : <span class="type">IBsonMapper</span>&lt;<span class="type">User</span>&gt;
{
    <span class="keyword">public void</span> Serialize(<span class="type">User</span> value, <span class="type">Span</span>&lt;<span class="keyword">byte</span>&gt; buffer)
    {
        <span class="comment">// Zero-allocation serialization</span>
    }
    
    <span class="keyword">public</span> <span class="type">User</span> Deserialize(<span class="type">ReadOnlySpan</span>&lt;<span class="keyword">byte</span>&gt; buffer)
    {
        <span class="comment">// Zero-allocation deserialization</span>
    }
}</code></pre>
    </section>

    <section>
      <h2>Supported Types</h2>
      <ul>
        <li><strong>Primitives:</strong> <code>int</code>, <code>long</code>, <code>double</code>, <code>decimal</code>, <code>bool</code>, <code>string</code></li>
        <li><strong>DateTime:</strong> <code>DateTime</code>, <code>DateTimeOffset</code></li>
        <li><strong>Collections:</strong> <code>List&lt;T&gt;</code>, <code>T[]</code>, <code>Dictionary&lt;string, T&gt;</code></li>
        <li><strong>BLite Types:</strong> <code>ObjectId</code>, <code>BsonDocument</code></li>
        <li><strong>Nested Objects:</strong> Automatic recursive mapping</li>
        <li><strong>Enums:</strong> Serialized as integers or strings</li>
      </ul>
    </section>

    <section>
      <h2>Custom BSON Field Names</h2>
      <p>BLite follows a lowercase convention by default. Override with attributes:</p>
      <pre><code><span class="keyword">public class</span> <span class="type">Person</span>
{
    <span class="comment">// BSON field: "firstname" (lowercase)</span>
    <span class="keyword">public string</span> FirstName { <span class="keyword">get</span>; <span class="keyword">set</span>; }
    
    <span class="comment">// BSON field: "family_name" (custom)</span>
    [<span class="type">Column</span>(<span class="string">"family_name"</span>)]
    <span class="keyword">public string</span> LastName { <span class="keyword">get</span>; <span class="keyword">set</span>; }
}</code></pre>
    </section>

    <section>
      <h2>Performance Benefits</h2>
      <ul>
        <li>✅ <strong>Zero reflection</strong> - All mapping is compile-time</li>
        <li>✅ <strong>Zero allocation</strong> - Direct <code>Span&lt;byte&gt;</code> operations</li>
        <li>✅ <strong>Type safety</strong> - Compile errors instead of runtime failures</li>
        <li>✅ <strong>Automatic validation</strong> - DataAnnotations checked on insert/update</li>
      </ul>
    </section>

    <section>
      <h2>Nested Objects</h2>
      <pre><code><span class="keyword">public class</span> <span class="type">Address</span>
{
    <span class="keyword">public string</span> Street { <span class="keyword">get</span>; <span class="keyword">set</span>; }
    <span class="keyword">public string</span> City { <span class="keyword">get</span>; <span class="keyword">set</span>; }
}

<span class="keyword">public class</span> <span class="type">Customer</span>
{
    [<span class="type">Key</span>]
    <span class="keyword">public</span> <span class="type">ObjectId</span> Id { <span class="keyword">get</span>; <span class="keyword">set</span>; }
    
    <span class="keyword">public string</span> Name { <span class="keyword">get</span>; <span class="keyword">set</span>; }
    
    <span class="comment">// Automatically mapped</span>
    <span class="keyword">public</span> <span class="type">Address</span> ShippingAddress { <span class="keyword">get</span>; <span class="keyword">set</span>; }
}</code></pre>
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
.keyword { color: var(--blite-red); }
.type { color: #06b6d4; }
.string { color: #a1a1aa; }
.number { color: #06b6d4; }
.comment { color: #52525b; font-style: italic; }
.info-box { padding: 16px 20px; border-radius: 8px; margin: 24px 0; border-left: 4px solid #06b6d4; background: rgba(6, 182, 212, 0.05); }
</style>
