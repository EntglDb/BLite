<template>
  <div class="doc-page">
    <h1>🤖 <span class="title-gradient">Source Generators</span></h1>
    <p class="lead">Zero-allocation, compile-time BSON mappers with no reflection overhead. Robust handling of nested objects, collections, and ref structs.</p>

    <section>
      <h2>How It Works</h2>
      <p>BLite uses Roslyn Source Generators to create optimized serialization code at compile-time. This eliminates reflection overhead and enables zero-allocation I/O with correct handling of complex type hierarchies.</p>
      
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
      <h2>Nested Objects & Collections</h2>
      <p>BLite generators correctly handle deeply nested object graphs, collections, and ref structs:</p>
      <pre><code><span class="keyword">public class</span> <span class="type">Address</span>
{
    <span class="keyword">public string</span> Street { <span class="keyword">get</span>; <span class="keyword">set</span>; }
    <span class="keyword">public string</span> City { <span class="keyword">get</span>; <span class="keyword">set</span>; }
    <span class="keyword">public</span> (<span class="keyword">double</span> Lat, <span class="keyword">double</span> Lng) Coordinates { <span class="keyword">get</span>; <span class="keyword">set</span>; }
}

<span class="keyword">public class</span> <span class="type">Order</span>
{
    <span class="keyword">public</span> <span class="type">ObjectId</span> Id { <span class="keyword">get</span>; <span class="keyword">set</span>; }
    <span class="keyword">public</span> <span class="type">List</span>&lt;<span class="type">string</span>&gt; Tags { <span class="keyword">get</span>; <span class="keyword">set</span>; }
}

<span class="keyword">public class</span> <span class="type">Customer</span>
{
    [<span class="type">Key</span>]
    <span class="keyword">public</span> <span class="type">ObjectId</span> Id { <span class="keyword">get</span>; <span class="keyword">set</span>; }
    
    <span class="keyword">public string</span> Name { <span class="keyword">get</span>; <span class="keyword">set</span>; }
    
    <span class="comment">// Nested object - automatically mapped</span>
    <span class="keyword">public</span> <span class="type">Address</span> ShippingAddress { <span class="keyword">get</span>; <span class="keyword">set</span>; }
    
    <span class="comment">// Collection of nested objects - fully supported</span>
    <span class="keyword">public</span> <span class="type">List</span>&lt;<span class="type">Order</span>&gt; Orders { <span class="keyword">get</span>; <span class="keyword">set</span>; }
    
    <span class="comment">// Nested collections</span>
    <span class="keyword">public</span> <span class="type">Dictionary</span>&lt;<span class="keyword">string</span>, <span class="type">List</span>&lt;<span class="keyword">int</span>&gt;&gt; Metadata { <span class="keyword">get</span>; <span class="keyword">set</span>; }
}</code></pre>

      <div class="info-box success">
        <strong>✨ Recent Improvements (v1.2.1):</strong>
        <ul>
          <li>✅ Property inheritance from base classes</li>
          <li>✅ Private and init-only setters via Expression Trees</li>
          <li>✅ Advanced collection types (IEnumerable&lt;T&gt;, ICollection&lt;T&gt;, HashSet&lt;T&gt;)</li>
          <li>✅ Nullable value types and collections</li>
          <li>✅ Circular reference protection for self-referencing entities</li>
          <li>✅ N-N relationship patterns with ObjectId collections</li>
        </ul>
      </div>
    </section>

    <section>
      <h2>✅ Supported Scenarios</h2>
      <p>The source generator handles a wide range of modern C# patterns, including advanced property configurations and complex relationships:</p>
      
      <table class="feature-table">
        <thead>
          <tr>
            <th>Feature</th>
            <th>Support</th>
            <th>Description</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td><strong>Property Inheritance</strong></td>
            <td><span class="badge success">✅</span></td>
            <td>Properties from base classes are automatically included in serialization</td>
          </tr>
          <tr>
            <td><strong>Private Setters</strong></td>
            <td><span class="badge success">✅</span></td>
            <td>Properties with <code>private set</code> are correctly deserialized using Expression Trees</td>
          </tr>
          <tr>
            <td><strong>Init-Only Setters</strong></td>
            <td><span class="badge success">✅</span></td>
            <td>Properties with <code>init</code> are supported via runtime compilation</td>
          </tr>
          <tr>
            <td><strong>Private Constructors</strong></td>
            <td><span class="badge success">✅</span></td>
            <td>Deserialization works even without parameterless public constructor</td>
          </tr>
          <tr>
            <td><strong>Advanced Collections</strong></td>
            <td><span class="badge success">✅</span></td>
            <td><code>IEnumerable&lt;T&gt;</code>, <code>ICollection&lt;T&gt;</code>, <code>IList&lt;T&gt;</code>, <code>HashSet&lt;T&gt;</code>, and more</td>
          </tr>
          <tr>
            <td><strong>Nullable Value Types</strong></td>
            <td><span class="badge success">✅</span></td>
            <td><code>ObjectId?</code>, <code>int?</code>, <code>DateTime?</code> are correctly serialized/deserialized</td>
          </tr>
          <tr>
            <td><strong>Nullable Collections</strong></td>
            <td><span class="badge success">✅</span></td>
            <td><code>List&lt;T&gt;?</code>, <code>string?</code> with proper null handling</td>
          </tr>
          <tr>
            <td><strong>Unlimited Nesting</strong></td>
            <td><span class="badge success">✅</span></td>
            <td>Deeply nested object graphs with circular reference protection</td>
          </tr>
          <tr>
            <td><strong>Self-Referencing</strong></td>
            <td><span class="badge success">✅</span></td>
            <td>Entities can reference themselves (e.g., <code>Manager</code> property in <code>Employee</code>)</td>
          </tr>
          <tr>
            <td><strong>N-N Relationships</strong></td>
            <td><span class="badge success">✅</span></td>
            <td>Collections of ObjectIds for efficient document referencing</td>
          </tr>
        </tbody>
      </table>

      <div class="code-example">
        <h4>Example: Advanced Property Patterns</h4>
        <pre><code><span class="keyword">public class</span> <span class="type">Person</span>
{
    <span class="comment">// Property inheritance - inherited properties are included</span>
    <span class="keyword">public</span> <span class="type">ObjectId</span> Id { <span class="keyword">get</span>; <span class="keyword">set</span>; }
    
    <span class="comment">// Private setter - Expression Trees used for deserialization</span>
    <span class="keyword">public string</span> Name { <span class="keyword">get</span>; <span class="keyword">private set</span>; }
    
    <span class="comment">// Init-only setter - Runtime compilation for setting</span>
    <span class="keyword">public string</span> Email { <span class="keyword">get</span>; <span class="keyword">init</span>; }
    
    <span class="comment">// Nullable value type - Proper null handling</span>
    <span class="keyword">public</span> <span class="type">DateTime</span>? BirthDate { <span class="keyword">get</span>; <span class="keyword">set</span>; }
    
    <span class="comment">// Advanced collection - ICollection&lt;T&gt; recognized</span>
    <span class="keyword">public</span> <span class="type">ICollection</span>&lt;<span class="keyword">string</span>&gt; Tags { <span class="keyword">get</span>; <span class="keyword">set</span>; }
    
    <span class="comment">// Self-referencing - Circular reference protection</span>
    <span class="keyword">public</span> <span class="type">ObjectId</span>? ManagerId { <span class="keyword">get</span>; <span class="keyword">set</span>; }
}</code></pre>
      </div>
    </section>

    <section>
      <h2>⚠️ Limitations & Design Choices</h2>
      <p>Some scenarios are handled differently or intentionally excluded:</p>
      
      <table class="feature-table">
        <thead>
          <tr>
            <th>Scenario</th>
            <th>Status</th>
            <th>Reason</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td><strong>Computed Properties</strong></td>
            <td><span class="badge warning">⚠️ Excluded</span></td>
            <td>Getter-only properties without backing fields are intentionally skipped (e.g., <code>FullName => $"{First} {Last}"</code>)</td>
          </tr>
          <tr>
            <td><strong>Constructor Logic</strong></td>
            <td><span class="badge warning">⚠️ Bypassed</span></td>
            <td>Deserialization uses <code>FormatterServices.GetUninitializedObject()</code> to avoid constructor execution</td>
          </tr>
          <tr>
            <td><strong>Constructor Validation</strong></td>
            <td><span class="badge warning">⚠️ Not Executed</span></td>
            <td>Validation logic in constructors won't run during deserialization - use Data Annotations instead</td>
          </tr>
        </tbody>
      </table>

      <div class="info-box">
        <strong>💡 Best Practice:</strong> For relationships between entities, prefer <strong>referencing</strong> (storing ObjectIds) over <strong>embedding</strong> (full nested objects) to avoid data duplication and maintain consistency.
      </div>

      <div class="code-example">
        <h4>Example: Referencing vs Embedding</h4>
        <pre><code><span class="comment">// ✅ GOOD: Reference pattern (recommended)</span>
<span class="keyword">public class</span> <span class="type">Category</span>
{
    <span class="keyword">public</span> <span class="type">ObjectId</span> Id { <span class="keyword">get</span>; <span class="keyword">set</span>; }
    <span class="keyword">public string</span> Name { <span class="keyword">get</span>; <span class="keyword">set</span>; }
    <span class="keyword">public</span> <span class="type">List</span>&lt;<span class="type">ObjectId</span>&gt; ProductIds { <span class="keyword">get</span>; <span class="keyword">set</span>; }  <span class="comment">// N-N via IDs</span>
}

<span class="keyword">public class</span> <span class="type">Product</span>
{
    <span class="keyword">public</span> <span class="type">ObjectId</span> Id { <span class="keyword">get</span>; <span class="keyword">set</span>; }
    <span class="keyword">public string</span> Name { <span class="keyword">get</span>; <span class="keyword">set</span>; }
    <span class="keyword">public</span> <span class="type">List</span>&lt;<span class="type">ObjectId</span>&gt; CategoryIds { <span class="keyword">get</span>; <span class="keyword">set</span>; }  <span class="comment">// N-N via IDs</span>
}

<span class="comment">// ⚠️ AVOID: Embedding full objects (data duplication)</span>
<span class="keyword">public class</span> <span class="type">BadCategory</span>
{
    <span class="keyword">public</span> <span class="type">ObjectId</span> Id { <span class="keyword">get</span>; <span class="keyword">set</span>; }
    <span class="keyword">public</span> <span class="type">List</span>&lt;<span class="type">Product</span>&gt; Products { <span class="keyword">get</span>; <span class="keyword">set</span>; }  <span class="comment">// Duplicates product data!</span>
}</code></pre>
      </div>
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
h4 { font-size: 1.1rem; font-weight: 600; margin: 16px 0 8px; color: var(--text-primary); }
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
.info-box.success { border-left-color: #10b981; background: rgba(16, 185, 129, 0.05); }
.info-box ul { margin-top: 12px; }
.info-box li { color: var(--text-secondary); font-size: 0.95rem; }
.feature-table { width: 100%; border-collapse: collapse; margin: 24px 0; }
.feature-table th { background: rgba(231, 76, 60, 0.1); padding: 12px; text-align: left; font-weight: 600; border-bottom: 2px solid rgba(231, 76, 60, 0.3); }
.feature-table td { padding: 12px; border-bottom: 1px solid rgba(231, 76, 60, 0.1); }
.feature-table td code { font-size: 0.85rem; }
.badge { display: inline-block; padding: 4px 8px; border-radius: 4px; font-size: 0.85rem; font-weight: 600; }
.badge.success { background: rgba(16, 185, 129, 0.2); color: #10b981; }
.badge.warning { background: rgba(245, 158, 11, 0.2); color: #f59e0b; }
.code-example { margin: 24px 0; }
.code-example h4 { margin-bottom: 8px; }
</style>
