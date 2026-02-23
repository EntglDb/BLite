<template>
  <div class="doc-page">
    <header class="doc-header">
      <h1>🔄 <span class="title-gradient">Converters</span></h1>
      <p class="lead">BLite ships two complementary conversion systems: <strong>Custom ID Converters</strong> for mapping ValueObjects to BSON storage types, and <strong>BsonJsonConverter</strong> for translating between JSON and native BSON — with zero extra dependencies.</p>
    </header>

    <section class="doc-section">
      <h2>Overview</h2>
      <p>
        BLite allows you to use strongly-typed identifiers (ValueObjects) for your entities while storing them as simple primitives (int, string, Guid, ObjectId) in the underlying BSON storage.
      </p>
      <div class="info-box">
        <span class="icon">ℹ️</span>
        <p>This feature leverages Source Generators to automatically handle conversion during serialization and deserialization, ensuring zero runtime overhead.</p>
      </div>
    </section>

    <section class="doc-section">
      <h2>1. Define your ValueObject</h2>
      <p>Usually, a <code>record</code> is the best choice for a ValueObject as it provides built-in equality and concise syntax.</p>
      <pre><code>public record OrderId(string Value);</code></pre>
    </section>

    <section class="doc-section">
      <h2>2. Implement ValueConverter</h2>
      <p>Create a converter by inheriting from <code>ValueConverter&lt;TModel, TProvider&gt;</code>.</p>
      <pre><code>public class OrderIdConverter : ValueConverter&lt;OrderId, string&gt;
{
    public override string ConvertToProvider(OrderId model) => model.Value;
    public override OrderId ConvertFromProvider(string provider) => new OrderId(provider);
}</code></pre>
    </section>

    <section class="doc-section">
      <h2>3. Register the Converter</h2>
      <p>Use the Fluent API in your <code>OnModelCreating</code> method to associate the converter with the entity property.</p>
      <pre><code>protected override void OnModelCreating(ModelBuilder modelBuilder)
{
    modelBuilder.Entity&lt;Order&gt;()
        .Property(x => x.Id)
        .HasConversion&lt;OrderIdConverter&gt;();
}</code></pre>
    </section>

    <section class="doc-section">
      <h2>How it Works</h2>
      <p>
        The BLite Source Generator detects the <code>HasConversion</code> call and generates a specialized mapper that:
      </p>
      <ul>
        <li>Instantiates the <code>OrderIdConverter</code> exactly once.</li>
        <li>Converts the <code>OrderId</code> to <code>string</code> before writing to BSON.</li>
        <li>Converts the <code>string</code> back to <code>OrderId</code> when reading from BSON.</li>
        <li>Optimizes index keys so you can query using the ValueObject directly.</li>
      </ul>
    </section>

    <section class="doc-section">
      <h2>Usage Example</h2>
      <pre><code>var myOrderId = new OrderId("ORD-2024-001");

// Insert works naturally
db.Orders.Insert(new Order { Id = myOrderId, Customer = "Alice" });

// FindById uses the ValueObject type
var order = db.Orders.FindById(myOrderId);</code></pre>
    </section>

    <section class="doc-section">
      <h2>BsonJsonConverter <span style="font-size:0.6em;vertical-align:middle;background:rgba(231,76,60,0.15);color:var(--blite-red);padding:2px 8px;border-radius:12px;font-weight:600">v1.7.0</span></h2>
      <p>
        <code>BsonJsonConverter</code> is a static utility in <code>BLite.Bson</code> that converts between UTF-8 JSON and BLite's native BSON representation.
        It uses <strong><code>System.Text.Json</code></strong> — built into the .NET runtime — so there are zero extra NuGet dependencies.
      </p>
      <p>
        It is especially useful in <strong>schema-less / server mode</strong> (<code>BLiteEngine</code> + <code>DynamicCollection</code>) when you need to ingest JSON payloads or export documents as JSON.
      </p>

      <h3>JSON → BsonDocument</h3>
      <pre><code>using BLite.Bson;

using var engine = new BLiteEngine("data.db");
var col = engine.GetOrCreateCollection("events");

// Get the engine's shared field-name dictionaries
var (keyMap, reverseKeyMap) = engine.GetKeyMap();

string json = """
    {
        "_id": "507f1f77bcf86cd799439011",
        "type": "order_placed",
        "total": 199.99,
        "placedAt": "2026-02-23T10:00:00Z"
    }
    """;

BsonDocument doc = BsonJsonConverter.FromJson(json, keyMap, reverseKeyMap);
col.Insert(doc);</code></pre>

      <h3>BsonDocument → JSON</h3>
      <pre><code>BsonDocument? doc = col.FindById(id);

if (doc is not null)
{
    // Indented output (default)
    string json = BsonJsonConverter.ToJson(doc);

    // Compact (no indentation)
    string compact = BsonJsonConverter.ToJson(doc, indented: false);

    Console.WriteLine(json);
    // {
    //   "_id": "507f1f77bcf86cd799439011",
    //   "type": "order_placed",
    //   "total": 199.99,
    //   "placedAt": "2026-02-23T10:00:00Z"
    // }
}</code></pre>

      <h3>Type Mapping</h3>
      <table>
        <thead>
          <tr><th>JSON</th><th>BSON</th><th>Notes</th></tr>
        </thead>
        <tbody>
          <tr><td><code>null</code></td><td><code>BsonValue.Null</code></td><td></td></tr>
          <tr><td><code>true</code> / <code>false</code></td><td><code>BsonValue.Boolean</code></td><td></td></tr>
          <tr><td>number (fits int32)</td><td><code>BsonValue.Int32</code></td><td></td></tr>
          <tr><td>number (fits int64)</td><td><code>BsonValue.Int64</code></td><td></td></tr>
          <tr><td>number (fractional)</td><td><code>BsonValue.Double</code></td><td></td></tr>
          <tr><td>string (ISO-8601)</td><td><code>BsonValue.DateTime</code></td><td>Auto-detected, stored as UTC</td></tr>
          <tr><td>string (UUID)</td><td><code>BsonValue.Guid</code></td><td>Standard <code>D</code> format</td></tr>
          <tr><td>string</td><td><code>BsonValue.String</code></td><td></td></tr>
          <tr><td>object <code>{…}</code></td><td>nested <code>BsonDocument</code></td><td>Shares engine key map</td></tr>
          <tr><td>array <code>[…]</code></td><td><code>BsonValue.Array</code></td><td></td></tr>
          <tr><td><code>"_id"</code> field</td><td><code>BsonId</code></td><td>24-hex → ObjectId, int, long, Guid, or string fallback</td></tr>
        </tbody>
      </table>

      <h3>Reverse Mapping (BSON → JSON)</h3>
      <table>
        <thead>
          <tr><th>BSON</th><th>JSON</th></tr>
        </thead>
        <tbody>
          <tr><td><code>Null</code></td><td><code>null</code></td></tr>
          <tr><td><code>Boolean</code></td><td><code>true</code> / <code>false</code></td></tr>
          <tr><td><code>Int32</code> / <code>Int64</code> / <code>Double</code> / <code>Decimal</code></td><td>number</td></tr>
          <tr><td><code>String</code></td><td>string</td></tr>
          <tr><td><code>DateTime</code></td><td>ISO-8601 string (<code>"2026-02-23T10:00:00Z"</code>)</td></tr>
          <tr><td><code>Guid</code></td><td>UUID string (<code>"xxxxxxxx-xxxx-..."</code>)</td></tr>
          <tr><td><code>ObjectId</code></td><td>24-char lowercase hex string</td></tr>
          <tr><td><code>Timestamp</code></td><td>number (Unix seconds)</td></tr>
          <tr><td><code>Binary</code></td><td>base64 string</td></tr>
          <tr><td><code>Coordinates</code></td><td><code>[lon, lat]</code> (GeoJSON order)</td></tr>
          <tr><td><code>Array</code></td><td>JSON array</td></tr>
          <tr><td><code>Document</code></td><td>JSON object</td></tr>
        </tbody>
      </table>

      <div class="info-box">
        <span class="icon">💡</span>
        <p>Both overloads of <code>FromJson</code> accept a <code>string</code> or a <code>ReadOnlyMemory&lt;byte&gt;</code> UTF-8 buffer. The byte-buffer overload avoids an extra string allocation when reading directly from a network stream.</p>
      </div>
    </section>
  </div>
</template>

<style scoped>
.doc-page {
  animation: fadeIn 0.5s ease-out;
}

@keyframes fadeIn {
  from { opacity: 0; transform: translateY(10px); }
  to { opacity: 1; transform: translateY(0); }
}

.doc-header {
  margin-bottom: 48px;
}

h1 {
  font-size: 3rem;
  font-weight: 800;
  margin-bottom: 16px;
}

.lead {
  font-size: 1.25rem;
  color: var(--text-secondary);
}

.doc-section {
  margin-bottom: 48px;
}

h2 {
  font-size: 1.75rem;
  font-weight: 700;
  margin-bottom: 24px;
  color: #fff;
}

p {
  color: var(--text-secondary);
  line-height: 1.7;
  margin-bottom: 16px;
}

code {
  background: rgba(231, 76, 60, 0.1);
  color: var(--blite-red);
  padding: 2px 6px;
  border-radius: 4px;
  font-family: 'JetBrains Mono', monospace;
  font-size: 0.9em;
}

pre {
  background: #1a1a1a;
  padding: 24px;
  border-radius: 12px;
  border: 1px solid rgba(255, 255, 255, 0.05);
  overflow-x: auto;
  margin-bottom: 24px;
}

pre code {
  background: none;
  color: #e0e0e0;
  padding: 0;
  font-size: 0.95rem;
}

.info-box {
  display: flex;
  gap: 16px;
  background: rgba(52, 152, 219, 0.1);
  border-left: 4px solid #3498db;
  padding: 20px;
  border-radius: 0 8px 8px 0;
  margin: 24px 0;
}

.info-box .icon {
  font-size: 1.25rem;
}

.info-box p {
  margin: 0;
  color: #e0e0e0;
}

ul {
  padding-left: 24px;
  margin-bottom: 24px;
}

li {
  color: var(--text-secondary);
  margin-bottom: 12px;
  line-height: 1.6;
}

table {
  width: 100%;
  border-collapse: collapse;
  margin: 16px 0 24px;
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

h3 {
  font-size: 1.2rem;
  font-weight: 600;
  margin: 28px 0 12px;
  color: var(--text-primary);
}
</style>
