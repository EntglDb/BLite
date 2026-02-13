<template>
  <div class="doc-page">
    <header class="doc-header">
      <h1>🆔 <span class="title-gradient">Custom ID Converters</span></h1>
      <p class="lead">Use ValueObjects and custom types as primary keys with native conversion support.</p>
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
</style>
