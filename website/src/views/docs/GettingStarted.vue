<template>
  <div class="doc-page">
    <h1>🚀 <span class="title-gradient">Getting Started with BLite</span></h1>
    <p class="lead">Get up and running with BLite in minutes. This guide will walk you through installation, basic setup, and your first CRUD operations.</p>

    <section>
      <h2>Quick Install</h2>
      <p>Install BLite from NuGet:</p>
      <pre><code>dotnet add package BLite</code></pre>
      <p>This single package includes:</p>
      <ul>
        <li><strong>BLite.Core</strong> - The database engine</li>
        <li><strong>BLite.Bson</strong> - Zero-allocation BSON serializer</li>
        <li><strong>BLite.SourceGenerators</strong> - Compile-time mappers</li>
      </ul>
    </section>

    <section>
      <h2>Create Your First Database</h2>
      <p>Define a model using standard .NET attributes:</p>
      <pre><code><span class="keyword">using</span> System.ComponentModel.DataAnnotations;
<span class="keyword">using</span> System.ComponentModel.DataAnnotations.Schema;

[<span class="type">Table</span>(<span class="string">"users"</span>)]
<span class="keyword">public class</span> <span class="type">User</span>
{
    [<span class="type">Key</span>]
    <span class="keyword">public</span> <span class="type">ObjectId</span> Id { <span class="keyword">get</span>; <span class="keyword">set</span>; }
    
    [<span class="type">Required</span>]
    [<span class="type">StringLength</span>(<span class="number">100</span>)]
    <span class="keyword">public string</span> Name { <span class="keyword">get</span>; <span class="keyword">set</span>; } = <span class="string">""</span>;
    
    <span class="keyword">public int</span> Age { <span class="keyword">get</span>; <span class="keyword">set</span>; }
    
    <span class="keyword">public string</span> Email { <span class="keyword">get</span>; <span class="keyword">set</span>; } = <span class="string">""</span>;
}</code></pre>
    </section>

    <section>
      <h2>Initialize the Database</h2>
      <pre><code><span class="keyword">using</span> BLite.Core;

<span class="comment">// Create database context (uses Source Generator for InitializeCollections)</span>
<span class="keyword">public partial class</span> <span class="type">MyDbContext</span> : <span class="type">DocumentDbContext</span>
{
    <span class="keyword">public</span> <span class="type">DocumentCollection</span>&lt;<span class="type">ObjectId</span>, <span class="type">User</span>&gt; Users { <span class="keyword">get</span>; <span class="keyword">set</span>; } = <span class="keyword">null</span>!;

    <span class="keyword">public</span> MyDbContext(<span class="keyword">string</span> path) : <span class="keyword">base</span>(path)
    {
        InitializeCollections();
    }
}

<span class="comment">// Use the context</span>
<span class="keyword">using var</span> db = <span class="keyword">new</span> <span class="type">MyDbContext</span>(<span class="string">"myapp.blite"</span>);
<span class="keyword">var</span> users = db.Users;</code></pre>
    </section>

    <section>
      <h2>Basic CRUD</h2>
      
      <h3>Insert</h3>
      <pre><code><span class="keyword">var</span> newUser = <span class="keyword">new</span> <span class="type">User</span>
{
    Name = <span class="string">"Alice"</span>,
    Age = <span class="number">30</span>,
    Email = <span class="string">"alice@example.com"</span>
};

users.Insert(newUser);
Console.WriteLine($<span class="string">"Inserted with ID: {newUser.Id}"</span>);</code></pre>

      <h3>Query</h3>
      <pre><code><span class="comment">// Find by ID</span>
<span class="keyword">var</span> user = users.FindById(newUser.Id);

<span class="comment">// Query with LINQ</span>
<span class="keyword">var</span> results = users.AsQueryable()
    .Where(u => u.Age > <span class="number">25</span>)
    .OrderBy(u => u.Name)
    .AsEnumerable();</code></pre>

      <h3>Update</h3>
      <pre><code>user.Age = <span class="number">31</span>;
users.Update(user);</code></pre>

      <h3>Delete</h3>
      <pre><code>users.Delete(user.Id);</code></pre>
    </section>

    <section>
      <h2>Next Steps</h2>
      <div class="next-links">
        <router-link to="/docs/crud" class="next-card">
          <span class="next-icon">✏️</span>
          <div>
            <h4>CRUD Operations</h4>
            <p>Learn advanced insert, update, and delete patterns</p>
          </div>
        </router-link>
        <router-link to="/docs/querying" class="next-card">
          <span class="next-icon">🔍</span>
          <div>
            <h4>Querying</h4>
            <p>Master the powerful LINQ-based query engine</p>
          </div>
        </router-link>
      </div>
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

ul {
  margin: 16px 0;
  padding-left: 24px;
}

li {
  margin-bottom: 8px;
  color: var(--text-secondary);
  line-height: 1.6;
}

li strong {
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

code {
  font-family: var(--font-mono);
  font-size: 0.9rem;
  line-height: 1.6;
  color: var(--text-secondary);
}

.keyword { color: var(--blite-red); }
.type { color: #06b6d4; }
.string { color: #a1a1aa; }
.number { color: #06b6d4; }
.comment { color: #52525b; font-style: italic; }

.next-links {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
  gap: 16px;
  margin-top: 32px;
}

.next-card {
  display: flex;
  gap: 16px;
  padding: 20px;
  background: rgba(231, 76, 60, 0.05);
  border: 1px solid rgba(231, 76, 60, 0.2);
  border-radius: 8px;
  transition: all 0.2s ease;
}

.next-card:hover {
  background: rgba(231, 76, 60, 0.1);
  transform: translateY(-2px);
  box-shadow: 0 4px 12px rgba(231, 76, 60, 0.15);
}

.next-icon {
  font-size: 2rem;
}

.next-card h4 {
  font-size: 1.1rem;
  font-weight: 600;
  margin-bottom: 4px;
  color: var(--blite-red);
}

.next-card p {
  font-size: 0.9rem;
  margin: 0;
  color: var(--text-muted);
}
</style>
