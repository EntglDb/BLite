<template>
  <div class="doc-page">
    <h1>✏️ <span class="title-gradient">CRUD Operations</span></h1>
    <p class="lead">Learn how to create, read, update, and delete documents in BLite collections.</p>

    <section>
      <h2>Insert</h2>
      <p>Add new documents to a collection:</p>
      
      <h3>Single Insert</h3>
      <pre><code><span class="keyword">var</span> user = <span class="keyword">new</span> <span class="type">User</span>
{
    Name = <span class="string">"Alice"</span>,
    Age = <span class="number">30</span>,
    Email = <span class="string">"alice@example.com"</span>
};

users.Insert(user);
<span class="comment">// ID is automatically generated and assigned</span>
Console.WriteLine(user.Id);</code></pre>

      <h3>Bulk Insert</h3>
      <pre><code><span class="keyword">var</span> batch = <span class="keyword">new</span> <span class="type">List</span>&lt;<span class="type">User</span>&gt;
{
    <span class="keyword">new</span> { Name = <span class="string">"Bob"</span>, Age = <span class="number">25</span> },
    <span class="keyword">new</span> { Name = <span class="string">"Charlie"</span>, Age = <span class="number">35</span> },
    <span class="keyword">new</span> { Name = <span class="string">"Diana"</span>, Age = <span class="number">28</span> }
};

users.InsertMany(batch);</code></pre>

      <h3>Async Insert</h3>
      <pre><code><span class="keyword">await</span> users.InsertAsync(user);
<span class="keyword">await</span> users.InsertManyAsync(batch);</code></pre>
    </section>

    <section>
      <h2>Read</h2>
      
      <h3>Find by ID</h3>
      <pre><code><span class="keyword">var</span> user = users.FindById(userId);
<span class="keyword">if</span> (user != <span class="keyword">null</span>)
{
    Console.WriteLine(user.Name);
}</code></pre>

      <h3>Find by ID — Async</h3>
      <pre><code><span class="type">User</span>? user = <span class="keyword">await</span> users.FindByIdAsync(userId, ct);</code></pre>

      <h3>Find All</h3>
      <pre><code><span class="keyword">var</span> allUsers = users.FindAll();</code></pre>

      <h3>Find All — Async Streaming</h3>
      <pre><code><span class="keyword">await foreach</span> (<span class="keyword">var</span> user <span class="keyword">in</span> users.FindAllAsync(ct))
{
    Process(user);
}</code></pre>

      <h3>Query with Filter</h3>
      <pre><code><span class="keyword">var</span> adults = users.AsQueryable()
    .Where(u => u.Age >= <span class="number">18</span>)
    .AsEnumerable();</code></pre>

      <h3>Query — Async LINQ</h3>
      <pre><code><span class="comment">// Materialise to list</span>
<span class="type">List</span>&lt;<span class="type">User</span>&gt; adults = <span class="keyword">await</span> users.AsQueryable()
    .Where(u => u.Age >= <span class="number">18</span>)
    .ToListAsync(ct);

<span class="comment">// First match</span>
<span class="type">User</span>? alice = <span class="keyword">await</span> users.AsQueryable()
    .FirstOrDefaultAsync(u => u.Name == <span class="string">"Alice"</span>, ct);

<span class="comment">// Aggregates</span>
<span class="keyword">int</span>  count  = <span class="keyword">await</span> users.AsQueryable().CountAsync(ct);
<span class="keyword">bool</span> any    = <span class="keyword">await</span> users.AsQueryable().AnyAsync(u => u.Age > <span class="number">60</span>, ct);
<span class="keyword">bool</span> all    = <span class="keyword">await</span> users.AsQueryable().AllAsync(u => u.Active, ct);

<span class="comment">// Materialise to array</span>
<span class="type">User</span>[] arr = <span class="keyword">await</span> users.AsQueryable().ToArrayAsync(ct);</code></pre>
    </section>

    <section>
      <h2>Update</h2>
      
      <h3>Update Single Document</h3>
      <pre><code><span class="keyword">var</span> user = users.FindById(userId);
user.Age = <span class="number">31</span>;
user.Email = <span class="string">"newemail@example.com"</span>;

users.Update(user);</code></pre>

      <h3>Upsert (Insert or Update)</h3>
      <pre><code><span class="comment">// Insert if not exists, update if exists</span>
users.Upsert(user);</code></pre>

      <h3>Partial Update</h3>
      <pre><code><span class="keyword">var</span> user = users.FindById(userId);
<span class="keyword">if</span> (user != <span class="keyword">null</span>)
{
    user.Age += <span class="number">1</span>; <span class="comment">// Increment age</span>
    users.Update(user);
}</code></pre>

      <h3>Async Update</h3>
      <pre><code><span class="keyword">await</span> users.UpdateAsync(user);
<span class="keyword">await</span> users.UpsertAsync(user);</code></pre>
    </section>

    <section>
      <h2>Delete</h2>
      
      <h3>Delete by ID</h3>
      <pre><code><span class="keyword">var</span> success = users.Delete(userId);
Console.WriteLine(success); <span class="comment">// true if deleted</span></code></pre>

      <h3>Delete by Document</h3>
      <pre><code><span class="keyword">var</span> user = users.FindById(userId);
users.Delete(user.Id);</code></pre>

      <h3>Delete Many</h3>
      <pre><code><span class="keyword">var</span> toDelete = users.AsQueryable()
    .Where(u => u.Age < <span class="number">18</span>)
    .Select(u => u.Id)
    .AsEnumerable();

<span class="keyword">foreach</span> (<span class="keyword">var</span> id <span class="keyword">in</span> toDelete)
{
    users.Delete(id);
}</code></pre>

      <h3>Async Delete</h3>
      <pre><code><span class="keyword">await</span> users.DeleteAsync(userId);</code></pre>
    </section>

    <section>
      <h2>Validation</h2>
      <p>BLite respects DataAnnotations attributes automatically:</p>
      <pre><code><span class="keyword">public class</span> <span class="type">User</span>
{
    [<span class="type">Required</span>]
    [<span class="type">StringLength</span>(<span class="number">100</span>, MinimumLength = <span class="number">2</span>)]
    <span class="keyword">public string</span> Name { <span class="keyword">get</span>; <span class="keyword">set</span>; }
    
    [<span class="type">Range</span>(<span class="number">0</span>, <span class="number">150</span>)]
    <span class="keyword">public int</span> Age { <span class="keyword">get</span>; <span class="keyword">set</span>; }
    
    [<span class="type">EmailAddress</span>]
    <span class="keyword">public string</span> Email { <span class="keyword">get</span>; <span class="keyword">set</span>; }
}

<span class="keyword">try</span>
{
    users.Insert(<span class="keyword">new</span> <span class="type">User</span> { Name = <span class="string">""</span>, Age = <span class="number">200</span> });
}
<span class="keyword">catch</span> (<span class="type">ValidationException</span> ex)
{
    Console.WriteLine(ex.Message);
}</code></pre>
    </section>

    <section>
      <h2>Best Practices</h2>
      <ul>
        <li>✅ Use <strong>async methods</strong> for better scalability — <code>FindByIdAsync</code>, <code>FindAllAsync</code>, <code>ToListAsync</code></li>
        <li>✅ Wrap writes in <strong>transactions</strong> for consistency</li>
        <li>✅ Use <strong>bulk operations</strong> for large datasets</li>
        <li>✅ Leverage <strong>validation attributes</strong> for data integrity</li>
        <li>⚠️ Avoid round-trips — batch related operations</li>
        <li>🔌 Need schema-less queries? See <router-link to="/docs/dynamic-api">Schema-less API (BLiteEngine)</router-link></li>
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
.keyword { color: var(--blite-red); }
.type { color: #06b6d4; }
.string { color: #a1a1aa; }
.number { color: #06b6d4; }
.comment { color: #52525b; font-style: italic; }
</style>
