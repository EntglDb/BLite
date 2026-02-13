<template>
  <div class="doc-page">
    <h1>🔄 Transactions</h1>
    <p class="lead">BLite provides full ACID transaction support with Write-Ahead Logging (WAL) for durability and crash recovery.</p>

    <section>
      <h2>Basic Transaction</h2>
      <p>All write operations in BLite must occur within a transaction:</p>
      <pre><code><span class="keyword">using</span> BLite.Core;

<span class="keyword">var</span> db = <span class="keyword">new</span> <span class="type">DocumentDb</span>(<span class="string">"mydb.blite"</span>);
<span class="keyword">var</span> users = db.GetCollection&lt;<span class="type">User</span>&gt;();

<span class="comment">// Begin a transaction</span>
<span class="keyword">using</span> <span class="keyword">var</span> txn = db.BeginTransaction();

<span class="keyword">try</span>
{
    users.Insert(<span class="keyword">new</span> <span class="type">User</span> { Name = <span class="string">"Alice"</span>, Age = <span class="number">30</span> });
    users.Insert(<span class="keyword">new</span> <span class="type">User</span> { Name = <span class="string">"Bob"</span>, Age = <span class="number">25</span> });
    
    <span class="comment">// Commit all changes</span>
    txn.Commit();
}
<span class="keyword">catch</span>
{
    <span class="comment">// Automatically rolled back on dispose if not committed</span>
    <span class="keyword">throw</span>;
}</code></pre>
    </section>

    <section>
      <h2>Async Operations</h2>
      <p>BLite supports async/await patterns for non-blocking I/O:</p>
      
      <h3>Async Transaction</h3>
      <pre><code><span class="keyword">await using</span> <span class="keyword">var</span> txn = <span class="keyword">await</span> db.BeginTransactionAsync();

<span class="keyword">try</span>
{
    <span class="keyword">await</span> users.InsertAsync(<span class="keyword">new</span> <span class="type">User</span> { Name = <span class="string">"Charlie"</span> });
    <span class="keyword">await</span> users.UpdateAsync(existingUser);
    
    <span class="keyword">await</span> txn.CommitAsync();
}
<span class="keyword">catch</span>
{
    <span class="keyword">await</span> txn.RollbackAsync();
    <span class="keyword">throw</span>;
}</code></pre>

      <h3>Bulk Async Insert</h3>
      <pre><code><span class="keyword">var</span> newUsers = <span class="keyword">new</span> <span class="type">List</span>&lt;<span class="type">User</span>&gt;
{
    <span class="keyword">new</span> { Name = <span class="string">"User1"</span>, Age = <span class="number">20</span> },
    <span class="keyword">new</span> { Name = <span class="string">"User2"</span>, Age = <span class="number">21</span> },
    <span class="keyword">new</span> { Name = <span class="string">"User3"</span>, Age = <span class="number">22</span> }
};

<span class="keyword">await using</span> <span class="keyword">var</span> txn = <span class="keyword">await</span> db.BeginTransactionAsync();
<span class="keyword">await</span> users.InsertManyAsync(newUsers);
<span class="keyword">await</span> txn.CommitAsync();</code></pre>
    </section>

    <section>
      <h2>Transaction Isolation</h2>
      <p>BLite uses <strong>Snapshot Isolation</strong> to prevent dirty reads and ensure consistency:</p>
      <pre><code><span class="comment">// Transaction 1 sees a consistent snapshot</span>
<span class="keyword">using</span> <span class="keyword">var</span> txn1 = db.BeginTransaction();
<span class="keyword">var</span> user = users.FindById(userId); <span class="comment">// Snapshot at txn1 start</span>

<span class="comment">// Transaction 2 modifies the same document</span>
<span class="keyword">using</span> <span class="keyword">var</span> txn2 = db.BeginTransaction();
user.Age = <span class="number">31</span>;
users.Update(user);
txn2.Commit();

<span class="comment">// txn1 still sees Age = 30 (snapshot isolation)</span>
Console.WriteLine(user.Age); <span class="comment">// 30</span></code></pre>
    </section>

    <section>
      <h2>Explicit Rollback</h2>
      <p>You can manually rollback a transaction if needed:</p>
      <pre><code><span class="keyword">using</span> <span class="keyword">var</span> txn = db.BeginTransaction();

users.Insert(<span class="keyword">new</span> <span class="type">User</span> { Name = <span class="string">"Test"</span> });

<span class="keyword">if</span> (someCondition)
{
    txn.Rollback(); <span class="comment">// Discard changes</span>
    <span class="keyword">return</span>;
}

txn.Commit();</code></pre>
    </section>

    <section>
      <h2>Batch Operations</h2>
      <p>Perform multiple operations atomically:</p>
      <pre><code><span class="keyword">await using</span> <span class="keyword">var</span> txn = <span class="keyword">await</span> db.BeginTransactionAsync();

<span class="comment">// Delete old users</span>
<span class="keyword">var</span> oldUsers = users.AsQueryable()
    .Where(u => u.Age > <span class="number">60</span>)
    .AsEnumerable();
    
<span class="keyword">foreach</span> (<span class="keyword">var</span> user <span class="keyword">in</span> oldUsers)
{
    <span class="keyword">await</span> users.DeleteAsync(user.Id);
}

<span class="comment">// Insert new batch</span>
<span class="keyword">await</span> users.InsertManyAsync(newBatch);

<span class="keyword">await</span> txn.CommitAsync();</code></pre>
    </section>

    <section>
      <h2>Read-Only Transactions</h2>
      <p>For queries that don't modify data, use read-only transactions for better performance:</p>
      <pre><code><span class="keyword">using</span> <span class="keyword">var</span> txn = db.BeginReadOnlyTransaction();

<span class="keyword">var</span> stats = users.AsQueryable()
    .GroupBy(u => u.Age)
    .Select(g => <span class="keyword">new</span> { Age = g.Key, Count = g.Count() })
    .AsEnumerable();

<span class="comment">// No commit needed for read-only</span></code></pre>
    </section>

    <section>
      <h2>Best Practices</h2>
      <ul>
        <li>✅ <strong>Always use <code>using</code></strong> statements to ensure proper disposal</li>
        <li>✅ <strong>Keep transactions short</strong> to minimize lock contention</li>
        <li>✅ <strong>Use async methods</strong> for I/O-bound operations to improve scalability</li>
        <li>✅ <strong>Handle exceptions</strong> gracefully to avoid partial writes</li>
        <li>⚠️ <strong>Avoid long-running transactions</strong> that hold resources</li>
        <li>⚠️ <strong>Don't nest transactions</strong> - BLite uses a single transaction model</li>
      </ul>
    </section>

    <section>
      <h2>Error Handling</h2>
      <pre><code><span class="keyword">try</span>
{
    <span class="keyword">await using</span> <span class="keyword">var</span> txn = <span class="keyword">await</span> db.BeginTransactionAsync();
    
    <span class="keyword">await</span> users.InsertAsync(newUser);
    <span class="keyword">await</span> txn.CommitAsync();
}
<span class="keyword">catch</span> (<span class="type">TransactionConflictException</span> ex)
{
    <span class="comment">// Handle write conflicts (e.g., retry logic)</span>
    Console.WriteLine(<span class="string">"Conflict detected, retrying..."</span>);
}
<span class="keyword">catch</span> (<span class="type">ValidationException</span> ex)
{
    <span class="comment">// Handle validation errors from attributes</span>
    Console.WriteLine($<span class="string">"Validation failed: {ex.Message}"</span>);
}</code></pre>
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
  margin-bottom: 12px;
  color: var(--text-secondary);
  line-height: 1.6;
}

li strong, li code {
  color: var(--blite-red);
}

code {
  font-family: var(--font-mono);
  font-size: 0.9rem;
  background: rgba(231, 76, 60, 0.1);
  padding: 2px 6px;
  border-radius: 4px;
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
.number { color: #06b6d4; }
.comment { color: #52525b; font-style: italic; }
</style>
