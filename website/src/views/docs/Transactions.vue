<template>
  <div class="doc-page">
    <h1>🔄 Transactions</h1>
    <p class="lead">BLite provides full ACID transaction support with Write-Ahead Logging (WAL) for durability and crash recovery. Thread-safe with built-in protection against race conditions.</p>

    <section>
      <h2>Implicit Transactions (Recommended)</h2>
      <p>Use <code>SaveChanges()</code> or <code>SaveChangesAsync()</code> for automatic transaction management - similar to Entity Framework Core:</p>
      
      <h3>Synchronous Example</h3>
      <pre><code><span class="keyword">using</span> <span class="keyword">var</span> db = <span class="keyword">new</span> <span class="type">MyDbContext</span>(<span class="string">"mydb.blite"</span>);

<span class="comment">// Begin implicit transaction automatically</span>
<span class="keyword">var</span> alice = <span class="keyword">new</span> <span class="type">User</span> { Name = <span class="string">"Alice"</span>, Age = <span class="number">30</span> };
<span class="keyword">var</span> bob = <span class="keyword">new</span> <span class="type">User</span> { Name = <span class="string">"Bob"</span>, Age = <span class="number">25</span> };

db.Users.Insert(alice);
db.Users.Insert(bob);

<span class="comment">// Commit all changes atomically</span>
db.SaveChanges();</code></pre>

      <h3>Async Example</h3>
      <pre><code><span class="keyword">using</span> <span class="keyword">var</span> db = <span class="keyword">new</span> <span class="type">MyDbContext</span>(<span class="string">"mydb.blite"</span>);

<span class="comment">// Insert multiple documents</span>
db.Users.Insert(<span class="keyword">new</span> <span class="type">User</span> { Name = <span class="string">"Charlie"</span>, Age = <span class="number">35</span> });
db.Users.Insert(<span class="keyword">new</span> <span class="type">User</span> { Name = <span class="string">"Diana"</span>, Age = <span class="number">28</span> });

<span class="comment">// Commit asynchronously</span>
<span class="keyword">await</span> db.SaveChangesAsync();</code></pre>

      <h3>Update & Delete Example</h3>
      <pre><code><span class="keyword">using</span> <span class="keyword">var</span> db = <span class="keyword">new</span> <span class="type">MyDbContext</span>(<span class="string">"mydb.blite"</span>);

<span class="comment">// Load and modify</span>
<span class="keyword">var</span> user = db.Users.FindById(userId);
user.Age = <span class="number">31</span>;
db.Users.Update(user);

<span class="comment">// Delete another</span>
db.Users.Delete(oldUserId);

<span class="comment">// Both operations committed together</span>
db.SaveChanges();</code></pre>

      <h3>Bulk Operations</h3>
      <pre><code><span class="keyword">using</span> <span class="keyword">var</span> db = <span class="keyword">new</span> <span class="type">MyDbContext</span>(<span class="string">"mydb.blite"</span>);

<span class="keyword">var</span> newUsers = <span class="keyword">new</span> <span class="type">List</span>&lt;<span class="type">User</span>&gt;
{
    <span class="keyword">new</span> { Name = <span class="string">"User1"</span>, Age = <span class="number">20</span> },
    <span class="keyword">new</span> { Name = <span class="string">"User2"</span>, Age = <span class="number">21</span> },
    <span class="keyword">new</span> { Name = <span class="string">"User3"</span>, Age = <span class="number">22</span> }
};

db.Users.InsertMany(newUsers);
<span class="keyword">await</span> db.SaveChangesAsync();</code></pre>

      <div class="info-box">
        <strong>💡 Why Use Implicit Transactions?</strong>
        <ul>
          <li>✅ <strong>Simpler code</strong> - No manual transaction management</li>
          <li>✅ <strong>Familiar pattern</strong> - Same as Entity Framework Core</li>
          <li>✅ <strong>Automatic begin</strong> - Transaction starts on first operation</li>
          <li>✅ <strong>Automatic cleanup</strong> - Auto-rollback on exceptions</li>
          <li>✅ <strong>Thread-safe</strong> - Built-in concurrency protection</li>
        </ul>
      </div>
    </section>

    <section>
      <h2>Explicit Transactions</h2>
      <p>For fine-grained control, use explicit transactions:</p>
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
      <h2>Explicit Transactions</h2>
      <p>For fine-grained control, use explicit transactions:</p>
      
      <h3>Basic Explicit Transaction</h3>
      <pre><code><span class="keyword">using</span> <span class="keyword">var</span> db = <span class="keyword">new</span> <span class="type">MyDbContext</span>(<span class="string">"mydb.blite"</span>);

<span class="comment">// Begin explicit transaction</span>
<span class="keyword">using</span> <span class="keyword">var</span> txn = db.BeginTransaction();

<span class="keyword">try</span>
{
    db.Users.Insert(<span class="keyword">new</span> <span class="type">User</span> { Name = <span class="string">"Alice"</span>, Age = <span class="number">30</span> });
    db.Users.Insert(<span class="keyword">new</span> <span class="type">User</span> { Name = <span class="string">"Bob"</span>, Age = <span class="number">25</span> });
    
    <span class="comment">// Explicit commit</span>
    txn.Commit();
}
<span class="keyword">catch</span>
{
    <span class="comment">// Automatically rolled back on dispose if not committed</span>
    <span class="keyword">throw</span>;
}</code></pre>

      <h3>When to Use Explicit Transactions</h3>
      <ul>
        <li>✅ Complex multi-step operations with conditional logic</li>
        <li>✅ When you need to control exactly when commit happens</li>
        <li>✅ Operations spanning multiple collections with dependencies</li>
        <li>✅ When you need to rollback based on business logic</li>
      </ul>
    </section>

    <section>
      <h2>Async Operations</h2>
      <p>BLite supports async/await patterns for non-blocking I/O:</p>
      
      <h3>Async Implicit Transaction</h3>
      <pre><code><span class="keyword">using</span> <span class="keyword">var</span> db = <span class="keyword">new</span> <span class="type">MyDbContext</span>(<span class="string">"mydb.blite"</span>);

db.Users.Insert(<span class="keyword">new</span> <span class="type">User</span> { Name = <span class="string">"Alice"</span> });
db.Users.Insert(<span class="keyword">new</span> <span class="type">User</span> { Name = <span class="string">"Bob"</span> });

<span class="keyword">await</span> db.SaveChangesAsync();</code></pre>

      <h3>Async Explicit Transaction</h3>
      <pre><code><span class="keyword">await using</span> <span class="keyword">var</span> txn = <span class="keyword">await</span> db.BeginTransactionAsync();

<span class="keyword">try</span>
{
    <span class="keyword">await</span> db.Users.InsertAsync(<span class="keyword">new</span> <span class="type">User</span> { Name = <span class="string">"Charlie"</span> });
    <span class="keyword">await</span> db.Users.UpdateAsync(existingUser);
    
    <span class="keyword">await</span> txn.CommitAsync();
}
<span class="keyword">catch</span>
{
    <span class="keyword">await</span> txn.RollbackAsync();
    <span class="keyword">throw</span>;
}</code></pre>

      <h3>Bulk Async Operations with SaveChangesAsync</h3>
      <pre><code><span class="keyword">using</span> <span class="keyword">var</span> db = <span class="keyword">new</span> <span class="type">MyDbContext</span>(<span class="string">"mydb.blite"</span>);

<span class="keyword">var</span> newUsers = <span class="keyword">new</span> <span class="type">List</span>&lt;<span class="type">User</span>&gt;
{
    <span class="keyword">new</span> { Name = <span class="string">"User1"</span>, Age = <span class="number">20</span> },
    <span class="keyword">new</span> { Name = <span class="string">"User2"</span>, Age = <span class="number">21</span> },
    <span class="keyword">new</span> { Name = <span class="string">"User3"</span>, Age = <span class="number">22</span> }
};

<span class="keyword">await</span> db.Users.InsertManyAsync(newUsers);
<span class="keyword">await</span> db.SaveChangesAsync();</code></pre>
    </section>

    <section>
      <h2>Comparing Implicit vs Explicit</h2>
      
      <h3>💡 Implicit Transactions (SaveChanges)</h3>
      <div class="comparison-box success">
        <strong>Best for:</strong>
        <ul>
          <li>Simple CRUD operations</li>
          <li>Batch inserts/updates/deletes</li>
          <li>When you want EF Core-like experience</li>
          <li>Rapid development</li>
        </ul>
        <pre><code><span class="keyword">using</span> <span class="keyword">var</span> db = <span class="keyword">new</span> <span class="type">MyDbContext</span>(<span class="string">"mydb.blite"</span>);
db.Users.Insert(user1);
db.Users.Insert(user2);
db.SaveChanges(); <span class="comment">// Simple!</span></code></pre>
      </div>

      <h3>🔧 Explicit Transactions (BeginTransaction)</h3>
      <div class="comparison-box warning">
        <strong>Best for:</strong>
        <ul>
          <li>Complex business logic with conditionals</li>
          <li>Manual rollback control</li>
          <li>Cross-collection atomic operations</li>
          <li>Fine-grained error handling</li>
        </ul>
        <pre><code><span class="keyword">using</span> <span class="keyword">var</span> txn = db.BeginTransaction();
<span class="keyword">try</span> {
    db.Users.Insert(user);
    <span class="keyword">if</span> (condition) txn.Commit();
    <span class="keyword">else</span> txn.Rollback();
} <span class="keyword">catch</span> { txn.Rollback(); }</code></pre>
      </div>
    </section>

    <section>
      <h2>Error Handling with Implicit Transactions</h2>
      <pre><code><span class="keyword">using</span> <span class="keyword">var</span> db = <span class="keyword">new</span> <span class="type">MyDbContext</span>(<span class="string">"mydb.blite"</span>);

<span class="keyword">try</span>
{
    db.Users.Insert(<span class="keyword">new</span> <span class="type">User</span> { Name = <span class="string">"Alice"</span> });
    db.Users.Insert(<span class="keyword">new</span> <span class="type">User</span> { Name = <span class="string">"Bob"</span> });
    
    db.SaveChanges();
}
<span class="keyword">catch</span> (<span class="type">ValidationException</span> ex)
{
    <span class="comment">// Transaction auto-rolled back</span>
    Console.WriteLine($<span class="string">"Validation failed: {ex.Message}"</span>);
}
<span class="keyword">catch</span> (<span class="type">IOException</span> ex)
{
    <span class="comment">// WAL write failure</span>
    Console.WriteLine($<span class="string">"I/O error: {ex.Message}"</span>);
}</code></pre>
    </section>

    <section>
      <h2>Batch Operations with Explicit Transactions</h2>
      <p>Perform multiple operations atomically:</p>
      <pre><code><span class="keyword">await using</span> <span class="keyword">var</span> txn = <span class="keyword">await</span> db.BeginTransactionAsync();

<span class="comment">// Delete old users</span>
<span class="keyword">var</span> oldUsers = db.Users.AsQueryable()
    .Where(u => u.Age > <span class="number">60</span>)
    .AsEnumerable();
    
<span class="keyword">foreach</span> (<span class="keyword">var</span> user <span class="keyword">in</span> oldUsers)
{
    <span class="keyword">await</span> db.Users.DeleteAsync(user.Id);
}

<span class="comment">// Insert new batch</span>
<span class="keyword">await</span> db.Users.InsertManyAsync(newBatch);

<span class="keyword">await</span> txn.CommitAsync();</code></pre>
    </section>

    <section>
      <h2>Thread-Safety & Concurrency</h2>
      <p>BLite transactions are protected against race conditions using <strong>SemaphoreSlim</strong> to ensure thread-safe access to the current transaction state:</p>
      <pre><code><span class="comment">// Safe to call from multiple threads</span>
<span class="keyword">var</span> tasks = <span class="keyword">new</span> <span class="type">List</span>&lt;<span class="type">Task</span>&gt;();

<span class="keyword">for</span> (<span class="keyword">int</span> i = <span class="number">0</span>; i &lt; <span class="number">10</span>; i++)
{
    tasks.Add(<span class="type">Task</span>.Run(<span class="keyword">async</span> () =>
    {
        <span class="keyword">await using</span> <span class="keyword">var</span> txn = <span class="keyword">await</span> db.BeginTransactionAsync();
        <span class="keyword">await</span> db.Users.InsertAsync(<span class="keyword">new</span> <span class="type">User</span> { Name = $<span class="string">"User{i}"</span> });
        <span class="keyword">await</span> txn.CommitAsync();
    }));
}

<span class="keyword">await</span> <span class="type">Task</span>.WhenAll(tasks);</code></pre>

      <h3>CancellationToken Support</h3>
      <h3>CancellationToken Support</h3>
      <p>All async operations properly handle cancellation:</p>
      <pre><code><span class="keyword">var</span> cts = <span class="keyword">new</span> <span class="type">CancellationTokenSource</span>(<span class="type">TimeSpan</span>.FromSeconds(<span class="number">5</span>));

<span class="keyword">try</span>
{
    <span class="keyword">await using</span> <span class="keyword">var</span> txn = <span class="keyword">await</span> db.BeginTransactionAsync(cts.Token);
    
    <span class="keyword">await</span> db.Users.InsertManyAsync(largeDataset, cts.Token);
    <span class="keyword">await</span> txn.CommitAsync(cts.Token);
}
<span class="keyword">catch</span> (<span class="type">OperationCanceledException</span>)
{
    <span class="comment">// Transaction automatically rolled back</span>
    Console.WriteLine(<span class="string">"Operation cancelled"</span>);
}</code></pre>
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

db.Users.Insert(<span class="keyword">new</span> <span class="type">User</span> { Name = <span class="string">"Test"</span> });

<span class="keyword">if</span> (someCondition)
{
    txn.Rollback(); <span class="comment">// Discard changes</span>
    <span class="keyword">return</span>;
}

txn.Commit();</code></pre>
    </section>

    <section>
      <h2>Read-Only Operations</h2>
      <p>Queries don't require transactions and never block writes:</p>
      <pre><code><span class="keyword">using</span> <span class="keyword">var</span> db = <span class="keyword">new</span> <span class="type">MyDbContext</span>(<span class="string">"mydb.blite"</span>);

<span class="comment">// No transaction needed for queries</span>
<span class="keyword">var</span> users = db.Users.AsQueryable()
    .Where(u => u.Age > <span class="number">25</span>)
    .OrderBy(u => u.Name)
    .AsEnumerable();

<span class="comment">// Complex aggregations also don't need transactions</span>
<span class="keyword">var</span> stats = db.Users.AsQueryable()
    .GroupBy(u => u.Age)
    .Select(g => <span class="keyword">new</span> { Age = g.Key, Count = g.Count() })
    .AsEnumerable();</code></pre>
    </section>

    <section>
      <h2>Best Practices</h2>
      <ul>
        <li>✅ <strong>Use <code>SaveChanges()</code></strong> for simple operations (recommended)</li>
        <li>✅ <strong>Use explicit transactions</strong> for complex logic with conditionals</li>
        <li>✅ <strong>Always use <code>using</code></strong> statements to ensure proper disposal</li>
        <li>✅ <strong>Keep transactions short</strong> to minimize lock contention</li>
        <li>✅ <strong>Use async methods</strong> for I/O-bound operations to improve scalability</li>
        <li>✅ <strong>Handle exceptions</strong> gracefully - transactions auto-rollback on error</li>
        <li>✅ <strong>Pass CancellationToken</strong> for long-running operations to enable timeouts and cancellation</li>
        <li>✅ <strong>Thread-safe by design</strong> - BLite handles concurrent transactions automatically</li>
        <li>⚠️ <strong>Avoid long-running transactions</strong> that hold resources</li>
        <li>⚠️ <strong>Don't nest transactions</strong> - BLite uses a single transaction model</li>
      </ul>
    </section>

    <section>
      <h2>Error Handling with Explicit Transactions</h2>
      <pre><code><span class="keyword">try</span>
{
    <span class="keyword">await using</span> <span class="keyword">var</span> txn = <span class="keyword">await</span> db.BeginTransactionAsync();
    
    <span class="keyword">await</span> db.Users.InsertAsync(newUser);
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

.info-box { 
  padding: 16px 20px; 
  border-radius: 8px; 
  margin: 24px 0; 
  border-left: 4px solid #06b6d4; 
  background: rgba(6, 182, 212, 0.05); 
}

.info-box ul { 
  margin-top: 12px; 
}

.info-box li { 
  color: var(--text-secondary); 
  font-size: 0.95rem; 
}

.comparison-box {
  padding: 20px;
  border-radius: 8px;
  margin: 16px 0;
  border-left: 4px solid;
}

.comparison-box.success {
  border-left-color: #10b981;
  background: rgba(16, 185, 129, 0.05);
}

.comparison-box.warning {
  border-left-color: #f59e0b;
  background: rgba(245, 158, 11, 0.05);
}

.comparison-box ul {
  margin: 12px 0;
}

.comparison-box pre {
  margin-top: 16px;
}
</style>
