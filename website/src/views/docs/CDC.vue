<template>
  <div class="doc-page">
    <h1>📡 <span class="title-gradient">Change Data Capture</span></h1>
    <p class="lead">Real-time change streams with 1000+ concurrent subscribers and zero-blocking architecture.</p>

    <section>
      <h2>What is CDC?</h2>
      <p>Change Data Capture allows you to observe and react to document changes in real-time. Perfect for:</p>
      <ul>
        <li>Real-time UI updates</li>
        <li>Cache invalidation</li>
        <li>Event-driven architectures</li>
        <li>Audit logging</li>
      </ul>
    </section>

    <section>
      <h2>Basic Usage</h2>
      <pre><code><span class="keyword">var</span> users = db.GetCollection&lt;<span class="type">User</span>&gt;();

<span class="comment">// Subscribe to all changes</span>
users.Watch().Subscribe(change =>
{
    Console.WriteLine($<span class="string">"{change.OperationType}: {change.DocumentId}"</span>);
});</code></pre>
    </section>

    <section>
      <h2>Change Types</h2>
      <pre><code><span class="keyword">var</span> subscription = users.Watch().Subscribe(change =>
{
    <span class="keyword">switch</span> (change.OperationType)
    {
        <span class="keyword">case</span> <span class="type">ChangeStreamOperationType</span>.Insert:
            Console.WriteLine(<span class="string">"New document: "</span> + change.FullDocument.Name);
            <span class="keyword">break</span>;
            
        <span class="keyword">case</span> <span class="type">ChangeStreamOperationType</span>.Update:
            Console.WriteLine(<span class="string">"Updated: "</span> + change.DocumentId);
            <span class="keyword">break</span>;
            
        <span class="keyword">case</span> <span class="type">ChangeStreamOperationType</span>.Delete:
            Console.WriteLine(<span class="string">"Deleted: "</span> + change.DocumentId);
            <span class="keyword">break</span>;
    }
});</code></pre>
    </section>

    <section>
      <h2>Filtering Events</h2>
      
      <h3>Filter by Operation</h3>
      <pre><code><span class="comment">// Only watch inserts</span>
users.Watch()
    .Where(c => c.OperationType == <span class="type">ChangeStreamOperationType</span>.Insert)
    .Subscribe(change => { <span class="comment">/* ... */</span> });</code></pre>

      <h3>Filter by Document</h3>
      <pre><code><span class="comment">// Only watch specific users</span>
users.Watch()
    .Where(c => c.FullDocument?.Age > <span class="number">18</span>)
    .Subscribe(change =>
    {
        Console.WriteLine(<span class="string">"Adult user changed: "</span> + change.FullDocument.Name);
    });</code></pre>
    </section>

    <section>
      <h2>Async Processing</h2>
      <pre><code>users.Watch().Subscribe(<span class="keyword">async</span> change =>
{
    <span class="keyword">await</span> SendNotificationAsync(change);
    <span class="keyword">await</span> UpdateCacheAsync(change);
});</code></pre>
    </section>

    <section>
      <h2>Disposing Subscriptions</h2>
      <pre><code><span class="keyword">var</span> subscription = users.Watch().Subscribe(change =>
{
    <span class="comment">// Handle change</span>
});

<span class="comment">// Later: stop watching</span>
subscription.Dispose();</code></pre>
    </section>

    <section>
      <h2>Scalability</h2>
      <p>BLite's CDC uses a <strong>"Channel-per-subscriber"</strong> architecture:</p>
      <ul>
        <li>✅ <strong>1000+ subscribers</strong> supported simultaneously</li>
        <li>✅ <strong>Zero-blocking</strong> - Slow subscribers don't affect others</li>
        <li>✅ <strong>Zero-allocation when idle</strong> - No overhead without watchers</li>
        <li>✅ <strong>Transactional consistency</strong> - Events fire after commit</li>
      </ul>
    </section>

    <section>
      <h2>Full Example</h2>
      <pre><code><span class="keyword">var</span> db = <span class="keyword">new</span> <span class="type">DocumentDb</span>(<span class="string">"mydb.blite"</span>);
<span class="keyword">var</span> users = db.GetCollection&lt;<span class="type">User</span>&gt;();

<span class="comment">// Real-time cache updater</span>
<span class="keyword">var</span> cacheSubscription = users.Watch()
    .Where(c => c.OperationType != <span class="type">ChangeStreamOperationType</span>.Delete)
    .Subscribe(change =>
    {
        cache.Set(change.DocumentId, change.FullDocument);
    });

<span class="comment">// Audit logger</span>
<span class="keyword">var</span> auditSubscription = users.Watch()
    .Subscribe(<span class="keyword">async</span> change =>
    {
        <span class="keyword">await</span> auditLog.WriteAsync(<span class="keyword">new</span>
        {
            Timestamp = DateTime.UtcNow,
            Operation = change.OperationType,
            UserId = change.DocumentId
        });
    });

<span class="comment">// Clean up on shutdown</span>
cacheSubscription.Dispose();
auditSubscription.Dispose();</code></pre>
    </section>

    <section>
      <h2>Best Practices</h2>
      <ul>
        <li>✅ Use filtering to reduce unnecessary work</li>
        <li>✅ Handle errors in subscribers to avoid crashes</li>
        <li>✅ Dispose subscriptions when no longer needed</li>
        <li>✅ Use async operations for I/O-bound work</li>
        <li>⚠️ Keep subscriber logic fast to avoid backpressure</li>
        <li>⚠️ Don't perform long-running operations in the callback</li>
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
