<template>
  <div class="doc-page">
    <h1>🌍 Spatial Indexing</h1>
    <p class="lead">Built-in R-Tree indexing for geospatial queries and vector search operations.</p>

    <section>
      <h2>Coordinate Tuples</h2>
      <p>BLite uses <code>(double, double)</code> tuples for coordinates to achieve zero allocation:</p>
      <pre><code><span class="keyword">public class</span> <span class="type">Location</span>
{
    [<span class="type">Key</span>]
    <span class="keyword">public</span> <span class="type">ObjectId</span> Id { <span class="keyword">get</span>; <span class="keyword">set</span>; }
    
    <span class="keyword">public string</span> Name { <span class="keyword">get</span>; <span class="keyword">set</span>; }
    
    <span class="comment">// (latitude, longitude)</span>
    <span class="keyword">public</span> (<span class="keyword">double</span>, <span class="keyword">double</span>) Coordinates { <span class="keyword">get</span>; <span class="keyword">set</span>; }
}</code></pre>
    </section>

    <section>
      <h2>Creating Spatial Index</h2>
      <pre><code><span class="keyword">var</span> locations = db.GetCollection&lt;<span class="type">Location</span>&gt;();

<span class="comment">// Create R-Tree spatial index</span>
locations.EnsureSpatialIndex(l => l.Coordinates);</code></pre>
    </section>

    <section>
      <h2>Inserting Locations</h2>
      <pre><code>locations.Insert(<span class="keyword">new</span> <span class="type">Location</span>
{
    Name = <span class="string">"Colosseum"</span>,
    Coordinates = (<span class="number">41.8902</span>, <span class="number">12.4922</span>) <span class="comment">// Rome</span>
});

locations.Insert(<span class="keyword">new</span> <span class="type">Location</span>
{
    Name = <span class="string">"Eiffel Tower"</span>,
    Coordinates = (<span class="number">48.8584</span>, <span class="number">2.2945</span>) <span class="comment">// Paris</span>
});</code></pre>
    </section>

    <section>
      <h2>Proximity Search</h2>
      <p>Find locations within a radius:</p>
      <pre><code><span class="keyword">var</span> centerPoint = (<span class="number">41.9028</span>, <span class="number">12.4964</span>); <span class="comment">// Vatican City</span>
<span class="keyword">var</span> radiusKm = <span class="number">5.0</span>;

<span class="keyword">var</span> nearby = locations.Near(
    l => l.Coordinates,
    centerPoint,
    radiusKm
);

<span class="keyword">foreach</span> (<span class="keyword">var</span> location <span class="keyword">in</span> nearby)
{
    Console.WriteLine(location.Name);
}</code></pre>
    </section>

    <section>
      <h2>Bounding Box Query</h2>
      <p>Find all locations within a rectangular area:</p>
      <pre><code><span class="comment">// Define bounding box: (minLat, minLon), (maxLat, maxLon)</span>
<span class="keyword">var</span> southwest = (<span class="number">41.85</span>, <span class="number">12.45</span>);
<span class="keyword">var</span> northeast = (<span class="number">41.95</span>, <span class="number">12.55</span>);

<span class="keyword">var</span> inBox = locations.Within(
    l => l.Coordinates,
    southwest,
    northeast
);
</code></pre>
    </section>

    <section>
      <h2>Sorted by Distance</h2>
      <p>Get nearest locations with distance calculation:</p>
      <pre><code><span class="keyword">var</span> myLocation = (<span class="number">41.9028</span>, <span class="number">12.4964</span>);

<span class="keyword">var</span> nearest = locations
    .NearestNeighbors(l => l.Coordinates, myLocation, limit: <span class="number">10</span>);

<span class="keyword">foreach</span> (<span class="keyword">var</span> (location, distance) <span class="keyword">in</span> nearest)
{
    Console.WriteLine($<span class="string">"{location.Name}: {distance:F2} km"</span>);
}</code></pre>
    </section>

    <section>
      <h2>Performance</h2>
      <p>R-Tree indexes provide logarithmic search complexity:</p>
      <ul>
        <li>✅ <strong>O(log n)</strong> spatial queries</li>
        <li>✅ <strong>Zero allocation</strong> with tuple coordinates</li>
        <li>✅ <strong>Efficient bounding box</strong> calculations</li>
        <li>✅ <strong>Optimized for proximity</strong> searches</li>
      </ul>
    </section>

    <section>
      <h2>Advanced Example</h2>
      <pre><code><span class="keyword">public class</span> <span class="type">Restaurant</span>
{
    [<span class="type">Key</span>]
    <span class="keyword">public</span> <span class="type">ObjectId</span> Id { <span class="keyword">get</span>; <span class="keyword">set</span>; }
    
    <span class="keyword">public string</span> Name { <span class="keyword">get</span>; <span class="keyword">set</span>; }
    <span class="keyword">public</span> (<span class="keyword">double</span>, <span class="keyword">double</span>) Location { <span class="keyword">get</span>; <span class="keyword">set</span>; }
    <span class="keyword">public string</span> Category { <span class="keyword">get</span>; <span class="keyword">set</span>; }
    <span class="keyword">public double</span> Rating { <span class="keyword">get</span>; <span class="keyword">set</span>; }
}

<span class="comment">// Find nearby Italian restaurants</span>
<span class="keyword">var</span> userLocation = (<span class="number">41.9028</span>, <span class="number">12.4964</span>);

<span class="keyword">var</span> italianNearby = restaurants
    .Near(r => r.Location, userLocation, <span class="number">2.0</span>) <span class="comment">// Within 2km</span>
    .Where(r => r.Category == <span class="string">"Italian"</span> && r.Rating >= <span class="number">4.0</span>)
    .OrderByDescending(r => r.Rating)
    .Take(<span class="number">5</span>);</code></pre>
    </section>

    <section>
      <h2>Best Practices</h2>
      <ul>
        <li>✅ Create spatial indexes <strong>before</strong> bulk inserts</li>
        <li>✅ Use appropriate radius for queries (avoid too large)</li>
        <li>✅ Combine spatial with regular filters for optimal performance</li>
        <li>⚠️ Spatial indexes consume more memory than B-Tree indexes</li>
        <li>⚠️ Update operations are slower with spatial indexes enabled</li>
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
