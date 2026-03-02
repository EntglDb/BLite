<script setup lang="ts">
const features = [
  {
    icon: '📡',
    title: 'Change Data Capture',
    description: 'Real-time CDC with 1000+ concurrent subscribers. Zero-blocking architecture.'
  },
  {
    icon: '⚡',
    title: 'Zero Allocation',
    description: 'High-performance BSON serialization with zero heap allocations.'
  },
  {
    icon: '🤖',
    title: 'Source Generators',
    description: 'Compile-time mappers. No reflection, pure performance.'
  },
  {
    icon: '🗄️',
    title: 'ACID Transactions',
    description: 'Full transactional support with Write-Ahead Logging.'
  },
  {
    icon: '🌍',
    title: 'Spatial Indexing',
    description: 'Built-in R-Tree for geospatial queries. 2D bounding-box, nearest-neighbour and intersection queries out of the box.'
  },
  {
    icon: '🧠',
    title: 'Vector Index',
    description: 'Native HNSW vector index for semantic similarity search and RAG patterns. Store embeddings directly in your embedded DB — no external vector store needed.'
  },
  {
    icon: '🔒',
    title: 'Embedded First',
    description: 'No server. No cloud. Add BLite via NuGet and get a full embedded NoSQL database living inside your .NET process — zero latency, zero dependencies.'
  },
  {
    icon: '🔌',
    title: 'Schema-less API',
    description: 'BLiteEngine + DynamicCollection for schema-less BSON queries. No compile-time types needed — same kernel, same performance.'
  },
  {
    icon: '🖥️',
    title: 'BLite Studio',
    description: 'Official cross-platform GUI to browse, query and manage your embedded NoSQL databases. Available for Windows, Linux and macOS.'
  },
  {
    icon: '📈',
    title: 'Native TimeSeries',
    description: 'Dedicated append-only page format for time-ordered data with automatic retention-based pruning. No background threads — pruning fires transparently on insert.'
  },
  {
    icon: '🌐',
    title: 'netstandard2.1',
    description: 'Works everywhere .NET runs — Unity, Xamarin, MAUI, .NET 5, 6, 7, 8, 9 and 10. One package, one binary protocol, all platforms.'
  }
]
</script>

<template>
  <section class="features">
    <div class="container">
      <h2 class="section-title">Built for<span class="brand-gradient"> Performance</span></h2>
      <p class="section-subtitle">The most capable embedded NoSQL database for .NET — zero allocations, ACID transactions, real-time CDC, netstandard2.1 compatible</p>
      
      <div class="feature-grid">
        <div v-for="(feature, index) in features" :key="index" class="feature-card animate-fade-in" :style="`animation-delay: ${0.1 * index}s`">
          <div class="feature-icon">{{ feature.icon }}</div>
          <h3 class="feature-title">{{ feature.title }}</h3>
          <p class="feature-description">{{ feature.description }}</p>
        </div>
      </div>
    </div>
  </section>
</template>

<style scoped>
.features {
  padding: 120px 0;
  position: relative;
}

.section-title {
  font-size: 3rem;
  font-weight: 800;
  text-align: center;
  margin-bottom: 16px;
  letter-spacing: -1px;
}

.section-subtitle {
  font-size: 1.2rem;
  color: var(--text-secondary);
  text-align: center;
  margin-bottom: 80px;
}

.feature-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
  gap: 32px;
}

.feature-card {
  padding: 32px;
  background: rgba(15, 15, 15, 0.5);
  border: 1px solid rgba(231, 76, 60, 0.1);
  border-radius: 12px;
  transition: all 0.3s ease;
}

.feature-card:hover {
  border-color: rgba(231, 76, 60, 0.4);
  background: rgba(231, 76, 60, 0.05);
  transform: translateY(-4px);
  box-shadow: 0 8px 24px rgba(231, 76, 60, 0.15);
}

.feature-icon {
  font-size: 2.5rem;
  margin-bottom: 16px;
}

.feature-title {
  font-size: 1.3rem;
  font-weight: 700;
  margin-bottom: 12px;
  color: var(--text-primary);
}

.feature-description {
  font-size: 0.95rem;
  color: var(--text-secondary);
  line-height: 1.6;
}
</style>
