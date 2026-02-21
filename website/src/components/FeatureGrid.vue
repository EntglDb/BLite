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
    description: 'Built-in R-Tree for geospatial queries and vector search.'
  },
  {
    icon: '🔒',
    title: 'Embedded First',
    description: 'No external dependencies. No cloud required. Zero latency.'
  },
  {
    icon: '🔌',
    title: 'Schema-less API',
    description: 'BLiteEngine + DynamicCollection for schema-less BSON queries. No compile-time types needed — same kernel, same performance.'
  }
]
</script>

<template>
  <section class="features">
    <div class="container">
      <h2 class="section-title">Built for<span class="brand-gradient"> Performance</span></h2>
      <p class="section-subtitle">Modern embedded database for .NET 10 applications</p>
      
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
