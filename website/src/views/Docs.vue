<script setup lang="ts">
import { RouterView, useRoute } from 'vue-router'

const route = useRoute()

const docSections = [
  { path: '/docs/getting-started', label: 'Getting Started', icon: '🚀' },
  { path: '/docs/installation', label: 'Installation', icon: '📦' },
  { path: '/docs/transactions', label: 'Transactions & Async', icon: '🔄' },
  { path: '/docs/crud', label: 'CRUD Operations', icon: '✏️' },
  { path: '/docs/querying', label: 'Querying', icon: '🔍' },
  { path: '/docs/generators', label: 'Source Generators', icon: '🤖' },
  { path: '/docs/cdc', label: 'Change Data Capture', icon: '📡' },
  { path: '/docs/spatial', label: 'Spatial Indexing', icon: '🌍' },
  { path: '/docs/architecture', label: 'Architecture', icon: '🏗️' },
  { path: '/docs/converters', label: 'Custom ID Converters', icon: '🆔' },
  { path: '/docs/benchmarks', label: 'Benchmarks', icon: '⚡' },
  { path: '/docs/dynamic-api', label: 'Schema-less API', icon: '🔌' },
  { path: '/docs/blql', label: 'BLQL', icon: '🔎' },
  { path: '/docs/timeseries', label: 'TimeSeries', icon: '📈' }
]

const isActive = (path: string) => route.path === path
</script>

<template>
  <div class="docs-layout">
    <aside class="docs-sidebar">
      <div class="sidebar-header">
        <h2>Documentation</h2>
      </div>
      <nav class="sidebar-nav">
        <router-link
          v-for="section in docSections"
          :key="section.path"
          :to="section.path"
          :class="{ active: isActive(section.path) }"
          class="nav-item"
        >
          <span class="nav-icon">{{ section.icon }}</span>
          <span class="nav-label">{{ section.label }}</span>
        </router-link>
      </nav>
    </aside>

    <main class="docs-content">
      <div class="container">
        <RouterView />
      </div>
    </main>
  </div>
</template>

<style scoped>
.docs-layout {
  display: flex;
  min-height: calc(100vh - var(--header-height));
  position: relative;
}

.docs-sidebar {
  width: 280px;
  background: rgba(15, 15, 15, 0.5);
  border-right: 1px solid rgba(231, 76, 60, 0.1);
  position: sticky;
  top: var(--header-height);
  height: calc(100vh - var(--header-height));
  overflow-y: auto;
  padding: 32px 0;
}

.sidebar-header {
  padding: 0 24px 24px;
  border-bottom: 1px solid rgba(231, 76, 60, 0.1);
}

.sidebar-header h2 {
  font-size: 1.5rem;
  font-weight: 700;
  color: var(--blite-red);
}

.sidebar-nav {
  padding: 24px 0;
  display: flex;
  flex-direction: column;
}

.nav-item {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 12px 24px;
  color: var(--text-secondary);
  transition: all 0.2s ease;
  border-left: 2px solid transparent;
}

.nav-item:hover {
  background: rgba(231, 76, 60, 0.05);
  color: var(--text-primary);
}

.nav-item.active {
  background: rgba(231, 76, 60, 0.1);
  border-left-color: var(--blite-red);
  color: var(--blite-red);
}

.nav-icon {
  font-size: 1.2rem;
}

.nav-label {
  font-size: 0.95rem;
  font-weight: 500;
}

.docs-content {
  flex: 1;
  padding: 48px 0;
  max-width: 900px;
}

@media (max-width: 1024px) {
  .docs-sidebar {
    position: fixed;
    left: -280px;
    z-index: 50;
    transition: left 0.3s ease;
  }
  
  .docs-sidebar.open {
    left: 0;
  }
}
</style>
