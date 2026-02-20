<script setup lang="ts">
import { RouterView, useRoute } from 'vue-router'
import { useHead } from '@unhead/vue'
import { DEFAULT_TITLE, DEFAULT_DESC } from './router'

const route = useRoute()
const BASE_URL = 'https://blitedb.com'

// useHead is reactive: vite-ssg bakes the correct title/description into the
// static HTML for each route at build time; on the client it stays reactive.
useHead(() => {
  const meta = route.meta as { title?: string; description?: string }
  const title = meta.title ?? DEFAULT_TITLE
  const description = meta.description ?? DEFAULT_DESC
  const url = `${BASE_URL}${route.path}`
  return {
    title,
    meta: [
      { name: 'description', content: description },
      { property: 'og:title', content: title },
      { property: 'og:description', content: description },
      { property: 'og:url', content: url },
      { name: 'twitter:title', content: title },
      { name: 'twitter:description', content: description },
    ],
    link: [
      { rel: 'canonical', href: url },
    ],
  }
})
</script>

<template>
  <div class="app-layout">
    <header class="main-header glass-panel">
      <div class="container header-content">
        <router-link to="/" class="logo-link">
          <img src="/icon.svg" alt="BLite" class="logo-img" />
          <div class="logo-text">BLite<span class="dot">.</span></div>
        </router-link>
        <nav>
          <router-link to="/">Home</router-link>
          <router-link to="/docs">Docs</router-link>
          <router-link to="/comparisons">Comparisons</router-link>
          <a href="https://github.com/EntglDb/BLite" target="_blank" class="github-link">GitHub ↗</a>
        </nav>
      </div>
    </header>

    <main>
      <RouterView v-slot="{ Component }">
        <transition name="fade" mode="out-in">
          <component :is="Component" />
        </transition>
      </RouterView>
    </main>

    <footer class="main-footer">
      <div class="container">
        <p>© 2026 BLite Project. Open Source (MIT).</p>
        <p class="tagline">"Sky is Blite, no Clouds today."</p>
      </div>
    </footer>
  </div>
</template>

<style scoped>
.app-layout {
  min-height: 100vh;
  display: flex;
  flex-direction: column;
}

.main-header {
  position: fixed;
  top: 0;
  width: 100%;
  height: var(--header-height);
  z-index: 100;
  border-bottom: 1px solid rgba(231, 76, 60, 0.1);
  backdrop-filter: blur(12px);
  background: rgba(15, 15, 15, 0.8);
}

.header-content {
  display: flex;
  align-items: center;
  justify-content: space-between;
  height: 100%;
}

.logo-link {
  display: flex;
  align-items: center;
  gap: 12px;
  text-decoration: none;
  transition: opacity 0.2s;
}

.logo-link:hover {
  opacity: 0.8;
}

.logo-img {
  width: 36px;
  height: 36px;
  object-fit: contain;
}

.logo-text {
  font-family: var(--font-mono);
  font-weight: 700;
  font-size: 1.3rem;
  letter-spacing: -0.5px;
  color: var(--text-primary);
}

.logo-text .dot {
  color: var(--blite-red);
}

nav {
  display: flex;
  gap: 32px;
}

nav a {
  font-size: 0.95rem;
  font-weight: 500;
  color: var(--text-secondary);
}

nav a:hover, nav a.router-link-active {
  color: var(--text-primary);
}

.github-link {
  color: var(--blite-red) !important;
  font-weight: 600 !important;
}

main {
  padding-top: var(--header-height);
  flex: 1;
}

.main-footer {
  border-top: 1px solid var(--border-color);
  padding: 40px 0;
  margin-top: 80px;
  color: var(--text-muted);
  font-size: 0.9rem;
  text-align: center;
}

.main-footer .tagline {
  margin-top: 8px;
  color: var(--blite-red);
  font-style: italic;
  font-size: 0.85rem;
}

.fade-enter-active,
.fade-leave-active {
  transition: opacity 0.3s ease;
}

.fade-enter-from,
.fade-leave-to {
  opacity: 0;
}
</style>
