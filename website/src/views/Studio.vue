<script setup lang="ts">
const VERSION = '1.9.0'
const BASE = `https://github.com/EntglDb/BLite/releases/download/studio-v${VERSION}`

const downloads = [
  {
    platform: 'Windows',
    icon: '🪟',
    items: [
      { label: 'Installer (.msi)', url: `${BASE}/BLite.Studio-${VERSION}-win-x64.msi`, tag: 'Recommended' },
      { label: 'Portable (.zip)', url: `${BASE}/BLite.Studio-${VERSION}-win-x64-portable.zip`, tag: '' },
    ]
  },
  {
    platform: 'Linux',
    icon: '🐧',
    items: [
      { label: 'Debian package (.deb)', url: `${BASE}/blite-studio_${VERSION}_amd64.deb`, tag: 'Recommended' },
      { label: 'Archive (.tar.gz)', url: `${BASE}/BLite.Studio-${VERSION}-linux-x64.tar.gz`, tag: '' },
    ]
  }
]

const features = [
  { icon: '🔍', title: 'Browse Collections', description: 'Navigate databases, collections and documents in a clean tree view.' },
  { icon: '⚡', title: 'BLQL Query Editor', description: 'Run BLQL queries directly from the UI and inspect results in real time.' },
  { icon: '✏️', title: 'Insert & Edit', description: 'Insert new documents and edit existing ones with a built-in BSON editor.' },
  { icon: '📋', title: 'Connection History', description: 'Quickly reopen recent databases with saved access settings.' },
  { icon: '🔒', title: 'Read-Only Mode', description: 'Open databases in safe read-only mode to prevent accidental writes.' },
  { icon: '🌐', title: 'Cross-Platform', description: 'Native Windows and Linux builds. macOS support coming soon.' },
]
</script>

<template>
  <div class="studio-page">

    <!-- ── Hero ─────────────────────────────────────────────────── -->
    <section class="studio-hero">
      <div class="hero-bg">
        <div class="glow-orb orb-1"></div>
        <div class="glow-orb orb-2"></div>
      </div>
      <div class="container hero-content">
        <div class="badge animate-fade-in">
          <span class="badge-dot"></span> v{{ VERSION }} — Now Available
        </div>
        <h1 class="hero-title animate-fade-in" style="animation-delay:0.1s">
          BLite <span class="brand-gradient">Studio</span>
        </h1>
        <p class="hero-subtitle animate-fade-in" style="animation-delay:0.2s">
          The official GUI for BLite databases.<br/>
          Browse, query, and manage your data without writing a single line of code.
        </p>
        <div class="hero-actions animate-fade-in" style="animation-delay:0.3s">
          <a href="#download" class="btn btn-primary">Download Studio</a>
          <a href="https://github.com/EntglDb/BLite" target="_blank" class="btn btn-secondary">View on GitHub ↗</a>
        </div>
      </div>
    </section>

    <!-- ── Features ──────────────────────────────────────────────── -->
    <section class="studio-features">
      <div class="container">
        <h2 class="section-title">Everything you need to<span class="brand-gradient"> explore</span></h2>
        <p class="section-subtitle">A lightweight, native Avalonia app — no Electron, no web runtime.</p>
        <div class="feature-grid">
          <div
            v-for="(f, i) in features"
            :key="i"
            class="feature-card animate-fade-in"
            :style="`animation-delay: ${0.05 * i}s`"
          >
            <div class="feature-icon">{{ f.icon }}</div>
            <h3 class="feature-title">{{ f.title }}</h3>
            <p class="feature-description">{{ f.description }}</p>
          </div>
        </div>
      </div>
    </section>

    <!-- ── Download ──────────────────────────────────────────────── -->
    <section class="studio-download" id="download">
      <div class="container">
        <h2 class="section-title">Download<span class="brand-gradient"> Studio</span></h2>
        <p class="section-subtitle">Version {{ VERSION }} — MIT License — Self-contained, no runtime required.</p>

        <div class="download-grid">
          <div v-for="group in downloads" :key="group.platform" class="download-card">
            <div class="download-header">
              <span class="platform-icon">{{ group.icon }}</span>
              <span class="platform-name">{{ group.platform }}</span>
            </div>
            <div class="download-items">
              <a
                v-for="item in group.items"
                :key="item.label"
                :href="item.url"
                class="download-link"
                :class="{ primary: item.tag === 'Recommended' }"
              >
                <span class="dl-icon">⬇</span>
                <span class="dl-label">{{ item.label }}</span>
                <span v-if="item.tag" class="dl-tag">{{ item.tag }}</span>
              </a>
            </div>
          </div>
        </div>

        <p class="release-note">
          All releases and changelogs on
          <a href="https://github.com/EntglDb/BLite/releases" target="_blank">GitHub Releases ↗</a>
        </p>
      </div>
    </section>

    <!-- ── BLQL callout ──────────────────────────────────────────── -->
    <section class="blql-callout">
      <div class="container callout-inner">
        <div class="callout-text">
          <h3>Powerful built-in query language</h3>
          <p>Use <strong>BLQL</strong> directly in Studio to filter, sort and project documents
          with the same MQL-inspired syntax available in the .NET API.</p>
        </div>
        <router-link to="/docs/blql" class="btn btn-primary">Read BLQL docs</router-link>
      </div>
    </section>

  </div>
</template>

<style scoped>
.studio-page {
  width: 100%;
}

/* ── Hero ─────────────────────────────────────────────── */
.studio-hero {
  position: relative;
  min-height: 60vh;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 120px 0 80px;
  overflow: hidden;
}

.hero-bg {
  position: absolute;
  inset: 0;
  pointer-events: none;
  z-index: 0;
  background: radial-gradient(circle at 50% 80%, #1a0f0f 0%, var(--bg-primary) 70%);
}

.glow-orb {
  position: absolute;
  border-radius: 50%;
  filter: blur(100px);
  animation: glowPulse 8s infinite ease-in-out;
}
.orb-1 { width: 400px; height: 400px; background: var(--blite-red); top: -120px; right: -80px; opacity: 0.15; }
.orb-2 { width: 320px; height: 320px; background: var(--blite-red-glow); bottom: -80px; left: -80px; opacity: 0.2; animation-delay: 2s; }

.hero-content {
  position: relative;
  z-index: 10;
  text-align: center;
  display: flex;
  flex-direction: column;
  align-items: center;
}

.badge {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 6px 16px;
  background: rgba(231, 76, 60, 0.1);
  border: 1px solid rgba(231, 76, 60, 0.3);
  border-radius: 100px;
  font-size: 0.85rem;
  color: var(--blite-red);
  margin-bottom: 24px;
}
.badge-dot {
  width: 6px; height: 6px;
  background: var(--blite-red);
  border-radius: 50%;
  box-shadow: 0 0 10px var(--blite-red);
  animation: glowPulse 2s infinite;
}

.hero-title {
  font-size: 4rem;
  font-weight: 800;
  line-height: 1.1;
  letter-spacing: -1.5px;
  margin-bottom: 24px;
}

.hero-subtitle {
  font-size: 1.2rem;
  color: var(--text-secondary);
  max-width: 580px;
  margin-bottom: 40px;
  line-height: 1.7;
}

.hero-actions { display: flex; gap: 16px; }

.btn {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  height: 52px;
  padding: 0 36px;
  border-radius: 10px;
  font-weight: 600;
  font-size: 1rem;
  cursor: pointer;
  transition: all 0.2s ease;
  border: none;
  text-decoration: none;
}
.btn-primary { background: var(--blite-red); color: white; }
.btn-primary:hover { transform: translateY(-2px); box-shadow: 0 8px 24px rgba(231,76,60,0.4); background: var(--blite-red-glow); }
.btn-secondary { background: transparent; color: var(--text-primary); border: 1px solid rgba(231,76,60,0.4); }
.btn-secondary:hover { border-color: var(--blite-red); background: rgba(231,76,60,0.1); }

/* ── Sections ─────────────────────────────────────────── */
.studio-features,
.studio-download {
  padding: 100px 0;
}

.section-title {
  font-size: 2.8rem;
  font-weight: 800;
  text-align: center;
  margin-bottom: 16px;
  letter-spacing: -1px;
}
.section-subtitle {
  font-size: 1.1rem;
  color: var(--text-secondary);
  text-align: center;
  margin-bottom: 64px;
}

/* ── Feature grid ─────────────────────────────────────── */
.feature-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
  gap: 28px;
}
.feature-card {
  padding: 28px;
  background: rgba(15, 15, 15, 0.5);
  border: 1px solid rgba(231, 76, 60, 0.1);
  border-radius: 12px;
  transition: all 0.3s ease;
}
.feature-card:hover {
  border-color: rgba(231, 76, 60, 0.4);
  background: rgba(231, 76, 60, 0.05);
  transform: translateY(-4px);
  box-shadow: 0 8px 24px rgba(231, 76, 60, 0.12);
}
.feature-icon { font-size: 2rem; margin-bottom: 16px; }
.feature-title { font-size: 1.05rem; font-weight: 600; margin-bottom: 8px; }
.feature-description { font-size: 0.9rem; color: var(--text-secondary); line-height: 1.6; }

/* ── Download ─────────────────────────────────────────── */
.studio-download { background: rgba(10, 10, 10, 0.3); }

.download-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(340px, 1fr));
  gap: 32px;
  max-width: 820px;
  margin: 0 auto;
}

.download-card {
  background: rgba(15, 15, 15, 0.6);
  border: 1px solid rgba(231, 76, 60, 0.15);
  border-radius: 16px;
  overflow: hidden;
}
.download-header {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 20px 24px;
  background: rgba(231, 76, 60, 0.08);
  border-bottom: 1px solid rgba(231, 76, 60, 0.1);
}
.platform-icon { font-size: 1.5rem; }
.platform-name { font-weight: 700; font-size: 1.1rem; }

.download-items { padding: 16px; display: flex; flex-direction: column; gap: 10px; }

.download-link {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 14px 16px;
  border-radius: 10px;
  text-decoration: none;
  color: var(--text-primary);
  border: 1px solid rgba(231, 76, 60, 0.15);
  transition: all 0.2s ease;
  font-size: 0.95rem;
}
.download-link:hover {
  border-color: var(--blite-red);
  background: rgba(231, 76, 60, 0.06);
}
.download-link.primary {
  border-color: rgba(231, 76, 60, 0.4);
  background: rgba(231, 76, 60, 0.05);
}
.dl-icon { color: var(--blite-red); font-size: 1rem; }
.dl-label { flex: 1; }
.dl-tag {
  font-size: 0.7rem;
  font-weight: 600;
  padding: 2px 8px;
  border-radius: 100px;
  background: rgba(231, 76, 60, 0.2);
  color: var(--blite-red);
  text-transform: uppercase;
  letter-spacing: 0.5px;
}

.release-note {
  text-align: center;
  margin-top: 40px;
  color: var(--text-muted);
  font-size: 0.9rem;
}
.release-note a { color: var(--blite-red); }

/* ── BLQL callout ─────────────────────────────────────── */
.blql-callout {
  border-top: 1px solid rgba(231, 76, 60, 0.1);
  border-bottom: 1px solid rgba(231, 76, 60, 0.1);
  padding: 64px 0;
  background: rgba(231, 76, 60, 0.03);
}
.callout-inner {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 40px;
}
.callout-text h3 { font-size: 1.5rem; font-weight: 700; margin-bottom: 10px; }
.callout-text p { color: var(--text-secondary); max-width: 560px; line-height: 1.7; }
.callout-text strong { color: var(--blite-red); }

@media (max-width: 768px) {
  .hero-title { font-size: 2.6rem; }
  .callout-inner { flex-direction: column; text-align: center; }
}
</style>
