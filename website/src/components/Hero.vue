<script setup lang="ts">
const goGithub = () => window.open('https://github.com/EntglDb/BLite', '_blank');
</script>

<template>
  <section class="hero">
    <div class="hero-bg">
      <div class="aurora-layer"></div>
      <div class="glow-orb orb-1"></div>
      <div class="glow-orb orb-2"></div>
      <div class="rising-sun"></div>
    </div>
    
    <div class="container hero-content">
      <div class="badge animate-fade-in" style="animation-delay: 0.1s">
        <span class="badge-dot"></span> v1.6.1 Released - Enum Support &amp; BLiteEngine Key Map API
      </div>
      
      <h1 class="hero-title animate-fade-in" style="animation-delay: 0.2s">
        Sky is <span class="brand-gradient">Blite</span>,<br/>
        no Clouds today
      </h1>
      
      <p class="hero-subtitle animate-fade-in" style="animation-delay: 0.3s">
        The Binary-Light Document Store for AI-Ready Apps.<br/>
        High performance, zero allocation. Local-first, cloud-free.
      </p>
      
      <div class="hero-actions animate-fade-in" style="animation-delay: 0.4s">
        <router-link to="/docs/getting-started" class="btn btn-primary">Get Started</router-link>
        <button class="btn btn-secondary" @click="goGithub">View on GitHub</button>
      </div>

      <div class="code-preview animate-fade-in" style="animation-delay: 0.5s">
        <div class="code-header">Quick Start</div>
        <pre><code>dotnet add package BLite

<span class="code-keyword">using</span> <span class="code-type">BLite.Core</span>;

<span class="code-keyword">var</span> db = <span class="code-keyword">new</span> <span class="code-type">DocumentDb</span>(<span class="code-string">"mydb.blite"</span>);
<span class="code-keyword">var</span> users = db.GetCollection&lt;<span class="code-type">User</span>&gt;();
users.Insert(<span class="code-keyword">new</span> <span class="code-type">User</span> { Name = <span class="code-string">"Alice"</span> });</code></pre>
      </div>

      <div class="hero-stats animate-fade-in" style="animation-delay: 0.6s">
        <div class="stat">
          <div class="stat-value">0</div>
          <div class="stat-label">Allocations</div>
        </div>
        <div class="stat-sep"></div>
        <div class="stat">
          <div class="stat-value">CDC</div>
          <div class="stat-label">Streams</div>
        </div>
        <div class="stat-sep"></div>
        <div class="stat">
          <div class="stat-value">BSON</div>
          <div class="stat-label">Native</div>
        </div>
        <div class="stat-sep"></div>
        <div class="stat">
          <div class="stat-value">ACID</div>
          <div class="stat-label">Transactions</div>
        </div>
      </div>
    </div>
  </section>
</template>

<style scoped>
.hero {
  position: relative;
  min-height: 100vh;
  display: flex;
  align-items: center;
  justify-content: center;
  padding-top: 60px;
  overflow: hidden;
}

.hero-bg {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  pointer-events: none;
  z-index: -1;
  background: radial-gradient(circle at 50% 80%, #1a0f0f 0%, var(--bg-primary) 70%);
}

.aurora-layer {
  position: absolute;
  bottom: 0;
  left: 0;
  width: 100%;
  height: 50%;
  background: linear-gradient(180deg, transparent 0%, var(--blite-red-dark) 100%);
  opacity: 0.3;
}

.glow-orb {
  position: absolute;
  border-radius: 50%;
  filter: blur(100px);
  animation: glowPulse 8s infinite ease-in-out;
}

.orb-1 {
  width: 500px;
  height: 500px;
  background: var(--blite-red);
  top: -150px;
  right: -100px;
  opacity: 0.2;
}

.orb-2 {
  width: 400px;
  height: 400px;
  background: var(--blite-red-glow);
  bottom: -100px;
  left: -100px;
  opacity: 0.25;
  animation-delay: 2s;
}

.rising-sun {
  position: absolute;
  bottom: 10%;
  left: 50%;
  transform: translateX(-50%);
  width: 200px;
  height: 100px;
  background: radial-gradient(ellipse at 50% 100%, var(--blite-red) 0%, transparent 70%);
  opacity: 0.4;
  filter: blur(40px);
}

.hero-content {
  text-align: center;
  display: flex;
  flex-direction: column;
  align-items: center;
  z-index: 10;
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
  width: 6px;
  height: 6px;
  background-color: var(--blite-red);
  border-radius: 50%;
  box-shadow: 0 0 10px var(--blite-red);
  animation: glowPulse 2s infinite;
}

.hero-title {
  font-size: 4.5rem;
  line-height: 1.1;
  font-weight: 800;
  margin-bottom: 24px;
  letter-spacing: -1.5px;
}

.hero-subtitle {
  font-size: 1.25rem;
  color: var(--text-secondary);
  max-width: 650px;
  margin-bottom: 40px;
  line-height: 1.7;
}

.hero-actions {
  display: flex;
  gap: 16px;
  margin-bottom: 40px;
}

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

.btn-primary {
  background: var(--blite-red);
  color: white;
}

.btn-primary:hover {
  transform: translateY(-2px);
  box-shadow: 0 8px 24px rgba(231, 76, 60, 0.4);
  background: var(--blite-red-glow);
}

.btn-secondary {
  background: transparent;
  color: var(--text-primary);
  border: 1px solid rgba(231, 76, 60, 0.4);
}

.btn-secondary:hover {
  border-color: var(--blite-red);
  background: rgba(231, 76, 60, 0.1);
}

.code-preview {
  width: 100%;
  max-width: 600px;
  margin-bottom: 60px;
  background: rgba(10, 10, 10, 0.6);
  border: 1px solid rgba(231, 76, 60, 0.2);
  border-radius: 12px;
  overflow: hidden;
  backdrop-filter: blur(10px);
}

.code-header {
  padding: 12px 20px;
  background: rgba(231, 76, 60, 0.1);
  border-bottom: 1px solid rgba(231, 76, 60, 0.2);
  font-family: var(--font-mono);
  font-size: 0.85rem;
  color: var(--blite-red);
}

.code-preview pre {
  padding: 24px;
  margin: 0;
  overflow-x: auto;
}

.code-preview code {
  font-family: var(--font-mono);
  font-size: 0.9rem;
  line-height: 1.6;
  color: var(--text-secondary);
}

.code-keyword { color: var(--blite-red); }
.code-type { color: #06b6d4; }
.code-string { color: #a1a1aa; }

.hero-stats {
  display: flex;
  align-items: center;
  gap: 40px;
  padding-top: 40px;
  border-top: 1px solid rgba(231, 76, 60, 0.2);
}

.stat {
  display: flex;
  flex-direction: column;
}

.stat-value {
  font-size: 1.75rem;
  font-weight: 700;
  font-family: var(--font-mono);
  color: var(--blite-red);
}

.stat-label {
  font-size: 0.8rem;
  color: var(--text-muted);
  text-transform: uppercase;
  letter-spacing: 1px;
}

.stat-sep {
  width: 1px;
  height: 40px;
  background: rgba(231, 76, 60, 0.2);
}

@media (max-width: 768px) {
  .hero-title {
    font-size: 2.5rem;
  }
  .hero-stats {
    flex-wrap: wrap;
    gap: 20px;
  }
}
</style>
