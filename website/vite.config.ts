import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

export default defineConfig({
  plugins: [vue()],
  base: '/',
  // Bundle vue-router into the SSR build so Vite replaces import.meta.env.SSR=true
  // and Rollup eliminates the createWebHistory branch (which uses window)
  ssr: {
    noExternal: ['vue-router']
  },
  ssgOptions: {
    script: 'async',
    formatting: 'minify',
  }
})
