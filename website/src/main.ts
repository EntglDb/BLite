import { ViteSSG } from 'vite-ssg'
import './style.css'
import App from './App.vue'
import { routes } from './router'

// ViteSSG handles createWebHistory (client) vs createMemoryHistory (SSG/Node)
// Title/meta are managed by useHead in App.vue — vite-ssg injects them into the static HTML
export const createApp = ViteSSG(
    App,
    {
        routes,
        scrollBehavior(_to, _from, savedPosition) {
            return savedPosition ?? { top: 0, left: 0 }
        }
    }
)
