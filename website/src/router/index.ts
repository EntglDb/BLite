import { createRouter, createWebHistory } from 'vue-router'
import Home from '../views/Home.vue'
import Docs from '../views/Docs.vue'
import GettingStarted from '../views/docs/GettingStarted.vue'
import Installation from '../views/docs/Installation.vue'
import Transactions from '../views/docs/Transactions.vue'
import CRUD from '../views/docs/CRUD.vue'
import Querying from '../views/docs/Querying.vue'
import Generators from '../views/docs/Generators.vue'
import CDC from '../views/docs/CDC.vue'
import Spatial from '../views/docs/Spatial.vue'
import Architecture from '../views/docs/Architecture.vue'
import Converters from '../views/docs/Converters.vue'
import Benchmarks from '../views/docs/Benchmarks.vue'
import Comparisons from '../views/docs/Comparisons.vue'

const router = createRouter({
    history: createWebHistory(import.meta.env.BASE_URL),
    scrollBehavior(_to, _from, savedPosition) {
        if (savedPosition) {
            return savedPosition
        } else {
            return { top: 0, left: 0 }
        }
    },
    routes: [
        {
            path: '/',
            name: 'home',
            component: Home
        },
        {
            path: '/docs',
            component: Docs,
            redirect: '/docs/getting-started',
            children: [
                { path: 'getting-started', component: GettingStarted },
                { path: 'installation', component: Installation },
                { path: 'transactions', component: Transactions },
                { path: 'crud', component: CRUD },
                { path: 'querying', component: Querying },
                { path: 'generators', component: Generators },
                { path: 'cdc', component: CDC },
                { path: 'spatial', component: Spatial },
                { path: 'architecture', component: Architecture },
                { path: 'converters', component: Converters },
                { path: 'benchmarks', component: Benchmarks },
                { path: 'comparisons', component: Comparisons }
            ]
        }
    ]
})

export default router
