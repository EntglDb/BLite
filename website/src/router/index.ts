import { type RouteRecordRaw } from 'vue-router'
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

export const DEFAULT_TITLE = 'BLite – High-Performance Embedded Database for .NET'
export const DEFAULT_DESC = 'BLite is an AI-ready, zero-allocation BSON document store for .NET 10. ACID transactions, Change Data Capture, R-Tree spatial indexing — embedded, cloud-free.'

export const routes: RouteRecordRaw[] = [
    {
        path: '/',
        name: 'home',
        component: Home,
        meta: {
            title: DEFAULT_TITLE,
            description: DEFAULT_DESC
        }
    },
    {
        path: '/docs',
        component: Docs,
        redirect: '/docs/getting-started',
        children: [
            {
                path: 'getting-started',
                component: GettingStarted,
                meta: {
                    title: 'Getting Started – BLite',
                    description: 'Get up and running with BLite in minutes. Install via NuGet, define your models, and perform your first CRUD operations with full ACID support.'
                }
            },
            {
                path: 'installation',
                component: Installation,
                meta: {
                    title: 'Installation – BLite',
                    description: 'Install BLite in your .NET 10 project via NuGet. A single package brings the core engine, zero-allocation BSON serializer, and source generators.'
                }
            },
            {
                path: 'transactions',
                component: Transactions,
                meta: {
                    title: 'ACID Transactions – BLite',
                    description: 'BLite supports full ACID transactions with Write-Ahead Logging and Snapshot Isolation. Learn how to compose atomic operations in your .NET apps.'
                }
            },
            {
                path: 'crud',
                component: CRUD,
                meta: {
                    title: 'CRUD Operations – BLite',
                    description: 'Insert, update, delete and read documents in BLite. Type-safe, no reflection — all serialization is generated at compile time via source generators.'
                }
            },
            {
                path: 'querying',
                component: Querying,
                meta: {
                    title: 'Querying with LINQ – BLite',
                    description: 'BLite ships a full LINQ IQueryable provider. Write expressive, type-safe queries that run on bare metal without an ORM layer.'
                }
            },
            {
                path: 'generators',
                component: Generators,
                meta: {
                    title: 'Source Generators – BLite',
                    description: 'BLite uses Roslyn source generators to produce compile-time BSON mappers. Zero reflection, zero overhead — maximum startup and runtime performance.'
                }
            },
            {
                path: 'cdc',
                component: CDC,
                meta: {
                    title: 'Change Data Capture (CDC) – BLite',
                    description: 'Subscribe to real-time document change streams in BLite. Supports 1000+ concurrent CDC subscribers with a zero-blocking, event-driven architecture.'
                }
            },
            {
                path: 'spatial',
                component: Spatial,
                meta: {
                    title: 'Spatial Indexing – BLite',
                    description: 'BLite includes a built-in R-Tree for geospatial and vector search queries. Query by bounding box, radius, or nearest-neighbour without external plugins.'
                }
            },
            {
                path: 'architecture',
                component: Architecture,
                meta: {
                    title: 'Architecture – BLite',
                    description: "Dive into BLite's internals: the Span-based I/O pipeline, WAL engine, B-Tree/R-Tree indexes, and how source generators wire everything together."
                }
            },
            {
                path: 'converters',
                component: Converters,
                meta: {
                    title: 'Custom Converters – BLite',
                    description: 'Register custom BSON type converters in BLite to control exactly how your domain types are serialized and deserialized without allocations.'
                }
            },
            {
                path: 'benchmarks',
                component: Benchmarks,
                meta: {
                    title: 'Benchmarks – BLite',
                    description: 'BLite benchmark results against LiteDB, SQLite, and RocksDB. Throughput, latency, and allocation numbers for read/write workloads on .NET 10.'
                }
            }
        ]
    },
    {
        path: '/comparisons',
        name: 'comparisons',
        component: Comparisons,
        meta: {
            title: 'BLite vs LiteDB, SQLite & RocksDB – Comparisons',
            description: 'Side-by-side comparison of BLite against popular embedded databases: LiteDB, SQLite, and RocksDB. Allocation count, throughput, features, and API ergonomics.'
        }
    }
]


