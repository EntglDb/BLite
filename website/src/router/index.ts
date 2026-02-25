import { type RouteRecordRaw } from 'vue-router'
import Home from '../views/Home.vue'
import Docs from '../views/Docs.vue'
import Studio from '../views/Studio.vue'
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
import DynamicAPI from '../views/docs/DynamicAPI.vue'
import Comparisons from '../views/docs/Comparisons.vue'
import BLQL from '../views/docs/BLQL.vue'
import TimeSeries from '../views/docs/TimeSeries.vue'

export const DEFAULT_TITLE = 'BLite – Embedded NoSQL Database for .NET'
export const DEFAULT_DESC = 'BLite is the high-performance embedded NoSQL database for .NET. Zero-allocation BSON document store with ACID transactions, CDC streams, and spatial indexing — no server, no cloud. Install via NuGet.'

export const routes: RouteRecordRaw[] = [
    {
        path: '/',
        name: 'home',
        component: Home,
        meta: {
            title: 'BLite – Embedded NoSQL Database for .NET | BSON Document Store',
            description: 'BLite is the fastest embedded NoSQL database for .NET. Zero-allocation BSON document store with ACID transactions, CDC streams, R-Tree spatial indexing and source generators. No cloud, install via NuGet.'
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
                    title: 'Getting Started with BLite – Embedded NoSQL for .NET',
                    description: 'Get up and running with BLite — the embedded NoSQL database for .NET — in minutes. Install via NuGet, define your models, and perform your first ACID-safe CRUD operations.'
                }
            },
            {
                path: 'installation',
                component: Installation,
                meta: {
                    title: 'Install BLite – Embedded NoSQL Database for .NET via NuGet',
                    description: 'Install BLite in your .NET project via NuGet. A single package brings the embedded NoSQL engine, zero-allocation BSON serializer, and compile-time source generators.'
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
                    title: 'BLite vs LiteDB vs SQLite – Embedded NoSQL .NET Benchmarks',
                    description: 'Benchmark results comparing BLite against LiteDB and SQLite+JSON. See why BLite is the fastest embedded NoSQL database for .NET 10 — throughput, latency, and zero allocations.'
                }
            },
            {
                path: 'dynamic-api',
                component: DynamicAPI,
                meta: {
                    title: 'Schema-less API (BLiteEngine) – BLite',
                    description: 'Use BLiteEngine and DynamicCollection for schema-less BSON queries without compile-time types. Ideal for server mode, migrations, and scripting.'
                }
            },
            {
                path: 'blql',
                component: BLQL,
                meta: {
                    title: 'BLQL — BLite Query Language',
                    description: 'BLQL is the MQL-inspired query language for DynamicCollection. Filter, sort, project, and page BsonDocument results using JSON strings or the fluent C# API. Built-in injection and ReDoS protection.'
                }
            },
            {
                path: 'timeseries',
                component: TimeSeries,
                meta: {
                    title: 'TimeSeries — BLite',
                    description: 'Native TimeSeries page type in BLite: append-only, time-indexed storage with automatic retention-based pruning. No background threads — pruning fires on insert.'
                }
            }
        ]
    },
    {
        path: '/comparisons',
        name: 'comparisons',
        component: Comparisons,
        meta: {
            title: 'BLite vs LiteDB vs SQLite – Best Embedded NoSQL Database for .NET',
            description: 'Side-by-side comparison: BLite vs LiteDB, SQLite, and RocksDB as embedded NoSQL databases for .NET. Allocation count, throughput, ACID support, API ergonomics and feature set.'
        }
    },
    {
        path: '/studio',
        name: 'studio',
        component: Studio,
        meta: {
            title: 'BLite Studio – GUI for Embedded NoSQL Databases (.NET)',
            description: 'Download BLite Studio, the official cross-platform GUI for browsing, querying, and managing BLite embedded NoSQL databases. Available for Windows, Linux, and macOS.'
        }
    }
]


