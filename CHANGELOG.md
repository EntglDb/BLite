# Change Log

All notable changes to this project will be documented in this file. See [versionize](https://github.com/versionize/versionize) for commit guidelines.

<a name="4.1.4"></a>
## [4.1.4](https://www.github.com/EntglDb/BLite/releases/tag/v4.1.4) (2026-04-04)

<a name="4.1.3"></a>
## [4.1.3](https://www.github.com/EntglDb/BLite/releases/tag/v4.1.3) (2026-04-04)

<a name="4.1.2"></a>
## [4.1.2](https://www.github.com/EntglDb/BLite/releases/tag/v4.1.2) (2026-04-03)

<a name="4.1.1"></a>
## [4.1.1](https://www.github.com/EntglDb/BLite/releases/tag/v4.1.1) (2026-04-02)

### Bug Fixes

* Query, index and transaction optimizations ([e7583cb](https://www.github.com/EntglDb/BLite/commit/e7583cbb64dc848a28443e1abba782dd2f5f1077))

<a name="4.1.0"></a>
## [4.1.0](https://www.github.com/EntglDb/BLite/releases/tag/v4.1.0) (2026-04-01)

### Features

* complete async LINQ materialiser layer for IBLiteQueryable ([#30](https://www.github.com/EntglDb/BLite/issues/30)) ([2499054](https://www.github.com/EntglDb/BLite/commit/2499054432c66070b06080a99495028951e3c4ed))

### Bug Fixes

* Add warm-start buffer sizing; remove WriteCString ([90955bd](https://www.github.com/EntglDb/BLite/commit/90955bd20350332957ad32bf980f3bfcf15e71ab))

<a name="4.0.3"></a>
## [4.0.3](https://www.github.com/EntglDb/BLite/releases/tag/v4.0.3) (2026-03-31)

<a name="4.0.2"></a>
## [4.0.2](https://www.github.com/EntglDb/BLite/releases/tag/v4.0.2) (2026-03-30)

<a name="4.0.1"></a>
## [4.0.1](https://www.github.com/EntglDb/BLite/releases/tag/v4.0.1) (2026-03-26)

### Bug Fixes

* Support ObjectId ordering, fix Execute deadlock ([609cb8a](https://www.github.com/EntglDb/BLite/commit/609cb8a1163e1dc88becf57a64713c1ed4a19ddc))

<a name="4.0.0"></a>
## [4.0.0](https://www.github.com/EntglDb/BLite/releases/tag/v4.0.0) (2026-03-25)

### Features

* Document async-only CRUD API & update samples ([e13fe24](https://www.github.com/EntglDb/BLite/commit/e13fe24e7888ea9067b9423422b682d851111f4d))

### Breaking Changes

* Document async-only CRUD API & update samples ([e13fe24](https://www.github.com/EntglDb/BLite/commit/e13fe24e7888ea9067b9423422b682d851111f4d))

<a name="3.8.1-preview.3"></a>
## [3.8.1-preview.3](https://www.github.com/EntglDb/BLite/releases/tag/v3.8.1-preview.3) (2026-03-25)

<a name="3.8.1-preview.2"></a>
## [3.8.1-preview.2](https://www.github.com/EntglDb/BLite/releases/tag/v3.8.1-preview.2) (2026-03-25)

<a name="3.8.1-preview.1"></a>
## [3.8.1-preview.1](https://www.github.com/EntglDb/BLite/releases/tag/v3.8.1-preview.1) (2026-03-25)

<a name="3.8.1-preview.0"></a>
## [3.8.1-preview.0](https://www.github.com/EntglDb/BLite/releases/tag/v3.8.1-preview.0) (2026-03-24)

<a name="3.8.0"></a>
## [3.8.0](https://www.github.com/EntglDb/BLite/releases/tag/v3.8.0) (2026-03-23)

<a name="3.8.0-preview.3"></a>
## [3.8.0-preview.3](https://www.github.com/EntglDb/BLite/releases/tag/v3.8.0-preview.3) (2026-03-22)

### Bug Fixes

* Non-blocking checkpoints; defer metadata writes ([455d28a](https://www.github.com/EntglDb/BLite/commit/455d28af28df3ebd5459e07e4d208b15d6061e89))
* Use Lazy<PageFile> for collection files ([8e017a1](https://www.github.com/EntglDb/BLite/commit/8e017a128be72b3533aed61eee98bb2a7a7b9c37))

<a name="3.8.0-preview.2"></a>
## [3.8.0-preview.2](https://www.github.com/EntglDb/BLite/releases/tag/v3.8.0-preview.2) (2026-03-22)

### Features

* Add session APIs, metadata lock, and benchmarks ([7e7e3a0](https://www.github.com/EntglDb/BLite/commit/7e7e3a09e8eaa4477ac9689598c3a06c75663472))

<a name="3.8.0-preview.1"></a>
## [3.8.0-preview.1](https://www.github.com/EntglDb/BLite/releases/tag/v3.8.0-preview.1) (2026-03-22)

<a name="3.8.0-preview.0"></a>
## [3.8.0-preview.0](https://www.github.com/EntglDb/BLite/releases/tag/v3.8.0-preview.0) (2026-03-22)

### Features

* add BLiteSession for per-connection isolated transaction context (server mode) ([13b59ec](https://www.github.com/EntglDb/BLite/commit/13b59ec0e72302fad08771bdef49e050c77f661c))
* **storage:** Server() base config, BLiteMigration (single↔multi), KvGetWithExpiry + ImportDictionary ([1a7946e](https://www.github.com/EntglDb/BLite/commit/1a7946eae2ab43b89e98809bcd87992c58d4a950))

### Bug Fixes

* harden PageFile post-dispose safety — volatile _disposed, ThrowIfDisposed, null fields on close ([3411e17](https://www.github.com/EntglDb/BLite/commit/3411e179da5783cbccd2a6812b8749697d22271d))
* replace SemaphoreSlim with ReaderWriterLockSlim in PageFile to fix concurrent read/write race ([97dbb47](https://www.github.com/EntglDb/BLite/commit/97dbb477b274666c9194806bd384c61b11e9624f))
* use _rwLock read lock for netstandard2.1 ReadPageAsync to prevent race with EnsureCapacityCore ([815cfa3](https://www.github.com/EntglDb/BLite/commit/815cfa3cfd002d4f7c484b9b0ae8dad28a34abe7))
* **storage:** bit-tagged pageIds for collision-free multi-file routing, collection scans, WAL recovery, path validation ([7b643a5](https://www.github.com/EntglDb/BLite/commit/7b643a5eccb0ff89000ffda3c45731e183edc572))
* **storage:** implement bit-tagged pageIds to eliminate collision and routing bugs ([4c8ef15](https://www.github.com/EntglDb/BLite/commit/4c8ef15496d3309c3ee991ae10b61be57a14c948))

<a name="3.7.0"></a>
## [3.7.0](https://www.github.com/EntglDb/BLite/releases/tag/v3.7.0) (2026-03-17)

### Features

* optimizing OLAP queries ([f83e0d5](https://www.github.com/EntglDb/BLite/commit/f83e0d561eb875248bcb55c97c10f2ec4839a659))
* optmized group by OLAP queries ([98c49ef](https://www.github.com/EntglDb/BLite/commit/98c49ef9716d55c9cb52663e42677f45dedb32b9))

<a name="3.6.9"></a>
## [3.6.9](https://www.github.com/EntglDb/BLite/releases/tag/v3.6.9) (2026-03-16)

### Bug Fixes

* **sourcegen:** fix mapper name collisions when entity types share a simple class name across different namespaces — underscore characters in namespace/type names are now escaped before separator replacement, making the `GetMapperName` transform bijective ([ee67c33](https://www.github.com/EntglDb/BLite/commit/ee67c33))
* **sourcegen:** parse `.ToCollection("name")` from `OnModelCreating` fluent chains (including `const string` and other compile-time constants) so duplicate-named entities in different namespaces use distinct database collections ([ee67c33](https://www.github.com/EntglDb/BLite/commit/ee67c33))
* **sourcegen:** emit BLITE003 build-time error when two or more entity types in the same `DocumentDbContext` resolve to the same collection name (case-insensitive), preventing silent data corruption at runtime ([ee67c33](https://www.github.com/EntglDb/BLite/commit/ee67c33))

<a name="3.6.8"></a>
## [3.6.8](https://www.github.com/EntglDb/BLite/releases/tag/v3.6.8) (2026-03-16)

<a name="3.6.7"></a>
## [3.6.7](https://www.github.com/EntglDb/BLite/releases/tag/v3.6.7) (2026-03-16)

### Bug Fixes

* **sourcegen:** increase nested type traversal depth to 20 and emit BLITE001 errors instead of silent skips ([fea19e4](https://www.github.com/EntglDb/BLite/commit/fea19e434748bf5fef95f81410f25b036ab64cf2))

<a name="3.6.6"></a>
## [3.6.6](https://www.github.com/EntglDb/BLite/releases/tag/v3.6.6) (2026-03-15)

### Bug Fixes

* AddString EnsureCapacity uses actual UTF-8 byte count ([b5c55be](https://www.github.com/EntglDb/BLite/commit/b5c55bea0120ea77b65087a53b24326dbd8890e4))

<a name="3.6.5"></a>
## [3.6.5](https://www.github.com/EntglDb/BLite/releases/tag/v3.6.5) (2026-03-14)

<a name="3.6.4"></a>
## [3.6.4](https://www.github.com/EntglDb/BLite/releases/tag/v3.6.4) (2026-03-13)

<a name="3.6.3"></a>
## [3.6.3](https://www.github.com/EntglDb/BLite/releases/tag/v3.6.3) (2026-03-12)

### Bug Fixes

* coerced BSON numeric reads + JSON type-preservation ([38d77df](https://www.github.com/EntglDb/BLite/commit/38d77df3095d6a52690277001fa98c428942b38c))

<a name="3.6.2"></a>
## [3.6.2](https://www.github.com/EntglDb/BLite/releases/tag/v3.6.2) (2026-03-12)

### Bug Fixes

* HNSW vector search correctness + edge-case tests ([fca8fc5](https://www.github.com/EntglDb/BLite/commit/fca8fc521e26a2f8947233a682205fde3f46e7e9))

<a name="3.6.1"></a>
## [3.6.1](https://www.github.com/EntglDb/BLite/releases/tag/v3.6.1) (2026-03-11)

### Bug Fixes

* corrects duplicates index inserts ([8b99e9c](https://www.github.com/EntglDb/BLite/commit/8b99e9c5c79a77565695219e528a981eeedf6610))

<a name="3.6.0"></a>
## [3.6.0](https://www.github.com/EntglDb/BLite/releases/tag/v3.6.0) (2026-03-11)

### Features

* adds watch to dynamic collections ([d22857f](https://www.github.com/EntglDb/BLite/commit/d22857f0d5f31818362aa469ed20a8f0e4557336))

<a name="3.5.2"></a>
## [3.5.2](https://www.github.com/EntglDb/BLite/releases/tag/v3.5.2) (2026-03-11)

### Bug Fixes

* adds abstractions for indexes ([59874a7](https://www.github.com/EntglDb/BLite/commit/59874a76ffdd039bd966ac247281da2cce428110))

<a name="3.5.1"></a>
## [3.5.1](https://www.github.com/EntglDb/BLite/releases/tag/v3.5.1) (2026-03-11)

### Bug Fixes

* add missing sync Update, UpdateBulk, Delete, DeleteBulk to IDocumentCollection ([d719b6d](https://www.github.com/EntglDb/BLite/commit/d719b6dcc21b25e03048fe9df96c122f99e17368))

<a name="3.5.0"></a>
## [3.5.0](https://www.github.com/EntglDb/BLite/releases/tag/v3.5.0) (2026-03-11)

### Features

* introduce IDocumentCollection<TId,T> abstraction ([fa68e0a](https://www.github.com/EntglDb/BLite/commit/fa68e0a0f54e8da1fc3cf99cb8d1a85daae690ff))

<a name="3.4.2"></a>
## [3.4.2](https://www.github.com/EntglDb/BLite/releases/tag/v3.4.2) (2026-03-10)

### Bug Fixes

* delete collection metadata ([19dac17](https://www.github.com/EntglDb/BLite/commit/19dac17dff255f59c350d0f0625ca52a686b0337))

<a name="3.4.1"></a>
## [3.4.1](https://www.github.com/EntglDb/BLite/releases/tag/v3.4.1) (2026-03-10)

<a name="3.4.0"></a>
## [3.4.0](https://www.github.com/EntglDb/BLite/releases/tag/v3.4.0) (2026-03-10)

### Features

* auto id fallback for string and Guid ([58a99ec](https://www.github.com/EntglDb/BLite/commit/58a99ec0c86ec453ff795d6258822a5495c98253))

### Bug Fixes

* corrects index navigation for number based indexes ([685873e](https://www.github.com/EntglDb/BLite/commit/685873e8050d7d0f0db2914142de2391ab553797))

<a name="3.3.0"></a>
## [3.3.0](https://www.github.com/EntglDb/BLite/releases/tag/v3.3.0) (2026-03-09)

### Features

* page compaction on delete, private-setter entity tests, TimeSeries in DocumentDbContext ([63213be](https://www.github.com/EntglDb/BLite/commit/63213bec4ad8e47cdf3014d9408b939f8a38352a))
* **studio:** adds better json view ([9991c67](https://www.github.com/EntglDb/BLite/commit/9991c677f3d3e851e02edb01dbc20fdc9ba85c22))

<a name="3.1.1"></a>
## [3.1.1](https://www.github.com/EntglDb/BLite/releases/tag/v3.1.1) (2026-03-07)

### Features

* **sourcegen:** supporto DDD private backing field per collezioni ([46f4e9a](https://www.github.com/EntglDb/BLite/commit/46f4e9a570a0d80aad7a0701b209724f9bce0bb0))

### Bug Fixes

* IndexOptimizer regressione con chiavi ValueObject senza registry ([50c5938](https://www.github.com/EntglDb/BLite/commit/50c5938dcbc41be36b2059c833c23aa83a532e96))

<a name="3.1.0"></a>
## [3.1.0](https://www.github.com/EntglDb/BLite/releases/tag/v3.1.0) (2026-03-07)

### Features

* integrate ValueConverter registry into query pipeline ([4de5919](https://www.github.com/EntglDb/BLite/commit/4de5919ff1cfbfcf4b0b55d247765db42f7a4588))

### Bug Fixes

* **query:** support AndAlso, .Equals() calls and closure captures in expression analysis ([715a77d](https://www.github.com/EntglDb/BLite/commit/715a77d10e5e36b46168babe3714e5a2fc2ef13f))

<a name="3.0.1"></a>
## [3.0.1](https://www.github.com/EntglDb/BLite/releases/tag/v3.0.1) (2026-03-07)

### Bug Fixes

* **schema:** prevent buffer overflow when serializing schemas larger than PageSize ([2495d20](https://www.github.com/EntglDb/BLite/commit/2495d20d4449e3c6028d386099e3b522f3bb862d))

<a name="3.0.0"></a>
## [3.0.0](https://www.github.com/EntglDb/BLite/releases/tag/v3.0.0) (2026-03-06)

### Features

* Add support for embedded property indexes and self-referencing collections ([fd5f9e8](https://www.github.com/EntglDb/BLite/commit/fd5f9e8b4ef46fede0925e84719890dabcc3fc08))

### Breaking Changes

* clarify ModelBuilder configuration precedence and tie HasConversion to Property() ([a5827a5](https://www.github.com/EntglDb/BLite/commit/a5827a50d690d307e96b2941d2c45f5a46f63d87))

<a name="2.0.2"></a>
## [2.0.2](https://www.github.com/EntglDb/BLite/releases/tag/v2.0.2) (2026-03-05)

### Bug Fixes

* corrects missing mapper creation for ValueObjects ([cfde540](https://www.github.com/EntglDb/BLite/commit/cfde540ac4467e5f753481ff09cd367bb7a877b8))

<a name="2.0.1"></a>
## [2.0.1](https://www.github.com/EntglDb/BLite/releases/tag/v2.0.1) (2026-03-04)

### Bug Fixes

* adds all Async methods to DocumentCollection ([954d2cd](https://www.github.com/EntglDb/BLite/commit/954d2cd7ddd3c2555db069aee887ad3d23762a46))

<a name="2.0.0"></a>
## [2.0.0](https://www.github.com/EntglDb/BLite/releases/tag/v2.0.0) (2026-03-02)

### Features

* add netstandard2.1 multi-targeting ([7199025](https://www.github.com/EntglDb/BLite/commit/7199025e9b4f013deae2d4df366f61078c82705c))

### Breaking Changes

* add netstandard2.1 multi-targeting ([7199025](https://www.github.com/EntglDb/BLite/commit/7199025e9b4f013deae2d4df366f61078c82705c))

<a name="1.12.0"></a>
## [1.12.0](https://www.github.com/EntglDb/BLite/releases/tag/v1.12.0) (2026-02-26)

### Features

* native TimeSeries page type with retention pruning + Studio UI ([a119e9b](https://www.github.com/EntglDb/BLite/commit/a119e9b37010c67877765937c5d135de40ba3a5d))

<a name="1.11.0"></a>
## [1.11.0](https://www.github.com/EntglDb/BLite/releases/tag/v1.11.0) (2026-02-25)

### Features

* adds vector source to collection metadata ([5905f20](https://www.github.com/EntglDb/BLite/commit/5905f20414cf0cefac4ae8c93a523b2ab416de58))

### Bug Fixes

* corrects the missing CDC in BLite Engine ([322542a](https://www.github.com/EntglDb/BLite/commit/322542a3db10e1b482ae4b3cc35d2662f8ba33f0))

<a name="1.10.0"></a>
## [1.10.0](https://www.github.com/EntglDb/BLite/releases/tag/v1.10.0) (2026-02-24)

### Features

* advanced BLQL filters ([c9b8fb7](https://www.github.com/EntglDb/BLite/commit/c9b8fb72cc3015ffcabe0cff4c618077fb633024))
* **studio:** add macOS build job — osx-x64 + osx-arm64 .app bundle + .dmg ([224a770](https://www.github.com/EntglDb/BLite/commit/224a770730b26f5bc5d78b70ca81d00ff8e022e1))
* **website:** add macOS download links — Studio page + footer ([c5ddf99](https://www.github.com/EntglDb/BLite/commit/c5ddf99f6c7cf98d7fd79618b3546c136e8afdb2))

### Bug Fixes

* corrects windows BLite Studio pipeline ([07a0266](https://www.github.com/EntglDb/BLite/commit/07a026671adac4bf810b2590fe3b6095d87518a2))
* **studio:** absolute SourceDir path + WixUI_FeatureTree with optional shortcuts and launch checkbox ([f3e56e8](https://www.github.com/EntglDb/BLite/commit/f3e56e828077438e3eb5cf543b4714427e6f3339))
* **studio:** correct WiX 4 Files syntax inside Component ([5442c60](https://www.github.com/EntglDb/BLite/commit/5442c60794f23c54172d06cc7b9c770b81d8d4d2))
* **studio:** fix WiX 4 Files harvesting inside Directory instead of ComponentGroup ([614a83b](https://www.github.com/EntglDb/BLite/commit/614a83baa171a781c549fdfc9dba63fe0d538cd0))
* **studio:** WiX SetProperty launch + LicenseRtf absolute path ([ef598e4](https://www.github.com/EntglDb/BLite/commit/ef598e465bc968d3cbe29a2aba44aaf2206f1296))

<a name="1.9.0"></a>
## [1.9.0](https://www.github.com/EntglDb/BLite/releases/tag/v1.9.0) (2026-02-24)

### Features

* BLite Studio ([11c6eb2](https://www.github.com/EntglDb/BLite/commit/11c6eb200e61154cdf23715584c89a968d507949))
* BLQL BLite Query Language ([a86d458](https://www.github.com/EntglDb/BLite/commit/a86d4584cf87136ed00876d16512a85a2367a213))

<a name="1.8.0"></a>
## [1.8.0](https://www.github.com/EntglDb/BLite/releases/tag/v1.8.0) (2026-02-24)

### Features

* **storage:** add hot backup support (BackupAsync) ([8a608bc](https://www.github.com/EntglDb/BLite/commit/8a608bc4104e11a4954d5b0339cee19841ff1c28))

### Bug Fixes

* ListCollections() now includes persisted collections (warm-up from storage catalog) ([64f7abb](https://www.github.com/EntglDb/BLite/commit/64f7abb32cda7694cf2354fcc177cbfc43902ce9))

<a name="1.7.1"></a>
## [1.7.1](https://www.github.com/EntglDb/BLite/releases/tag/v1.7.1) (2026-02-23)

### Bug Fixes

* corrects array document write to avoid key dictionary lookup ([7ac36a7](https://www.github.com/EntglDb/BLite/commit/7ac36a76c99b85b786f7eec744415cbc6dfdb509))

<a name="1.7.0"></a>
## [1.7.0](https://www.github.com/EntglDb/BLite/releases/tag/v1.7.0) (2026-02-23)

### Features

* **bson:** BsonJsonConverter - JSON<->BSON conversion in BLite.Bson ([c2e295c](https://www.github.com/EntglDb/BLite/commit/c2e295c60149feaad2cfb95a009a34b45f2ec4a2))

### Bug Fixes

* nested objects with [Key] Id + enum index support ([e6a334a](https://www.github.com/EntglDb/BLite/commit/e6a334a6d4bc155adb69e3d340f93ba39c2cc95a))

<a name="1.6.2"></a>
## [1.6.2](https://www.github.com/EntglDb/BLite/releases/tag/v1.6.2) (2026-02-23)

### Bug Fixes

* allows document metadata page overflow ([c663462](https://www.github.com/EntglDb/BLite/commit/c663462cdaf25868282678103a349ad07fb5487d))

<a name="1.6.1"></a>
## [1.6.1](https://www.github.com/EntglDb/BLite/releases/tag/v1.6.1) (2026-02-22)

### Bug Fixes

* add enum support to Source Generator serialization ([7096950](https://www.github.com/EntglDb/BLite/commit/709695028aeb9633be14038b93626dfcfcc8756d))

<a name="1.6.0"></a>
## [1.6.0](https://www.github.com/EntglDb/BLite/releases/tag/v1.6.0) (2026-02-22)

### Features

* expose RegisterKeys/GetKeyMap publicly on BLiteEngine ([cb9c6c9](https://www.github.com/EntglDb/BLite/commit/cb9c6c9703abc1608beb387d45f9fb60a2cc7cda))

<a name="1.5.0"></a>
## [1.5.0](https://www.github.com/EntglDb/BLite/releases/tag/v1.5.0) (2026-02-22)

### Features

* add FindAsync(predicate, ct) to DocumentCollection ([ee9566a](https://www.github.com/EntglDb/BLite/commit/ee9566a4f286ad47e53d4c41b2b8b88d1d959cb6))
* extend push-down to WHERE+SELECT combined queries ([ea4feaf](https://www.github.com/EntglDb/BLite/commit/ea4feaf9a6950f340ef19d240f221e4c1f91f59c))
* IBLiteQueryable<T> + async LINQ chain preservation (Step 1) ([c1926bb](https://www.github.com/EntglDb/BLite/commit/c1926bbe5f9e6a2162c98fa5e662343bfa871469))
* push-down SELECT projection at storage level (Steps 2-5) ([df7cceb](https://www.github.com/EntglDb/BLite/commit/df7ccebee1904369c552d26679f92c59dea7a2ba))
* symmetric CRUD API for DynamicCollection and BLiteEngine ([42cb68c](https://www.github.com/EntglDb/BLite/commit/42cb68cbeae7dec02b7ee2ffbf949eb05d6059da))

### Bug Fixes

* restore IAsyncEnumerable<T> on BTreeQueryable<T> ([444a96a](https://www.github.com/EntglDb/BLite/commit/444a96a6db043e9bc8a60b6e6b5278fbda4e3dba))

<a name="1.4.0"></a>
## [1.4.0](https://www.github.com/EntglDb/BLite/releases/tag/v1.4.0) (2026-02-21)

### Features

* async read infrastructure + BTreeIndex async API + concurrency fix ([0636221](https://www.github.com/EntglDb/BLite/commit/063622108a4af7d4ccd1499d47b20abf35a5311b))
* async read path for DocumentCollection, DynamicCollection, BLiteEngine and AsQueryable ([71266a7](https://www.github.com/EntglDb/BLite/commit/71266a7aeab8c595a7a9a8fb1f44606c3001084a))
* BLiteEngine for generic access to data ([39e6c89](https://www.github.com/EntglDb/BLite/commit/39e6c89971597700e585b7e3d2ea47b0e3a1a692))

<a name="1.3.1"></a>
## [1.3.1](https://www.github.com/EntglDb/BLite/releases/tag/v1.3.1) (2026-02-19)

### Bug Fixes

* generate SerializeFields and Deserialize(ref) for all entities ([69555cd](https://www.github.com/EntglDb/BLite/commit/69555cd16e11f0bf69b73b503ee3bb8e6511f9b5))

<a name="1.3.0"></a>
## [1.3.0](https://www.github.com/EntglDb/BLite/releases/tag/v1.3.0) (2026-02-19)

### Features

* **bson:** add support for DateTimeOffset, TimeSpan, DateOnly and TimeOnly ([43f92a9](https://www.github.com/EntglDb/BLite/commit/43f92a9f0954135ebeab276eabb1668f20cdc399))

<a name="1.2.1"></a>
## [1.2.1](https://www.github.com/EntglDb/BLite/releases/tag/v1.2.1) (2026-02-19)

### Bug Fixes

* **source-generator:** handle nullable string Id in mapper base class selection ([a12bb94](https://www.github.com/EntglDb/BLite/commit/a12bb94c3a99c76d52ed3277e7dec6c05d373477))

<a name="1.2.0"></a>
## [1.2.0](https://www.github.com/EntglDb/BLite/releases/tag/v1.2.0) (2026-02-19)

### Features

* **source-generator:** add circular reference and N-N relationship tests ([d48c8fc](https://www.github.com/EntglDb/BLite/commit/d48c8fc471c9428fdf9b44ce8f8cf194d1c4dbfd))
* **source-generator:** add private setter/constructor support via Expression Trees ([3d017ba](https://www.github.com/EntglDb/BLite/commit/3d017ba9d8335e8f3ba103872e3740423a3dc414))
* **source-generator:** improve property and collection analysis ([9506d1f](https://www.github.com/EntglDb/BLite/commit/9506d1fb2a76e3f563ca930503fde3365e98d399))

<a name="1.1.1"></a>
## [1.1.1](https://www.github.com/EntglDb/BLite/releases/tag/v1.1.1) (2026-02-18)

### Bug Fixes

* inheritance of dbcontext for set method ([dd59f8f](https://www.github.com/EntglDb/BLite/commit/dd59f8f621df4874709d0dd8fda91e895ab01920))

<a name="1.1.0"></a>
## [1.1.0](https://www.github.com/EntglDb/BLite/releases/tag/v1.1.0) (2026-02-18)

### Features

* adds Set<TId, T> to DocumentDbContext ([f3a6b5d](https://www.github.com/EntglDb/BLite/commit/f3a6b5d645cc1f5fb0d0526307a81cd730d3fb4e))

<a name="1.0.5"></a>
## [1.0.5](https://www.github.com/EntglDb/BLite/releases/tag/v1.0.5) (2026-02-17)

<a name="1.0.4"></a>
## [1.0.4](https://www.github.com/EntglDb/BLite/releases/tag/v1.0.4) (2026-02-17)

### Bug Fixes

* **source-generators:** correct ref struct handling for nested object serialization ([079d8c0](https://www.github.com/EntglDb/BLite/commit/079d8c0a5acb6f24820fc0f01f4be2c8fe6c8bb6))

<a name="1.0.3"></a>
## [1.0.3](https://www.github.com/EntglDb/BLite/releases/tag/v1.0.3) (2026-02-17)

### Bug Fixes

* add SemaphoreSlim to protect CurrentTransaction from race conditions ([f409e36](https://www.github.com/EntglDb/BLite/commit/f409e36ad43c9cc5291504d4ce990cd84afe80da))
* corrects mapper generation for nested objects and collections ([3382fc4](https://www.github.com/EntglDb/BLite/commit/3382fc466e1ab8dabb83851ca1cdf21ae7861b4a))
* handle cancellation token properly in BeginTransactionAsync ([394bf85](https://www.github.com/EntglDb/BLite/commit/394bf8552fe60648481b7ce30e8d02334a5b9ea5))

<a name="1.0.2"></a>
## [1.0.2](https://www.github.com/EntglDb/BLite/releases/tag/v1.0.2) (2026-02-16)

### Bug Fixes

* corrects nuget publish info ([d73016a](https://www.github.com/EntglDb/BLite/commit/d73016a3699af84067f820bb0b1e5b59f9b26edc))

