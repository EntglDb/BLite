# Change Log

All notable changes to this project will be documented in this file. See [versionize](https://github.com/versionize/versionize) for commit guidelines.

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

