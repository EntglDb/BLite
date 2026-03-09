# Change Log

All notable changes to this project will be documented in this file. See [versionize](https://github.com/versionize/versionize) for commit guidelines.

<a name="3.3.0"></a>
## [3.3.0](https://www.github.com/EntglDb/BLite/releases/tag/v3.3.0) (2026-03-09)

### Features

* page compaction on delete — intra-page space recovered on every delete; freed bytes immediately reusable ([63213be](https://www.github.com/EntglDb/BLite/commit/63213bec4ad8e47cdf3014d9408b939f8a38352a))
* typed TimeSeries in DocumentDbContext — `HasTimeSeries()` fluent API on `EntityTypeBuilder<T>`; `ForcePrune()` on `DocumentCollection<TId,T>` ([63213be](https://www.github.com/EntglDb/BLite/commit/63213bec4ad8e47cdf3014d9408b939f8a38352a))
* private-setter entity support verified with dedicated tests (`FullyPrivateEntity`) ([63213be](https://www.github.com/EntglDb/BLite/commit/63213bec4ad8e47cdf3014d9408b939f8a38352a))

<a name="3.2.0"></a>
## [3.2.0](https://www.github.com/EntglDb/BLite/releases/tag/v3.2.0) (2026-03-07)

### Features

* embedded Key-Value store + BLite.Caching ([b028cfb](https://www.github.com/EntglDb/BLite/commit/b028cfb20f2ee39f660152d2a37c561722a4a5a3))

