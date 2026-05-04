using BLite.Core.GDPR;

namespace BLite.Core.KeyValue;

/// <summary>
/// Configuration options for the embedded Key-Value store.
/// </summary>
public sealed class BLiteKvOptions
{
    /// <summary>Shared default instance with no options set.</summary>
    public static readonly BLiteKvOptions Default = new();

    /// <summary>
    /// Engine-wide GDPR enforcement profile applied to every collection.
    /// When using <see cref="BLite.Core.DocumentDbContext"/>, per-collection overrides set
    /// via <c>EntityTypeBuilder&lt;T&gt;.HasGdprMode()</c> or the <c>[GdprMode]</c> attribute
    /// are resolved at model-building time; if any entity carries <c>Strict</c>, the whole
    /// context is validated as Strict regardless of this value.
    /// Defaults to <see cref="GdprMode.None"/> (backwards-compatible — no enforcement).
    /// </summary>
    public GdprMode DefaultGdprMode { get; init; } = GdprMode.None;

    /// <summary>
    /// Default TTL applied to entries when <see cref="IBLiteKvStore.Set"/> is called without
    /// an explicit TTL. <c>null</c> (default) means entries never expire.
    /// </summary>
    public TimeSpan? DefaultTtl { get; init; }

    /// <summary>
    /// When <c>true</c>, <see cref="IBLiteKvStore.PurgeExpired"/> is invoked automatically
    /// when the store is opened, removing all previously-expired entries before any reads.
    /// Default: <c>false</c> — expiry is lazy (entries removed on first stale access).
    /// </summary>
    public bool PurgeExpiredOnOpen { get; init; }
}
