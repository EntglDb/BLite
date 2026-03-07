using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Text.Json;
using BLite.Core.KeyValue;
using Microsoft.Extensions.Caching.Distributed;

namespace BLite.Caching;

/// <summary>
/// <see cref="IDistributedCache"/> implementation backed by a BLite Key-Value store.
///
/// Supports absolute and sliding expiration. Typed get/set methods use System.Text.Json.
/// Uses per-key <see cref="SemaphoreSlim"/> locking in <see cref="GetOrSet{T}"/> to prevent
/// thundering-herd scenarios.
///
/// Wire format for stored values (enables sliding expiration without extra metadata storage):
/// <code>
/// [1 byte: 0x00 = no sliding | 0x01 = has sliding]
/// [8 bytes: sliding ticks — only present when byte 0 == 0x01]
/// [user payload bytes]
/// </code>
/// </summary>
public sealed class BLiteDistributedCache : IBLiteCache
{
    private readonly IBLiteKvStore _kv;
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _keyLocks =
        new(StringComparer.Ordinal);

    public BLiteDistributedCache(IBLiteKvStore kvStore)
    {
        _kv = kvStore;
    }

    // ── IDistributedCache ─────────────────────────────────────────────────────

    public byte[]? Get(string key)
    {
        var raw = _kv.Get(key);
        if (raw is null) return null;
        var (slidingTicks, payload) = Unwrap(raw);
        if (slidingTicks > 0)
            _kv.Refresh(key, TimeSpan.FromTicks(slidingTicks));
        return payload;
    }

    public void Set(string key, byte[] value, DistributedCacheEntryOptions options)
    {
        var (ttl, slidingTicks) = ComputeTtl(options);
        _kv.Set(key, Wrap(value, slidingTicks), ttl);
    }

    public void Refresh(string key)
    {
        // Read raw (not using IBLiteCache.Get to avoid double-refresh)
        var raw = _kv.Get(key);
        if (raw is null) return;
        var (slidingTicks, _) = Unwrap(raw);
        if (slidingTicks > 0)
            _kv.Refresh(key, TimeSpan.FromTicks(slidingTicks));
    }

    public void Remove(string key) => _kv.Delete(key);

    public Task<byte[]?> GetAsync(string key, CancellationToken token = default)
        => Task.FromResult(Get(key));

    public Task SetAsync(string key, byte[] value, DistributedCacheEntryOptions options,
        CancellationToken token = default)
    {
        Set(key, value, options);
        return Task.CompletedTask;
    }

    public Task RefreshAsync(string key, CancellationToken token = default)
    {
        Refresh(key);
        return Task.CompletedTask;
    }

    public Task RemoveAsync(string key, CancellationToken token = default)
    {
        Remove(key);
        return Task.CompletedTask;
    }

    // ── IBLiteCache typed helpers ─────────────────────────────────────────────

    public T? Get<T>(string key) where T : class
    {
        var bytes = Get(key);
        return bytes is null ? null : JsonSerializer.Deserialize<T>(bytes);
    }

    public void Set<T>(string key, T value, DistributedCacheEntryOptions? options = null)
        where T : class
        => Set(key, JsonSerializer.SerializeToUtf8Bytes(value),
               options ?? new DistributedCacheEntryOptions());

    public Task<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default)
        where T : class
        => Task.FromResult(Get<T>(key));

    public Task SetAsync<T>(string key, T value, DistributedCacheEntryOptions? options = null,
        CancellationToken cancellationToken = default) where T : class
    {
        Set(key, value, options);
        return Task.CompletedTask;
    }

    public T GetOrSet<T>(string key, Func<T> factory, DistributedCacheEntryOptions? options = null)
        where T : class
    {
        var cached = Get<T>(key);
        if (cached is not null) return cached;

        var sem = _keyLocks.GetOrAdd(key, static _ => new SemaphoreSlim(1, 1));
        sem.Wait();
        try
        {
            cached = Get<T>(key);
            if (cached is not null) return cached;
            var value = factory();
            Set(key, value, options);
            return value;
        }
        finally { sem.Release(); }
    }

    public async Task<T> GetOrSetAsync<T>(string key, Func<Task<T>> factory,
        DistributedCacheEntryOptions? options = null,
        CancellationToken cancellationToken = default) where T : class
    {
        var cached = await GetAsync<T>(key, cancellationToken);
        if (cached is not null) return cached;

        var sem = _keyLocks.GetOrAdd(key, static _ => new SemaphoreSlim(1, 1));
        await sem.WaitAsync(cancellationToken);
        try
        {
            cached = await GetAsync<T>(key, cancellationToken);
            if (cached is not null) return cached;
            var value = await factory();
            await SetAsync(key, value, options, cancellationToken);
            return value;
        }
        finally { sem.Release(); }
    }

    // ── Envelope encoding ─────────────────────────────────────────────────────

    private static byte[] Wrap(ReadOnlySpan<byte> payload, long slidingTicks)
    {
        if (slidingTicks > 0)
        {
            var result = new byte[9 + payload.Length];
            result[0] = 0x01;
            BinaryPrimitives.WriteInt64LittleEndian(result.AsSpan(1), slidingTicks);
            payload.CopyTo(result.AsSpan(9));
            return result;
        }

        var plain = new byte[1 + payload.Length];
        plain[0] = 0x00;
        payload.CopyTo(plain.AsSpan(1));
        return plain;
    }

    private static (long SlidingTicks, byte[] Payload) Unwrap(byte[] raw)
    {
        if (raw.Length == 0) return (0, raw);
        if (raw[0] == 0x01 && raw.Length >= 9)
            return (BinaryPrimitives.ReadInt64LittleEndian(raw.AsSpan(1)), raw[9..]);
        return (0, raw.Length > 1 ? raw[1..] : Array.Empty<byte>());
    }

    private static (TimeSpan? Ttl, long SlidingTicks) ComputeTtl(DistributedCacheEntryOptions? opts)
    {
        if (opts is null) return (null, 0);

        // Sliding expiration: initial window = sliding duration; refreshed on each Get
        if (opts.SlidingExpiration.HasValue)
            return (opts.SlidingExpiration.Value, opts.SlidingExpiration.Value.Ticks);

        if (opts.AbsoluteExpiration.HasValue)
        {
            var ttl = opts.AbsoluteExpiration.Value - DateTimeOffset.UtcNow;
            return (ttl > TimeSpan.Zero ? ttl : TimeSpan.FromMilliseconds(1), 0);
        }

        if (opts.AbsoluteExpirationRelativeToNow.HasValue)
            return (opts.AbsoluteExpirationRelativeToNow.Value, 0);

        return (null, 0);
    }
}
