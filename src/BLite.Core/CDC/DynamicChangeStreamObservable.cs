// BLite.Core — DynamicChangeStreamObservable
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Schema-less variant of ChangeStreamObservable<TId,T>.
// Subscribes to a ChangeStreamDispatcher and emits BsonChangeEvent
// without deserializing to a typed entity T.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using BLite.Bson;
using BLite.Core.GDPR;

namespace BLite.Core.CDC;

internal sealed class DynamicChangeStreamObservable : IObservable<BsonChangeEvent>
{
    // Tracks collections for which the GDPR "no personal-data metadata" advisory has
    // already been logged at least once, to satisfy the "log exactly once per collection"
    // requirement from WP2.
    private static readonly ConcurrentDictionary<string, byte> _warnedCollections = new(StringComparer.OrdinalIgnoreCase);

    private readonly ChangeStreamDispatcher _dispatcher;
    private readonly string _collectionName;
    private readonly WatchOptions _options;
    private readonly ConcurrentDictionary<ushort, string> _keyReverseMap;
    private readonly ConcurrentDictionary<string, ushort>? _forwardKeyMap;

    public DynamicChangeStreamObservable(
        ChangeStreamDispatcher dispatcher,
        string collectionName,
        WatchOptions options,
        ConcurrentDictionary<ushort, string> keyReverseMap,
        ConcurrentDictionary<string, ushort>? forwardKeyMap = null)
    {
        _dispatcher = dispatcher;
        _collectionName = collectionName;
        _options = options;
        _keyReverseMap = keyReverseMap;
        _forwardKeyMap = forwardKeyMap;

        // Log an advisory once per collection when CapturePayload=true and
        // RevealPersonalData=false: personal-data metadata is unavailable for
        // dynamic (untyped) collections, so rule 2 of the masking pipeline is a no-op.
        // The consumer must use ExcludeFields / IncludeOnlyFields explicitly.
        if (options.CapturePayload && !options.RevealPersonalData
            && _warnedCollections.TryAdd(collectionName, 0))
        {
            Trace.TraceInformation(
                "[BLite CDC] Dynamic collection '{0}' is watched with CapturePayload=true and " +
                "RevealPersonalData=false, but no personal-data metadata is available for " +
                "untyped collections (rule 2 of the masking pipeline is a no-op). " +
                "Use ExcludeFields or IncludeOnlyFields to restrict the payload explicitly.",
                collectionName);
        }
    }

    public IDisposable Subscribe(IObserver<BsonChangeEvent> observer)
    {
        if (observer == null) throw new ArgumentNullException(nameof(observer));

        var cts = new CancellationTokenSource();
        var channel = Channel.CreateUnbounded<InternalChangeEvent>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true
        });

        var subscription = _dispatcher.Subscribe(_collectionName, _options.CapturePayload, channel.Writer);
        var bridgeTask = Task.Run(() => BridgeAsync(channel.Reader, observer, cts.Token));

        return new SubscriptionHandle(subscription, cts, channel.Writer, bridgeTask);
    }

    private async Task BridgeAsync(
        ChannelReader<InternalChangeEvent> reader,
        IObserver<BsonChangeEvent> observer,
        CancellationToken ct)
    {
        try
        {
            while (await reader.WaitToReadAsync(ct).ConfigureAwait(false))
            {
                while (reader.TryRead(out var e))
                {
                    BsonDocument? payload = null;
                    if (_options.CapturePayload && e.PayloadBytes.HasValue)
                    {
                        var rawDoc = new BsonDocument(e.PayloadBytes.Value.ToArray(), _keyReverseMap, _forwardKeyMap);

                        // For dynamic collections the personal-data field list is always empty,
                        // so rule 2 (MaskPersonalData) is a no-op.  ExcludeFields and
                        // IncludeOnlyFields still apply.
                        payload = _forwardKeyMap != null
                            ? PayloadMask.Apply(rawDoc, _options, Array.Empty<PersonalDataField>(), _forwardKeyMap, _keyReverseMap)
                            : rawDoc;
                    }

                    observer.OnNext(new BsonChangeEvent
                    {
                        Timestamp = e.Timestamp,
                        TransactionId = e.TransactionId,
                        CollectionName = e.CollectionName,
                        Type = e.Type,
                        IdType = e.IdType,
                        IdBytes = e.IdBytes,
                        Payload = payload
                    });
                }
            }
            observer.OnCompleted();
        }
        catch (OperationCanceledException)
        {
            observer.OnCompleted();
        }
        catch (Exception ex)
        {
            observer.OnError(ex);
        }
    }

    private sealed class SubscriptionHandle : IDisposable
    {
        private readonly IDisposable _dispatcherSub;
        private readonly CancellationTokenSource _cts;
        private readonly ChannelWriter<InternalChangeEvent> _writer;
        private readonly Task _bridgeTask;
        private bool _disposed;

        public SubscriptionHandle(
            IDisposable dispatcherSub,
            CancellationTokenSource cts,
            ChannelWriter<InternalChangeEvent> writer,
            Task bridgeTask)
        {
            _dispatcherSub = dispatcherSub;
            _cts = cts;
            _writer = writer;
            _bridgeTask = bridgeTask;
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            _dispatcherSub.Dispose();
            _cts.Cancel();
            _writer.TryComplete();
        }
    }

    /// <summary>
    /// Clears the static "already warned" collection set.
    /// For testing only — ensures each test sees a fresh warning state.
    /// </summary>
    internal static void ResetWarnedCollections() => _warnedCollections.Clear();
}
