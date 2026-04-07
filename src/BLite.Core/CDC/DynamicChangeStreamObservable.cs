// BLite.Core — DynamicChangeStreamObservable
// Copyright (C) 2026 Luca Fabbri — AGPL-3.0
//
// Schema-less variant of ChangeStreamObservable<TId,T>.
// Subscribes to a ChangeStreamDispatcher and emits BsonChangeEvent
// without deserializing to a typed entity T.

using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using BLite.Bson;

namespace BLite.Core.CDC;

internal sealed class DynamicChangeStreamObservable : IObservable<BsonChangeEvent>
{
    private readonly ChangeStreamDispatcher _dispatcher;
    private readonly string _collectionName;
    private readonly bool _capturePayload;
    private readonly ConcurrentDictionary<ushort, string> _keyReverseMap;
    private readonly ConcurrentDictionary<string, ushort>? _forwardKeyMap;

    public DynamicChangeStreamObservable(
        ChangeStreamDispatcher dispatcher,
        string collectionName,
        bool capturePayload,
        ConcurrentDictionary<ushort, string> keyReverseMap,
        ConcurrentDictionary<string, ushort>? forwardKeyMap = null)
    {
        _dispatcher = dispatcher;
        _collectionName = collectionName;
        _capturePayload = capturePayload;
        _keyReverseMap = keyReverseMap;
        _forwardKeyMap = forwardKeyMap;
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

        var subscription = _dispatcher.Subscribe(_collectionName, _capturePayload, channel.Writer);
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
                    if (e.PayloadBytes.HasValue)
                        payload = new BsonDocument(e.PayloadBytes.Value.ToArray(), _keyReverseMap, _forwardKeyMap);

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
}
