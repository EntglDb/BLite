using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Threading;
using BLite.Bson;
using BLite.Core.Collections;
using BLite.Core.Indexing;

namespace BLite.Core.CDC;

internal sealed class ChangeStreamObservable<TId, T> : IObservable<ChangeStreamEvent<TId, T>> where T : class
{
    private readonly ChangeStreamDispatcher _dispatcher;
    private readonly string _collectionName;
    private readonly bool _capturePayload;
    private readonly IDocumentMapper<TId, T> _mapper;
    private readonly ConcurrentDictionary<ushort, string> _keyReverseMap;

    public ChangeStreamObservable(
        ChangeStreamDispatcher dispatcher, 
        string collectionName, 
        bool capturePayload,
        IDocumentMapper<TId, T> mapper,
        ConcurrentDictionary<ushort, string> keyReverseMap)
    {
        _dispatcher = dispatcher;
        _collectionName = collectionName;
        _capturePayload = capturePayload;
        _mapper = mapper;
        _keyReverseMap = keyReverseMap;
    }

    public IDisposable Subscribe(IObserver<ChangeStreamEvent<TId, T>> observer)
    {
        if (observer == null) throw new ArgumentNullException(nameof(observer));

        var cts = new CancellationTokenSource();
        var channel = Channel.CreateUnbounded<InternalChangeEvent>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true
        });

        var dispatcherSubscription = _dispatcher.Subscribe(_collectionName, _capturePayload, channel.Writer);

        // Background task to bridge Channel -> Observer
        var bridgeTask = Task.Run(() => BridgeChannelToObserverAsync(channel.Reader, observer, cts.Token));

        return new CompositeDisposable(dispatcherSubscription, cts, channel.Writer, bridgeTask);
    }

    private async Task BridgeChannelToObserverAsync(ChannelReader<InternalChangeEvent> reader, IObserver<ChangeStreamEvent<TId, T>> observer, CancellationToken ct)
    {
        try
        {
            while (await reader.WaitToReadAsync(ct))
            {
                while (reader.TryRead(out var internalEvent))
                {
                    try
                    {
                        // Deserializza ID
                        var eventId = _mapper.FromIndexKey(new IndexKey(internalEvent.IdBytes.ToArray()));
                        
                        // Deserializza Payload (se presente)
                        T? entity = default;
                        if (internalEvent.PayloadBytes.HasValue)
                        {
                            entity = _mapper.Deserialize(new BsonSpanReader(internalEvent.PayloadBytes.Value.Span, _keyReverseMap));
                        }

                        var externalEvent = new ChangeStreamEvent<TId, T>
                        {
                            Timestamp = internalEvent.Timestamp,
                            TransactionId = internalEvent.TransactionId,
                            CollectionName = internalEvent.CollectionName,
                            Type = internalEvent.Type,
                            DocumentId = eventId,
                            Entity = entity
                        };

                        observer.OnNext(externalEvent);
                    }
                    catch (Exception ex)
                    {
                        // In case of deserialization error, we notify and continue if possible
                        // Or we can stop the observer.
                        observer.OnError(ex);
                    }
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

    private sealed class CompositeDisposable : IDisposable
    {
        private readonly IDisposable _dispatcherSubscription;
        private readonly CancellationTokenSource _cts;
        private readonly ChannelWriter<InternalChangeEvent> _writer;
        private readonly Task _bridgeTask;
        private bool _disposed;

        public CompositeDisposable(IDisposable dispatcherSubscription, CancellationTokenSource cts, ChannelWriter<InternalChangeEvent> writer, Task bridgeTask)
        {
            _dispatcherSubscription = dispatcherSubscription;
            _cts = cts;
            _writer = writer;
            _bridgeTask = bridgeTask;
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            _dispatcherSubscription.Dispose();
            _writer.TryComplete();
            _cts.Cancel();
            _cts.Dispose();
        }
    }
}
