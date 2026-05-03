using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Threading;
using BLite.Bson;
using BLite.Core.Collections;
using BLite.Core.GDPR;
using BLite.Core.Indexing;

namespace BLite.Core.CDC;

internal sealed class ChangeStreamObservable<TId, T> : IObservable<ChangeStreamEvent<TId, T>> where T : class
{
    private readonly ChangeStreamDispatcher _dispatcher;
    private readonly string _collectionName;
    private readonly WatchOptions _options;
    private readonly IDocumentMapper<TId, T> _mapper;
    private readonly ConcurrentDictionary<ushort, string> _keyReverseMap;
    private readonly IReadOnlyDictionary<string, ushort> _forwardKeyMap;
    private readonly IReadOnlyList<PersonalDataField> _personalDataFields;

    public ChangeStreamObservable(
        ChangeStreamDispatcher dispatcher,
        string collectionName,
        WatchOptions options,
        IDocumentMapper<TId, T> mapper,
        ConcurrentDictionary<ushort, string> keyReverseMap,
        IReadOnlyDictionary<string, ushort> forwardKeyMap,
        IReadOnlyList<PersonalDataField> personalDataFields)
    {
        _dispatcher = dispatcher;
        _collectionName = collectionName;
        _options = options;
        _mapper = mapper;
        _keyReverseMap = keyReverseMap;
        _forwardKeyMap = forwardKeyMap;
        _personalDataFields = personalDataFields;
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

        var dispatcherSubscription = _dispatcher.Subscribe(_collectionName, _options.CapturePayload, channel.Writer);

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
                        // Deserialise ID
                        var eventId = _mapper.FromIndexKey(IndexKey.FromOwnedArray(internalEvent.IdBytes.ToArray()));

                        // Deserialise payload (only when capture was requested and bytes are present).
                        T? entity = default;
                        if (_options.CapturePayload && internalEvent.PayloadBytes.HasValue)
                        {
                            // Clone the payload as a BsonDocument, apply the masking pipeline,
                            // then deserialise from the (possibly masked) bytes.
                            var rawDoc = new BsonDocument(
                                internalEvent.PayloadBytes.Value.ToArray(),
                                _keyReverseMap);

                            var maskedDoc = PayloadMask.Apply(
                                rawDoc,
                                _options,
                                _personalDataFields,
                                _forwardKeyMap,
                                _keyReverseMap);

                            entity = _mapper.Deserialize(
                                new BsonSpanReader(maskedDoc.RawData.Span, _keyReverseMap));
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
                        // In case of deserialization error, notify and continue if possible.
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
