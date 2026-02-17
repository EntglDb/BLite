using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace BLite.Core.CDC;

internal sealed class ChangeStreamDispatcher : IDisposable
{
    private readonly Channel<InternalChangeEvent> _channel;
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<ChannelWriter<InternalChangeEvent>, byte>> _subscriptions = new();
    private readonly ConcurrentDictionary<string, int> _payloadWatcherCounts = new();
    private readonly CancellationTokenSource _cts = new();

    public ChangeStreamDispatcher()
    {
        _channel = Channel.CreateUnbounded<InternalChangeEvent>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });

        Task.Run(ProcessEventsAsync);
    }

    public void Publish(InternalChangeEvent change)
    {
        _channel.Writer.TryWrite(change);
    }

    public bool HasPayloadWatchers(string collectionName)
    {
        return _payloadWatcherCounts.TryGetValue(collectionName, out var count) && count > 0;
    }

    public bool HasAnyWatchers(string collectionName)
    {
        return _subscriptions.TryGetValue(collectionName, out var subs) && !subs.IsEmpty;
    }

    public IDisposable Subscribe(string collectionName, bool capturePayload, ChannelWriter<InternalChangeEvent> writer)
    {
        if (capturePayload)
        {
            _payloadWatcherCounts.AddOrUpdate(collectionName, 1, (_, count) => count + 1);
        }

        var collectionSubs = _subscriptions.GetOrAdd(collectionName, _ => new ConcurrentDictionary<ChannelWriter<InternalChangeEvent>, byte>());
        collectionSubs.TryAdd(writer, 0);

        return new Subscription(() => Unsubscribe(collectionName, capturePayload, writer));
    }

    private void Unsubscribe(string collectionName, bool capturePayload, ChannelWriter<InternalChangeEvent> writer)
    {
        if (_subscriptions.TryGetValue(collectionName, out var collectionSubs))
        {
            collectionSubs.TryRemove(writer, out _);
        }

        if (capturePayload)
        {
            _payloadWatcherCounts.AddOrUpdate(collectionName, 0, (_, count) => Math.Max(0, count - 1));
        }
    }

    private async Task ProcessEventsAsync()
    {
        try
        {
            var reader = _channel.Reader;
            while (await reader.WaitToReadAsync(_cts.Token))
            {
                while (reader.TryRead(out var @event))
                {
                    if (_subscriptions.TryGetValue(@event.CollectionName, out var collectionSubs))
                    {
                        foreach (var writer in collectionSubs.Keys)
                        {
                            // Optimized fan-out: non-blocking TryWrite.
                            // If a subscriber channel is full (unlikely with Unbounded), 
                            // we skip or drop. Usually, subscribers will also use Unbounded.
                            writer.TryWrite(@event);
                        }
                    }
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception)
        {
            // Internal error logging could go here
        }
    }

    public void Dispose()
    {
        _cts.Cancel();
        _cts.Dispose();
    }

    private sealed class Subscription : IDisposable
    {
        private readonly Action _onDispose;
        private bool _disposed;

        public Subscription(Action onDispose)
        {
            _onDispose = onDispose;
        }

        public void Dispose()
        {
            if (_disposed) return;
            _onDispose();
            _disposed = true;
        }
    }
}
