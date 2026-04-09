using System.Threading.Channels;

namespace BLite.Core.Metrics;

/// <summary>
/// An <see cref="IObservable{T}"/> that emits a <see cref="MetricsSnapshot"/> at a
/// fixed interval by polling the underlying <see cref="MetricsDispatcher"/>.
///
/// Usage:
/// <code>
/// using var sub = engine.WatchMetrics(TimeSpan.FromSeconds(5))
///     .Subscribe(snapshot => Console.WriteLine(snapshot.TransactionCommitsTotal));
/// </code>
/// </summary>
internal sealed class BLiteMetricsObservable : IObservable<MetricsSnapshot>
{
    private readonly MetricsDispatcher _dispatcher;
    private readonly TimeSpan _interval;

    public BLiteMetricsObservable(MetricsDispatcher dispatcher, TimeSpan interval)
    {
        _dispatcher = dispatcher ?? throw new ArgumentNullException(nameof(dispatcher));
        if (interval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(interval), "Interval must be positive.");
        _interval = interval;
    }

    public IDisposable Subscribe(IObserver<MetricsSnapshot> observer)
    {
        if (observer == null) throw new ArgumentNullException(nameof(observer));
        return new Subscription(_dispatcher, _interval, observer);
    }

    // ── Subscription ────────────────────────────────────────────────────────

    private sealed class Subscription : IDisposable
    {
        private readonly CancellationTokenSource _cts = new();
        private readonly Task _task;

        public Subscription(MetricsDispatcher dispatcher, TimeSpan interval, IObserver<MetricsSnapshot> observer)
        {
            _task = Task.Run(() => RunAsync(dispatcher, interval, observer, _cts.Token));
        }

        private static async Task RunAsync(
            MetricsDispatcher dispatcher,
            TimeSpan interval,
            IObserver<MetricsSnapshot> observer,
            CancellationToken ct)
        {
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    await Task.Delay(interval, ct).ConfigureAwait(false);
                    observer.OnNext(dispatcher.GetSnapshot());
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

        public void Dispose()
        {
            _cts.Cancel();
            _cts.Dispose();
        }
    }
}
