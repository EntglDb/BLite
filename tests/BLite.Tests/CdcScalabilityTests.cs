using BLite.Shared;

namespace BLite.Tests;

public class CdcScalabilityTests : IDisposable
{
    private readonly TestDbContext _db;
    private readonly string _dbPath;

    public CdcScalabilityTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"cdc_scaling_{Guid.NewGuid()}.db");
        _db = new TestDbContext(_dbPath);
    }

    [Fact]
    public async Task Test_Cdc_1000_Subscribers_Receive_Events()
    {
        const int SubscriberCount = 1000;
        var eventCounts = new int[SubscriberCount];
        var subscriptions = new List<IDisposable>();

        // 1. Create 1000 subscribers
        for (int i = 0; i < SubscriberCount; i++)
        {
            int index = i;
            var sub = _db.People.Watch().Subscribe(_ => 
            {
                Interlocked.Increment(ref eventCounts[index]);
            });
            subscriptions.Add(sub);
        }

        // 2. Perform some writes
        _db.People.Insert(new Person { Id = 1, Name = "John", Age = 30 });
        _db.People.Insert(new Person { Id = 2, Name = "Jane", Age = 25 });
        _db.SaveChanges();

        // 3. Wait for events to propagate
        await Task.Delay(1000);

        // 4. Verify all subscribers received both events
        for (int i = 0; i < SubscriberCount; i++)
        {
            Assert.Equal(2, eventCounts[i]);
        }

        foreach (var sub in subscriptions) sub.Dispose();
    }

    [Fact(Skip="Performance test - run manually when needed")]
    public async Task Test_Cdc_Slow_Subscriber_Does_Not_Block_Others()
    {
        var fastEventCount = 0;
        var slowEventCount = 0;

        // 1. Register a slow subscriber that blocks SYNCHRONOUSLY
        using var slowSub = _db.People.Watch().Subscribe(_ => 
        {
            Interlocked.Increment(ref slowEventCount);
            // Synchronous block to block the BridgeChannelToObserverAsync loop for this sub
            Thread.Sleep(2000); 
        });

        // 2. Register a fast subscriber
        using var fastSub = _db.People.Watch().Subscribe(_ => 
        {
            Interlocked.Increment(ref fastEventCount);
        });

        // 3. Perform a write
        _db.People.Insert(new Person { Id = 1, Name = "John", Age = 30 });
        _db.SaveChanges();

        // 4. Verification: Fast subscriber should receive it immediately
        await Task.Delay(200);
        Assert.Equal(1, fastEventCount);
        Assert.Equal(1, slowEventCount); // Started but not finished or blocking others

        // 5. Perform another write
        _db.People.Insert(new Person { Id = 2, Name = "Jane", Age = 25 });
        _db.SaveChanges();

        // 6. Verification: Fast subscriber should receive second event while slow one is still busy
        await Task.Delay(200);
        Assert.Equal(2, fastEventCount);
        Assert.Equal(1, slowEventCount); // Still processing first one or second one queued in private channel

        // 7. Wait for slow one to eventually catch up
        await Task.Delay(2500); // Wait for the second one in slow sub to be processed after the first Sleep
        Assert.Equal(2, slowEventCount);
    }

    public void Dispose()
    {
        _db.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var wal = Path.ChangeExtension(_dbPath, ".wal");
        if (File.Exists(wal)) File.Delete(wal);
    }
}
