using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using BLite.Bson;
using BLite.Core.CDC;
using BLite.Core.Transactions;
using BLite.Shared;
using Xunit;

namespace BLite.Tests;

public class CdcTests : IDisposable
{
    private readonly string _dbPath = $"cdc_test_{Guid.NewGuid()}.db";
    private readonly TestDbContext _db;

    public CdcTests()
    {
        _db = new TestDbContext(_dbPath);
    }

    [Fact]
    public async Task Test_Cdc_Basic_Insert_Fires_Event()
    {
        var events = new List<ChangeStreamEvent<int, Person>>();
        using var subscription = _db.People.Watch(capturePayload: true).Subscribe(e => events.Add(e));

        var person = new Person { Id = 1, Name = "John", Age = 30 };
        _db.People.Insert(person);
        _db.SaveChanges();

        // Wait for event (it's async via Channel)
        await Task.Delay(200);

        Assert.Single(events);
        Assert.Equal(OperationType.Insert, events[0].Type);
        Assert.Equal(1, events[0].DocumentId);
        Assert.NotNull(events[0].Entity);
        Assert.Equal("John", events[0].Entity!.Name);
    }

    [Fact]
    public async Task Test_Cdc_No_Payload_When_Not_Requested()
    {
        var events = new List<ChangeStreamEvent<int, Person>>();
        using var subscription = _db.People.Watch(capturePayload: false).Subscribe(e => events.Add(e));

        var person = new Person { Id = 1, Name = "John", Age = 30 };
        _db.People.Insert(person);
        _db.SaveChanges();

        await Task.Delay(200);

        Assert.Single(events);
        Assert.Null(events[0].Entity);
    }

    [Fact]
    public async Task Test_Cdc_Commit_Only()
    {
        var events = new List<ChangeStreamEvent<int, Person>>();
        using var subscription = _db.People.Watch(capturePayload: true).Subscribe(e => events.Add(e));

        using (var txn = _db.BeginTransaction())
        {
            _db.People.Insert(new Person { Id = 1, Name = "John" });
            await Task.Delay(50);
            Assert.Empty(events); // Not committed yet

            txn.Rollback();
        }

        await Task.Delay(200);
        Assert.Empty(events); // Rolled back

        using (var txn = _db.BeginTransaction())
        {
            _db.People.Insert(new Person { Id = 2, Name = "Jane" });
            _db.SaveChanges();
        }

        await Task.Delay(200);
        Assert.Single(events);
        Assert.Equal(2, events[0].DocumentId);
    }

    [Fact]
    public async Task Test_Cdc_Update_And_Delete()
    {
        var events = new List<ChangeStreamEvent<int, Person>>();
        using var subscription = _db.People.Watch(capturePayload: true).Subscribe(e => events.Add(e));

        var person = new Person { Id = 1, Name = "John", Age = 30 };
        _db.People.Insert(person);
        _db.SaveChanges();

        person.Name = "Johnny";
        _db.People.Update(person);
        _db.SaveChanges();

        _db.People.Delete(1);
        _db.SaveChanges();

        await Task.Delay(300);

        Assert.Equal(3, events.Count);
        Assert.Equal(OperationType.Insert, events[0].Type);
        Assert.Equal(OperationType.Update, events[1].Type);
        Assert.Equal(OperationType.Delete, events[2].Type);
        
        Assert.Equal("Johnny", events[1].Entity!.Name);
        Assert.Equal(1, events[2].DocumentId);
    }

    public void Dispose()
    {
        _db.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        if (File.Exists(_dbPath + "-wal")) File.Delete(_dbPath + "-wal");
    }
}

// Simple helper to avoid System.Reactive dependency in tests
public static class ObservableExtensions
{
    public static IDisposable Subscribe<T>(this IObservable<T> observable, Action<T> onNext)
    {
        return observable.Subscribe(new AnonymousObserver<T>(onNext));
    }

    private class AnonymousObserver<T> : IObserver<T>
    {
        private readonly Action<T> _onNext;
        public AnonymousObserver(Action<T> onNext) => _onNext = onNext;
        public void OnCompleted() { }
        public void OnError(Exception error) { }
        public void OnNext(T value) => _onNext(value);
    }
}
