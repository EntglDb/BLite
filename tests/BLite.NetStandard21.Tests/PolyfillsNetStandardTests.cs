#if NETSTANDARD2_1
using System.Collections.Generic;

namespace BLite.NetStandard21.Tests;

public class PolyfillsNetStandardTests
{
    [Fact]
    public void PriorityQueue_Enqueue_Dequeue_ReturnsMinFirst()
    {
        var pq = new PriorityQueue<string, int>();
        pq.Enqueue("high", 10);
        pq.Enqueue("low", 1);
        pq.Enqueue("mid", 5);

        Assert.Equal("low", pq.Dequeue());
        Assert.Equal("mid", pq.Dequeue());
        Assert.Equal("high", pq.Dequeue());
    }

    [Fact]
    public void PriorityQueue_TryPeek_DoesNotRemove()
    {
        var pq = new PriorityQueue<string, int>();
        pq.Enqueue("item", 1);

        Assert.True(pq.TryPeek(out var element, out var priority));
        Assert.Equal("item", element);
        Assert.Equal(1, priority);

        // Count should still be 1 after TryPeek
        Assert.Equal(1, pq.Count);
        Assert.Equal("item", pq.Dequeue());
    }

    [Fact]
    public void PriorityQueue_Dequeue_EmptyQueue_Throws()
    {
        var pq = new PriorityQueue<string, int>();
        Assert.Throws<InvalidOperationException>(() => pq.Dequeue());
    }

    [Fact]
    public void PriorityQueue_MultipleElements_OrderedCorrectly()
    {
        var pq = new PriorityQueue<int, int>();
        var priorities = new[] { 7, 3, 9, 1, 5, 2, 8, 4, 6, 0 };
        foreach (var p in priorities)
            pq.Enqueue(p, p);

        var result = new List<int>();
        while (pq.Count > 0)
            result.Add(pq.Dequeue());

        for (int i = 0; i < result.Count - 1; i++)
            Assert.True(result[i] <= result[i + 1],
                $"Expected ascending order but got {result[i]} before {result[i + 1]}");
    }
}
#endif
