using BLite.Core.Storage;

namespace BLite.Tests
{
    /// <summary>
    /// Regression coverage for a torn-read bug in PageFile.WritePage's non-growing fast
    /// path: it took the shared read lock instead of the exclusive write lock, letting a
    /// concurrent ReadPage copy from the same memory-mapped region mid-write and observe a
    /// mix of pre- and post-write bytes.
    /// </summary>
    public class PageFileTornReadTests : IDisposable
    {
        private readonly string _path;

        public PageFileTornReadTests()
        {
            _path = Path.Combine(Path.GetTempPath(), $"blite_pagefile_torn_{Guid.NewGuid()}.db");
        }

        public void Dispose()
        {
            if (File.Exists(_path)) File.Delete(_path);
        }

        [Fact]
        public async Task Concurrent_ReadPage_During_WritePage_NeverObservesTornBytes()
        {
            using var pf = new PageFile(_path, PageFileConfig.Default);
            pf.Open();

            var pageId = pf.AllocatePage();
            var pageSize = pf.PageSize;

            var patternA = new byte[pageSize];
            var patternB = new byte[pageSize];
            Array.Fill(patternA, (byte)0xAA);
            Array.Fill(patternB, (byte)0xBB);
            pf.WritePage(pageId, patternA);
            pf.Flush();

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
            var tornReads = 0;

            var writer = Task.Run(() =>
            {
                var toggle = false;
                while (!cts.IsCancellationRequested)
                {
                    pf.WritePage(pageId, toggle ? patternA : patternB);
                    toggle = !toggle;
                }
            });

            var readers = Enumerable.Range(0, 4).Select(_ => Task.Run(() =>
            {
                var buf = new byte[pageSize];
                while (!cts.IsCancellationRequested)
                {
                    pf.ReadPage(pageId, buf);
                    var first = buf[0];
                    if (first != 0xAA && first != 0xBB)
                    {
                        Interlocked.Increment(ref tornReads);
                        continue;
                    }
                    for (var i = 1; i < buf.Length; i++)
                    {
                        if (buf[i] != first)
                        {
                            Interlocked.Increment(ref tornReads);
                            break;
                        }
                    }
                }
            })).ToArray();

            await Task.WhenAll(readers.Append(writer));

            Assert.Equal(0, tornReads);
        }
    }
}
