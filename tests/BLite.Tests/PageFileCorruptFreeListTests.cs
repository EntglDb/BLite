using BLite.Core;
using BLite.Core.Storage;

namespace BLite.Tests
{
    /// <summary>
    /// A free-list head that points outside the file (seen in the field: page 0 said the first free page
    /// was 402653184 in a 768-page file) used to end in an AccessViolationException inside AllocatePage,
    /// killing the process on every insert that needed a page.
    /// </summary>
    public class PageFileCorruptFreeListTests : IDisposable
    {
        private readonly string _path = Path.Combine(Path.GetTempPath(), $"blite_pagefile_freelist_{Guid.NewGuid()}.db");

        public void Dispose()
        {
            if (File.Exists(_path)) File.Delete(_path);
        }

        private static void SetFreeListHead(string path, uint head)
        {
            using var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite);
            fs.Position = 7; // PageHeader.NextPageId of page 0 holds the free-list head
            fs.Write(BitConverter.GetBytes(head));
        }

        private static uint ReadFreeListHead(string path)
        {
            using var fs = new FileStream(path, FileMode.Open, FileAccess.Read);
            var buf = new byte[4];
            fs.Position = 7;
            fs.ReadExactly(buf);
            return BitConverter.ToUInt32(buf);
        }

        [Fact]
        public void AllocatePage_WithHeadOutsideTheFile_DropsTheFreeListAndAppends()
        {
            uint lastPage;
            using (var pf = new PageFile(_path, PageFileConfig.Default))
            {
                pf.Open();
                lastPage = pf.AllocatePage();
                pf.Flush();
            }

            SetFreeListHead(_path, 402653184);

            uint expected, allocated;
            int dropped;
            using (var reopened = new PageFile(_path, PageFileConfig.Default))
            {
                reopened.Open();
                expected = reopened.NextPageId;
                allocated = reopened.AllocatePage();
                dropped = reopened.CorruptFreeListsDropped;
                reopened.Flush();
            }

            Assert.Equal(expected, allocated);
            Assert.True(allocated > lastPage);
            Assert.Equal(1, dropped);
            Assert.Equal(0u, ReadFreeListHead(_path));
        }

        [Fact]
        public void AllocatePage_WithHeadOnALivePage_DoesNotHandOutThatPage()
        {
            uint live;
            using (var pf = new PageFile(_path, PageFileConfig.Default))
            {
                pf.Open();
                live = pf.AllocatePage();
                var data = new byte[pf.PageSize];
                data[4] = (byte)PageType.Data;
                Array.Fill(data, (byte)0x5A, 32, 64);
                pf.WritePage(live, data);
                pf.Flush();
            }

            SetFreeListHead(_path, live);

            using var reopened = new PageFile(_path, PageFileConfig.Default);
            reopened.Open();

            var allocated = reopened.AllocatePage();

            Assert.NotEqual(live, allocated);
            var check = new byte[reopened.PageSize];
            reopened.ReadPage(live, check);
            Assert.Equal(0x5A, check[40]);
        }

        [Fact]
        public void ReadPage_OutsideTheFile_ThrowsInsteadOfFaulting()
        {
            using var pf = new PageFile(_path, PageFileConfig.Default);
            pf.Open();

            Assert.Throws<InvalidDataException>(() => pf.ReadPage(402653184, new byte[pf.PageSize]));
        }

        [Fact]
        public void WritePage0_KeepsTheFreeListHeadOwnedByThePageFile()
        {
            using var pf = new PageFile(_path, PageFileConfig.Default);
            pf.Open();
            var a = pf.AllocatePage();
            var b = pf.AllocatePage();

            // A caller takes a copy of page 0 while the free list is empty...
            var stale = new byte[pf.PageSize];
            pf.ReadPage(0, stale);

            // ...meanwhile pages are freed...
            pf.FreePage(a);
            pf.FreePage(b);

            // ...and the stale copy is written back (what the dictionary/KV root updates do).
            pf.WritePage(0, stale);

            Assert.Equal(b, pf.AllocatePage());
            Assert.Equal(a, pf.AllocatePage());
        }
    }
}
