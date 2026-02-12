using BLite.Core;
using BLite.Bson;
using BLite.Core.Indexing;
using BLite.Core.Metadata;
using Xunit;
using System.IO;
using BLite.Core.Storage;
using BLite.Core.Collections;

namespace BLite.Tests.AutoInit
{
    public class AutoInitTests : System.IDisposable
    {
        private const string DbPath = "autoinit.db";

        public AutoInitTests()
        {
            if (File.Exists(DbPath)) File.Delete(DbPath);
        }

        public void Dispose()
        {
            if (File.Exists(DbPath)) File.Delete(DbPath);
        }

        [Fact]
        public void Collections_Are_Initialized_By_Generator()
        {
            using var db = new TestDbContext(DbPath);
            
            // Verify Collection is not null (initialized by generated method)
            Assert.NotNull(db.AutoInitEntities);
            
            // Verify we can use it
            db.AutoInitEntities.Insert(new AutoInitEntity { Id = 1, Name = "Test" });
            var stored = db.AutoInitEntities.FindById(1);
            Assert.NotNull(stored);
            Assert.Equal("Test", stored.Name);
        }
    }
}
