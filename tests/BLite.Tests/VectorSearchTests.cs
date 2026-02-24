using BLite.Core;
using BLite.Shared;

namespace BLite.Tests;

public class VectorSearchTests
{
    [Fact]
    public void Test_VectorSearch_Basic()
    {
        string dbPath = "vector_test.db";
        if (File.Exists(dbPath)) File.Delete(dbPath);

        using (var db = new TestDbContext(dbPath))
        {
            db.VectorItems.Insert(new VectorEntity { Title = "Near", Embedding = [1.0f, 1.0f, 1.0f] });
            db.VectorItems.Insert(new VectorEntity { Title = "Far", Embedding = [10.0f, 10.0f, 10.0f] });

            var query = new[] { 0.9f, 0.9f, 0.9f };
            var results = db.VectorItems.AsQueryable().Where(x => x.Embedding.VectorSearch(query, 1)).ToList();

            Assert.Single(results);
            Assert.Equal("Near", results[0].Title);
        }

        File.Delete(dbPath);
    }
}
