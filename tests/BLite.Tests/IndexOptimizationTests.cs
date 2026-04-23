using BLite.Core.Query;
using BLite.Core.Indexing;
using System.Linq.Expressions;

namespace BLite.Tests
{
    public class IndexOptimizationTests
    {
        public class TestEntity
        {
            public int Id { get; set; }
            public string Name { get; set; } = "";
            public int Age { get; set; }
            public bool IsActive { get; set; }
            public Guid ExternalId { get; set; }
            public byte[] Payload { get; set; } = [];
        }

        [Fact]
        public void Optimizer_Identifies_Equality()
        {
            var indexes = new List<CollectionIndexInfo>
            {
                new CollectionIndexInfo { Name = "idx_age", PropertyPaths = ["Age"] }
            };

            Expression<Func<TestEntity, bool>> predicate = x => x.Age == 30;
            var model = new QueryModel { WhereClause = predicate };

            var result = IndexOptimizer.TryOptimize<TestEntity>(model, indexes);

            Assert.NotNull(result);
            Assert.Equal("idx_age", result.IndexName);
            Assert.Equal(30, result.MinValue);
            Assert.Equal(30, result.MaxValue);
            Assert.False(result.IsRange);
        }

        [Fact]
        public void Optimizer_Identifies_Range_GreaterThan()
        {
            var indexes = new List<CollectionIndexInfo>
            {
                new CollectionIndexInfo { Name = "idx_age", PropertyPaths = ["Age"] }
            };

            Expression<Func<TestEntity, bool>> predicate = x => x.Age > 25;
            var model = new QueryModel { WhereClause = predicate };

            var result = IndexOptimizer.TryOptimize<TestEntity>(model, indexes);

            Assert.NotNull(result);
            Assert.Equal("idx_age", result.IndexName);
            Assert.Equal(25, result.MinValue);
            Assert.Null(result.MaxValue);
            Assert.True(result.IsRange);
        }

        [Fact]
        public void Optimizer_Identifies_Range_LessThan()
        {
            var indexes = new List<CollectionIndexInfo>
            {
                new CollectionIndexInfo { Name = "idx_age", PropertyPaths = ["Age"] }
            };

            Expression<Func<TestEntity, bool>> predicate = x => x.Age < 50;
            var model = new QueryModel { WhereClause = predicate };

            var result = IndexOptimizer.TryOptimize<TestEntity>(model, indexes);

            Assert.NotNull(result);
            Assert.Equal("idx_age", result.IndexName);
            Assert.Null(result.MinValue);
            Assert.Equal(50, result.MaxValue);
            Assert.True(result.IsRange);
        }
        
        [Fact]
        public void Optimizer_Identifies_Range_Between_Simulated()
        {
            var indexes = new List<CollectionIndexInfo>
            {
                new CollectionIndexInfo { Name = "idx_age", PropertyPaths = ["Age"] }
            };

            Expression<Func<TestEntity, bool>> predicate = x => x.Age > 20 && x.Age < 40;
            var model = new QueryModel { WhereClause = predicate };

            var result = IndexOptimizer.TryOptimize<TestEntity>(model, indexes);

            Assert.NotNull(result);
            Assert.Equal("idx_age", result.IndexName);
            Assert.Equal(20, result.MinValue);
            Assert.Equal(40, result.MaxValue);
            Assert.True(result.IsRange);
            // Both sides are strict — merged range still needs a post-filter to exclude boundaries.
            Assert.Equal(IndexOptimizer.FilterCompleteness.StrictBoundary, result.FilterCompleteness);
        }

        [Fact]
        public void Optimizer_Identifies_Range_Between_InclusiveLower_StrictUpper()
        {
            var indexes = new List<CollectionIndexInfo>
            {
                new CollectionIndexInfo { Name = "idx_age", PropertyPaths = ["Age"] }
            };

            // >= is Exact, < is StrictBoundary — merged result must be StrictBoundary.
            Expression<Func<TestEntity, bool>> predicate = x => x.Age >= 20 && x.Age < 40;
            var model = new QueryModel { WhereClause = predicate };

            var result = IndexOptimizer.TryOptimize<TestEntity>(model, indexes);

            Assert.NotNull(result);
            Assert.Equal("idx_age", result.IndexName);
            Assert.Equal(20, result.MinValue);
            Assert.Equal(40, result.MaxValue);
            Assert.True(result.IsRange);
            Assert.Equal(IndexOptimizer.FilterCompleteness.StrictBoundary, result.FilterCompleteness);
        }

        [Fact]
        public void Optimizer_Identifies_Range_Between_InclusiveBounds()
        {
            var indexes = new List<CollectionIndexInfo>
            {
                new CollectionIndexInfo { Name = "idx_age", PropertyPaths = ["Age"] }
            };

            // Both sides are inclusive — merged result should be Exact (no post-filter needed).
            Expression<Func<TestEntity, bool>> predicate = x => x.Age >= 20 && x.Age <= 40;
            var model = new QueryModel { WhereClause = predicate };

            var result = IndexOptimizer.TryOptimize<TestEntity>(model, indexes);

            Assert.NotNull(result);
            Assert.Equal("idx_age", result.IndexName);
            Assert.Equal(20, result.MinValue);
            Assert.Equal(40, result.MaxValue);
            Assert.True(result.IsRange);
            Assert.Equal(IndexOptimizer.FilterCompleteness.Exact, result.FilterCompleteness);
        }
        
         [Fact]
        public void Optimizer_Identifies_StartsWith()
        {
            var indexes = new List<CollectionIndexInfo>
            {
                new CollectionIndexInfo { Name = "idx_name", PropertyPaths = ["Name"], Type = IndexType.BTree }
            };

            Expression<Func<TestEntity, bool>> predicate = x => x.Name.StartsWith("Ali");
            var model = new QueryModel { WhereClause = predicate };

            var result = IndexOptimizer.TryOptimize<TestEntity>(model, indexes);

            Assert.NotNull(result);
            Assert.Equal("idx_name", result.IndexName);
            Assert.Equal("Ali", result.MinValue);
            // "Ali" + next char -> "Alj"
            Assert.Equal("Alj", result.MaxValue);
            Assert.True(result.IsRange);
        }

        [Fact]
        public void Optimizer_Ignores_NonIndexed_Fields()
        {
            var indexes = new List<CollectionIndexInfo>
            {
                new CollectionIndexInfo { Name = "idx_age", PropertyPaths = ["Age"] }
            };

            Expression<Func<TestEntity, bool>> predicate = x => x.Name == "Alice"; // Name is not indexed
            var model = new QueryModel { WhereClause = predicate };

            var result = IndexOptimizer.TryOptimize<TestEntity>(model, indexes);

            Assert.Null(result);
        }

        [Fact]
        public void Optimizer_BareBoolMember_ReturnsEqualityTrue()
        {
            var indexes = new List<CollectionIndexInfo>
            {
                new CollectionIndexInfo { Name = "idx_active", PropertyPaths = ["IsActive"] }
            };

            Expression<Func<TestEntity, bool>> predicate = x => x.IsActive;
            var model = new QueryModel { WhereClause = predicate };

            var result = IndexOptimizer.TryOptimize<TestEntity>(model, indexes);

            Assert.NotNull(result);
            Assert.Equal("idx_active", result.IndexName);
            Assert.Equal(true, result.MinValue);
            Assert.Equal(true, result.MaxValue);
            Assert.False(result.IsRange);
            Assert.Equal(IndexOptimizer.FilterCompleteness.Exact, result.FilterCompleteness);
        }

        [Fact]
        public void Optimizer_NotBoolMember_ReturnsEqualityFalse()
        {
            var indexes = new List<CollectionIndexInfo>
            {
                new CollectionIndexInfo { Name = "idx_active", PropertyPaths = ["IsActive"] }
            };

            Expression<Func<TestEntity, bool>> predicate = x => !x.IsActive;
            var model = new QueryModel { WhereClause = predicate };

            var result = IndexOptimizer.TryOptimize<TestEntity>(model, indexes);

            Assert.NotNull(result);
            Assert.Equal("idx_active", result.IndexName);
            Assert.Equal(false, result.MinValue);
            Assert.Equal(false, result.MaxValue);
            Assert.False(result.IsRange);
            Assert.Equal(IndexOptimizer.FilterCompleteness.Exact, result.FilterCompleteness);
        }

        [Fact]
        public void Optimizer_BareBoolMember_NonIndexedField_ReturnsNull()
        {
            var indexes = new List<CollectionIndexInfo>
            {
                new CollectionIndexInfo { Name = "idx_age", PropertyPaths = ["Age"] }
            };

            Expression<Func<TestEntity, bool>> predicate = x => x.IsActive; // IsActive not indexed
            var model = new QueryModel { WhereClause = predicate };

            var result = IndexOptimizer.TryOptimize<TestEntity>(model, indexes);

            Assert.Null(result);
        }

        [Fact]
        public void Optimizer_StrictGreaterThan_ReturnsStrictBoundary()
        {
            var indexes = new List<CollectionIndexInfo>
            {
                new CollectionIndexInfo { Name = "idx_age", PropertyPaths = ["Age"] }
            };

            Expression<Func<TestEntity, bool>> predicate = x => x.Age > 25;
            var model = new QueryModel { WhereClause = predicate };

            var result = IndexOptimizer.TryOptimize<TestEntity>(model, indexes);

            Assert.NotNull(result);
            Assert.True(result.IsRange);
            Assert.Equal(IndexOptimizer.FilterCompleteness.StrictBoundary, result.FilterCompleteness);
        }

        [Fact]
        public void Optimizer_StrictLessThan_ReturnsStrictBoundary()
        {
            var indexes = new List<CollectionIndexInfo>
            {
                new CollectionIndexInfo { Name = "idx_age", PropertyPaths = ["Age"] }
            };

            Expression<Func<TestEntity, bool>> predicate = x => x.Age < 50;
            var model = new QueryModel { WhereClause = predicate };

            var result = IndexOptimizer.TryOptimize<TestEntity>(model, indexes);

            Assert.NotNull(result);
            Assert.True(result.IsRange);
            Assert.Equal(IndexOptimizer.FilterCompleteness.StrictBoundary, result.FilterCompleteness);
        }

        [Fact]
        public void Optimizer_InclusiveGreaterThanOrEqual_ReturnsExact()
        {
            var indexes = new List<CollectionIndexInfo>
            {
                new CollectionIndexInfo { Name = "idx_age", PropertyPaths = ["Age"] }
            };

            Expression<Func<TestEntity, bool>> predicate = x => x.Age >= 25;
            var model = new QueryModel { WhereClause = predicate };

            var result = IndexOptimizer.TryOptimize<TestEntity>(model, indexes);

            Assert.NotNull(result);
            Assert.True(result.IsRange);
            Assert.Equal(IndexOptimizer.FilterCompleteness.Exact, result.FilterCompleteness);
        }

        [Fact]
        public void Optimizer_PartialAnd_OneNonIndexedField_ReturnsPartialAnd()
        {
            var indexes = new List<CollectionIndexInfo>
            {
                new CollectionIndexInfo { Name = "idx_age", PropertyPaths = ["Age"] }
            };

            // Name is not indexed, so only the Age side is covered by the index.
            Expression<Func<TestEntity, bool>> predicate = x => x.Age > 20 && x.Name == "Alice";
            var model = new QueryModel { WhereClause = predicate };

            var result = IndexOptimizer.TryOptimize<TestEntity>(model, indexes);

            Assert.NotNull(result);
            Assert.Equal("idx_age", result.IndexName);
            Assert.Equal(IndexOptimizer.FilterCompleteness.PartialAnd, result.FilterCompleteness);
        }

        [Fact]
        public void Optimizer_Or_Equality_SameIndexedField_ReturnsInValues()
        {
            var indexes = new List<CollectionIndexInfo>
            {
                new CollectionIndexInfo { Name = "idx_name", PropertyPaths = ["Name"] }
            };

            Expression<Func<TestEntity, bool>> predicate = x => x.Name == "Alice" || x.Name == "Bob";
            var model = new QueryModel { WhereClause = predicate };

            var result = IndexOptimizer.TryOptimize<TestEntity>(model, indexes);

            Assert.NotNull(result);
            Assert.Equal("idx_name", result.IndexName);
            Assert.NotNull(result.InValues);
            Assert.Equal(2, result.InValues!.Count);
            Assert.Contains("Alice", result.InValues);
            Assert.Contains("Bob", result.InValues);
            Assert.Equal(IndexOptimizer.FilterCompleteness.Exact, result.FilterCompleteness);
        }

        [Fact]
        public void Optimizer_Contains_OnIndexedField_ReturnsInValues()
        {
            var indexes = new List<CollectionIndexInfo>
            {
                new CollectionIndexInfo { Name = "idx_name", PropertyPaths = ["Name"] }
            };

            var names = new[] { "Alice", "Bob" };
            Expression<Func<TestEntity, bool>> predicate = x => names.Contains(x.Name);
            var model = new QueryModel { WhereClause = predicate };

            var result = IndexOptimizer.TryOptimize<TestEntity>(model, indexes);

            Assert.NotNull(result);
            Assert.Equal("idx_name", result.IndexName);
            Assert.NotNull(result.InValues);
            Assert.Equal(2, result.InValues!.Count);
            Assert.Contains("Alice", result.InValues);
            Assert.Contains("Bob", result.InValues);
            Assert.Equal(IndexOptimizer.FilterCompleteness.Exact, result.FilterCompleteness);
        }

        [Fact]
        public void Optimizer_Contains_WithDuplicateValues_DeDupesInValues()
        {
            var indexes = new List<CollectionIndexInfo>
            {
                new CollectionIndexInfo { Name = "idx_name", PropertyPaths = ["Name"] }
            };

            var names = new[] { "Alice", "Alice", "Bob", "Bob" };
            Expression<Func<TestEntity, bool>> predicate = x => names.Contains(x.Name);
            var model = new QueryModel { WhereClause = predicate };

            var result = IndexOptimizer.TryOptimize<TestEntity>(model, indexes);

            Assert.NotNull(result);
            Assert.NotNull(result.InValues);
            Assert.Equal(2, result.InValues!.Count);
            Assert.Equal(["Alice", "Bob"], result.InValues);
        }

        [Fact]
        public void Optimizer_Contains_GuidField_ReturnsInValues()
        {
            var indexes = new List<CollectionIndexInfo>
            {
                new CollectionIndexInfo { Name = "idx_external_id", PropertyPaths = ["ExternalId"] }
            };

            var a = Guid.NewGuid();
            var b = Guid.NewGuid();
            var ids = new[] { a, b };
            Expression<Func<TestEntity, bool>> predicate = x => ids.Contains(x.ExternalId);
            var model = new QueryModel { WhereClause = predicate };

            var result = IndexOptimizer.TryOptimize<TestEntity>(model, indexes);

            Assert.NotNull(result);
            Assert.NotNull(result.InValues);
            Assert.Equal([a, b], result.InValues);
        }

        [Fact]
        public void Optimizer_Contains_ByteArrayField_ReturnsInValues()
        {
            var indexes = new List<CollectionIndexInfo>
            {
                new CollectionIndexInfo { Name = "idx_payload", PropertyPaths = ["Payload"] }
            };

            var p1 = new byte[] { 1, 2, 3 };
            var p2 = new byte[] { 4, 5, 6 };
            var payloads = new[] { p1, p2, p1 };
            Expression<Func<TestEntity, bool>> predicate = x => payloads.Contains(x.Payload);
            var model = new QueryModel { WhereClause = predicate };

            var result = IndexOptimizer.TryOptimize<TestEntity>(model, indexes);

            Assert.NotNull(result);
            Assert.NotNull(result.InValues);
            Assert.Equal(2, result.InValues!.Count);
            Assert.Same(p1, result.InValues[0]);
            Assert.Same(p2, result.InValues[1]);
        }

        [Fact]
        public void Optimizer_Contains_ByteArrayField_DeDupesByValue()
        {
            var indexes = new List<CollectionIndexInfo>
            {
                new CollectionIndexInfo { Name = "idx_payload", PropertyPaths = ["Payload"] }
            };

            var p1 = new byte[] { 1, 2, 3 };
            var p1Copy = new byte[] { 1, 2, 3 };
            var payloads = new[] { p1, p1Copy };
            Expression<Func<TestEntity, bool>> predicate = x => payloads.Contains(x.Payload);
            var model = new QueryModel { WhereClause = predicate };

            var result = IndexOptimizer.TryOptimize<TestEntity>(model, indexes);

            Assert.NotNull(result);
            Assert.NotNull(result.InValues);
            Assert.Single(result.InValues);
        }
    }
}
