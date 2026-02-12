using Xunit;
using BLite.Core.Query;
using BLite.Core.Indexing;
using System.Linq.Expressions;
using System.Collections.Generic;
using System;

namespace BLite.Tests
{
    public class IndexOptimizationTests
    {
        public class TestEntity
        {
            public int Id { get; set; }
            public string Name { get; set; } = "";
            public int Age { get; set; }
        }

        [Fact]
        public void Optimizer_Identifies_Equality()
        {
            var indexes = new List<CollectionIndexInfo>
            {
                new CollectionIndexInfo { Name = "idx_age", PropertyPaths = new[] { "Age" } }
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
                new CollectionIndexInfo { Name = "idx_age", PropertyPaths = new[] { "Age" } }
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
                new CollectionIndexInfo { Name = "idx_age", PropertyPaths = new[] { "Age" } }
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
                new CollectionIndexInfo { Name = "idx_age", PropertyPaths = new[] { "Age" } }
            };

            Expression<Func<TestEntity, bool>> predicate = x => x.Age > 20 && x.Age < 40;
            var model = new QueryModel { WhereClause = predicate };

            var result = IndexOptimizer.TryOptimize<TestEntity>(model, indexes);

            Assert.NotNull(result);
            Assert.Equal("idx_age", result.IndexName);
            Assert.Equal(20, result.MinValue);
            Assert.Equal(40, result.MaxValue);
            Assert.True(result.IsRange);
        }
        
         [Fact]
        public void Optimizer_Identifies_StartsWith()
        {
            var indexes = new List<CollectionIndexInfo>
            {
                new CollectionIndexInfo { Name = "idx_name", PropertyPaths = new[] { "Name" }, Type = IndexType.BTree }
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
                new CollectionIndexInfo { Name = "idx_age", PropertyPaths = new[] { "Age" } }
            };

            Expression<Func<TestEntity, bool>> predicate = x => x.Name == "Alice"; // Name is not indexed
            var model = new QueryModel { WhereClause = predicate };

            var result = IndexOptimizer.TryOptimize<TestEntity>(model, indexes);

            Assert.Null(result);
        }
    }
}
