using BLite.Bson;
using BLite.Shared;

namespace BLite.Tests
{
    /// <summary>
    /// Regression coverage for entities and DbContexts declared in the global
    /// namespace (see <c>GlobalNamespaceFixtures.cs</c> in BLite.Shared): the
    /// mapper generator used to receive the literal "<global namespace>" from
    /// <c>ContainingNamespace.ToDisplayString()</c> and crash with an invalid
    /// hint-name character (CS8785). These fixtures compiling at all already
    /// proves the generator runs; the tests below prove the generated mapper
    /// actually round-trips data.
    /// </summary>
    public class GlobalNamespaceGeneratorTests : System.IDisposable
    {
        private const string DbPath = "globalns.db";

        public GlobalNamespaceGeneratorTests()
        {
            if (File.Exists(DbPath)) File.Delete(DbPath);
        }

        public void Dispose()
        {
            if (File.Exists(DbPath)) File.Delete(DbPath);
        }

        [Fact]
        public async Task GlobalNamespaceDbContext_Inserts_And_Reads_Back_Through_Generated_Mapper()
        {
            using var db = new GlobalNsDbContext(DbPath);

            Assert.NotNull(db.People);

            var id = ObjectId.NewObjectId();
            await db.People.InsertAsync(new GlobalNsPerson
            {
                Id = id,
                Name = "Ada",
                Metrics = new GlobalNsPerson.GlobalNsMetrics { Score = 42 }
            });

            var stored = await db.People.FindByIdAsync(id);
            Assert.NotNull(stored);
            Assert.Equal("Ada", stored.Name);
            Assert.NotNull(stored.Metrics);
            Assert.Equal(42, stored.Metrics.Score);
        }

        [Fact]
        public void Generated_Mapper_Namespace_Has_No_GlobalNamespace_Prefix()
        {
            // The mapper namespace is "{safeName}_Mappers" for global-namespace
            // contexts — it must never be "<global namespace>." or start with ".".
            var mapperTypes = typeof(GlobalNsDbContext).Assembly
                .GetTypes()
                .Where(t => t.Namespace == $"{nameof(GlobalNsDbContext)}_GlobalNamespaceFixtures_Mappers")
                .ToArray();

            Assert.NotEmpty(mapperTypes);
            Assert.Contains(mapperTypes, t => t.Name.Contains(nameof(GlobalNsPerson)));
            // The nested type of the global-namespace entity gets a mapper too.
            Assert.Contains(mapperTypes, t => t.Name.Contains(nameof(GlobalNsPerson.GlobalNsMetrics)));
        }
    }
}
