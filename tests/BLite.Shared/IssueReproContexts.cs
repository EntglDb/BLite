using BLite.Bson;
using BLite.Core;
using BLite.Core.Collections;
using BLite.Core.Metadata;

namespace BLite.Shared;

public class ReproA { public ObjectId Id { get; set; } public string Name { get; set; } = ""; }
public class ReproB { public ObjectId Id { get; set; } public string Name { get; set; } = ""; }
public class ReproFigure { public ObjectId Id { get; set; } public decimal Amount { get; set; } }
public class ReproIdxFigure { public ObjectId Id { get; set; } public decimal Amount { get; set; } }

public partial class ReproADbContext : DocumentDbContext
{
    public DocumentCollection<ObjectId, ReproA> A { get; set; } = null!;
    public ReproADbContext(string path) : base(path) => InitializeCollections();
}
public partial class ReproBDbContext : DocumentDbContext
{
    public DocumentCollection<ObjectId, ReproB> B { get; set; } = null!;
    public ReproBDbContext(string path) : base(path) => InitializeCollections();
}
public partial class ReproFigDbContext : DocumentDbContext
{
    public DocumentCollection<ObjectId, ReproFigure> Figures { get; set; } = null!;
    public DocumentCollection<ObjectId, ReproIdxFigure> Indexed { get; set; } = null!;
    public ReproFigDbContext(string path) : base(path) => InitializeCollections();
    protected override void OnModelCreating(ModelBuilder mb)
    {
        mb.Entity<ReproFigure>();
        mb.Entity<ReproIdxFigure>().HasIndex(x => x.Amount);
    }
}

