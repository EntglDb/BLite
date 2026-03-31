namespace FirstOrDefaultAyncIssueRepro;

// EntityBase<TId> — base class with Id property
// PoBase<TId> — persistence object base
public abstract class PoBase<TId>(TId id) : EntityBase<TId>(id), IPoEntity where TId : notnull
{
    public DateTime AddedAt { get; set; }
}
