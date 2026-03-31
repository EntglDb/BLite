namespace FirstOrDefaultAyncIssueRepro;

// EntityBase<TId> — base class with Id property
public abstract class EntityBase<TId> : IEntity<TId> where TId : notnull
{
    protected EntityBase(TId id)
    {
        ArgumentNullException.ThrowIfNull(id, "entity id");
        Id = id;
    }

    public virtual TId Id { get; init; }
}
