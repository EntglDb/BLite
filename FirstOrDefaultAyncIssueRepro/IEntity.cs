namespace FirstOrDefaultAyncIssueRepro;

// EntityBase<TId> — base class with Id property
public interface IEntity<TId> where TId : notnull
{
    TId Id { get; init; }
}
