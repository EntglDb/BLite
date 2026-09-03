namespace BLite.Core.Collections;

/// <summary>
/// Outcome of an <c>UpsertAsync</c> call: the resolved primary key, and whether the
/// document was newly inserted (<c>true</c>) or an existing document was replaced (<c>false</c>).
/// </summary>
public readonly record struct UpsertResult<TId>(TId Id, bool Inserted);
