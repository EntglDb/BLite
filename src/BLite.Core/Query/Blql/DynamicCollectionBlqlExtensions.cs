using BLite.Core.Query.Blql;

namespace BLite.Core;

/// <summary>
/// Extends <see cref="DynamicCollection"/> with BLQL (BLite Query Language) query support.
/// </summary>
public static class DynamicCollectionBlqlExtensions
{
    /// <summary>
    /// Creates a new BLQL query builder for this collection.
    /// 
    /// <example>
    /// <code>
    /// // Filter, sort, page and project:
    /// var users = collection.Query()
    ///     .Filter(BlqlFilter.And(
    ///         BlqlFilter.Eq("status", "active"),
    ///         BlqlFilter.Gte("age", 18)))
    ///     .OrderBy("name")
    ///     .Skip(0).Take(25)
    ///     .Project(BlqlProjection.Include("name", "email", "age"))
    ///     .ToList();
    ///
    /// // Range query:
    /// var recent = collection.Query()
    ///     .Filter(BlqlFilter.Between("createdAt", DateTime.UtcNow.AddDays(-7), DateTime.UtcNow))
    ///     .OrderByDescending("createdAt")
    ///     .ToList();
    ///
    /// // IN operator:
    /// var admins = collection.Query()
    ///     .Filter(BlqlFilter.In("role", "admin", "superadmin"))
    ///     .ToList();
    ///
    /// // Count matching documents:
    /// int count = collection.Query()
    ///     .Filter(BlqlFilter.Eq("status", "pending"))
    ///     .Count();
    /// </code>
    /// </example>
    /// </summary>
    /// <param name="collection">The collection to query.</param>
    /// <returns>A new <see cref="BlqlQuery"/> builder.</returns>
    public static BlqlQuery Query(this DynamicCollection collection)
        => new BlqlQuery(collection);

    /// <summary>
    /// Creates a new BLQL query builder pre-initialized with the given filter.
    /// Shorthand for <c>collection.Query().Filter(filter)</c>.
    /// </summary>
    /// <param name="collection">The collection to query.</param>
    /// <param name="filter">The initial filter to apply.</param>
    /// <returns>A new <see cref="BlqlQuery"/> builder with the filter set.</returns>
    public static BlqlQuery Query(this DynamicCollection collection, BlqlFilter filter)
        => new BlqlQuery(collection).Filter(filter);

    /// <summary>
    /// Creates a BLQL query builder from a JSON filter string (MQL-style).
    /// Parses the string using <see cref="BlqlFilterParser"/>.
    /// <example><code>
    /// collection.Query("{ \"status\": \"active\", \"age\": { \"$gt\": 18 } }")
    /// </code></example>
    /// </summary>
    public static BlqlQuery Query(this DynamicCollection collection, string filterJson)
        => new BlqlQuery(collection).Filter(BlqlFilterParser.Parse(filterJson));
}
