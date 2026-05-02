namespace BLite.Core.Audit;

/// <summary>
/// Hook for the host application to supply the current caller identity.
/// Implement this interface and assign it to <see cref="BLiteAuditOptions.ContextProvider"/>
/// to propagate per-request identities (e.g. from an HTTP request context) into audit events.
/// </summary>
/// <remarks>
/// The built-in implementation is <see cref="AmbientAuditContext"/>, which uses
/// <see cref="System.Threading.AsyncLocal{T}"/> so that the identity flows automatically
/// across <c>await</c> boundaries.
/// </remarks>
public interface IAuditContextProvider
{
    /// <summary>
    /// Returns the current caller identity, or <see langword="null"/> when not set.
    /// This is not an error — an embedded database has no authentication layer.
    /// </summary>
    string? GetCurrentUserId();
}
