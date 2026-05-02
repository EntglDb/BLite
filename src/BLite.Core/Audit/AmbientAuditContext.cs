namespace BLite.Core.Audit;

/// <summary>
/// Built-in <see cref="IAuditContextProvider"/> that uses <see cref="System.Threading.AsyncLocal{T}"/>
/// to propagate caller identity across <c>await</c> boundaries.
/// </summary>
/// <remarks>
/// <para>
/// Set the current caller identity at the start of a request or unit-of-work:
/// <code>
/// AmbientAuditContext.CurrentUserId = httpContext.User.FindFirst("sub")?.Value;
/// </code>
/// </para>
/// <para>
/// The value flows automatically through all <c>await</c> continuations on the same
/// logical execution context, and is automatically reset when the context exits.
/// </para>
/// <para>
/// If not set, <see cref="GetCurrentUserId"/> returns <see langword="null"/> — which is
/// correct, not an error; BLite has no authentication layer.
/// </para>
/// </remarks>
public sealed class AmbientAuditContext : IAuditContextProvider
{
    private static readonly System.Threading.AsyncLocal<string?> s_userId = new();

    /// <summary>The singleton instance used by default when no custom provider is configured.</summary>
    public static readonly AmbientAuditContext Instance = new();

    private AmbientAuditContext() { }

    /// <summary>
    /// Gets or sets the current caller identity for the ambient execution context.
    /// Set this once per request; it propagates automatically to all awaited operations.
    /// </summary>
    public static string? CurrentUserId
    {
        get => s_userId.Value;
        set => s_userId.Value = value;
    }

    /// <inheritdoc />
    public string? GetCurrentUserId() => s_userId.Value;
}
