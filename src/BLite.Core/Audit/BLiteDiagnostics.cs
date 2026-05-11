using System.Diagnostics;
using System.Reflection;

namespace BLite.Core.Audit;

/// <summary>
/// Static entry point for BLite's OpenTelemetry-compatible diagnostics. Hosts a
/// single <see cref="System.Diagnostics.ActivitySource"/> named <c>"BLite.Core"</c>
/// that emits activities for commit, query, and insert operations when the audit
/// configuration's <see cref="BLiteAuditOptions.EnableDiagnosticSource"/> flag is on.
/// Tag keys follow the OpenTelemetry database semantic conventions so existing
/// OTel collectors interpret the data without bespoke mapping.
/// </summary>
public static class BLiteDiagnostics
{
    /// <summary>
    /// The single <see cref="System.Diagnostics.ActivitySource"/> used for all BLite
    /// activities. Stamped with the executing assembly version so consumers can
    /// distinguish event streams from different BLite builds.
    /// </summary>
    public static readonly ActivitySource ActivitySource = new(
        "BLite.Core",
        typeof(BLiteDiagnostics).Assembly.GetName().Version?.ToString() ?? "0.0.0");

    /// <summary>Activity name constants — kept as a nested type for discoverability.</summary>
    public static class Activity
    {
        public const string Commit = "BLite.Commit";
        public const string Query  = "BLite.Query";
        public const string Insert = "BLite.Insert";
    }

    /// <summary>
    /// OpenTelemetry semantic-convention tag keys plus BLite-specific extensions.
    /// Reusing the constants avoids typos and ensures every activity emits the same
    /// key strings, which downstream collectors group on.
    /// </summary>
    public static class Tags
    {
        // OpenTelemetry semantic conventions for databases.
        public const string DbSystem         = "db.system";
        public const string DbCollectionName = "db.collection.name";
        public const string DbOperation      = "db.operation";

        // BLite-specific extensions (namespaced under "blite.*" to avoid clashing).
        public const string TransactionId    = "blite.transaction.id";
        public const string PagesWritten     = "blite.commit.pages_written";
        public const string WalSizeBytes     = "blite.commit.wal_size_bytes";
        public const string QueryStrategy    = "blite.query.strategy";
        public const string QueryIndexName   = "blite.query.index_name";
        public const string QueryResultCount = "blite.query.result_count";
        public const string DocumentSize     = "blite.insert.document_size_bytes";

        /// <summary>Constant value for <see cref="DbSystem"/> — identifies the DB engine.</summary>
        public const string DbSystemValue = "blite";
    }
}
