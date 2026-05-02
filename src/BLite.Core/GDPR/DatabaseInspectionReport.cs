namespace BLite.Core.GDPR;

/// <summary>
/// Snapshot of the database's compliance-relevant configuration, produced by
/// <see cref="GdprEngineExtensions.InspectDatabase"/> for Art. 30 record-of-processing purposes.
/// This is a pure data record — it holds no resources and does not need to be disposed.
/// </summary>
/// <param name="DatabasePath">
/// Absolute path of the main database file, or an empty string for in-memory databases.
/// </param>
/// <param name="IsEncrypted">
/// <see langword="true"/> when the engine was constructed with <c>CryptoOptions</c> or an
/// <c>EncryptionCoordinator</c> and the active provider is not <c>NullCryptoProvider</c>.
/// </param>
/// <param name="IsAuditEnabled">
/// <see langword="true"/> when an <c>IBLiteAuditSink</c> is registered on the engine.
/// <see langword="false"/> when the audit module has not been configured.
/// </param>
/// <param name="IsMultiFileMode">
/// <see langword="true"/> when the engine uses a separate file per collection
/// (server-layout / <c>PageFileConfig.Server(…)</c>).
/// </param>
/// <param name="Collections">
/// Snapshot of every collection registered in the catalog at the time of the call.
/// </param>
public sealed record DatabaseInspectionReport(
    string DatabasePath,
    bool IsEncrypted,
    bool IsAuditEnabled,
    bool IsMultiFileMode,
    IReadOnlyList<CollectionInfo> Collections);
