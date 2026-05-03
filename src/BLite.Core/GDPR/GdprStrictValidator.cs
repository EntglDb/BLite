using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using BLite.Core.KeyValue;

namespace BLite.Core.GDPR;

/// <summary>
/// Privacy-by-default validation for <see cref="GdprMode.Strict"/> (Art. 25).
/// Invoked at <see cref="BLite.Core.BLiteEngine"/> construction end, once per engine.
/// When <see cref="GdprMode"/> is <see cref="GdprMode.None"/> the method returns
/// immediately and emits zero log lines.
/// </summary>
/// <remarks>
/// Strict mode never deletes existing data, never rotates keys, and never modifies
/// stored documents — it only validates configuration and emits diagnostics once at
/// startup.
/// </remarks>
internal static class GdprStrictValidator
{
    // ── EventId catalogue ────────────────────────────────────────────────────
    // One dedicated EventId per emitted log line (§6 WP3 task 5).

    /// <summary>Encryption is absent on a Strict engine. Fatal — throws.</summary>
    internal const int GdprStrictEncryptionMissing = 9001;

    /// <summary>Audit module is present but no audit sink was registered.</summary>
    internal const int GdprStrictAuditMissing = 9002;

    /// <summary>Audit module has not shipped yet; continuing without audit checks.</summary>
    internal const int GdprStrictAuditModuleAbsent = 9003;

    /// <summary>A <c>[PersonalData]</c> collection has no retention policy.</summary>
    internal const int GdprStrictRetentionWarning = 9004;

    /// <summary>SecureEraseOnDelete was forced to <see langword="true"/>.</summary>
    internal const int GdprStrictSecureEraseEnabled = 9005;

    /// <summary><c>SecureEraseOnDelete</c> is not yet implemented; noting the gap.</summary>
    internal const int GdprStrictSecureEraseUnavailable = 9006;

    /// <summary>A CDC Watch was configured with <c>CapturePayload = true</c> under Strict.</summary>
    internal const int GdprStrictCdcCapturePayloadWarning = 9007;

    // ── Entry point ──────────────────────────────────────────────────────────

    /// <summary>
    /// Validates a Strict-mode engine configuration.
    /// </summary>
    /// <param name="engine">The engine being constructed.</param>
    /// <param name="resolvedMode">
    /// The resolved GDPR mode for this engine (engine-wide default from
    /// <see cref="BLiteKvOptions.DefaultGdprMode"/>).
    /// </param>
    /// <param name="options">Engine key-value options.</param>
    /// <exception cref="InvalidOperationException">
    /// Thrown when <paramref name="resolvedMode"/> is <see cref="GdprMode.Strict"/> and
    /// encryption is not configured.
    /// </exception>
    [UnconditionalSuppressMessage("Trimming", "IL2026",
        Justification = "PersonalDataResolver is a reflection fallback; source-gen path is preferred.")]
    internal static void Apply(
        BLite.Core.BLiteEngine engine,
        GdprMode resolvedMode,
        BLiteKvOptions? options)
    {
        if (resolvedMode != GdprMode.Strict)
            return;

        // ── 1. Encryption — required ─────────────────────────────────────────
        if (!engine.Storage.IsEncryptionEnabled)
        {
            throw new InvalidOperationException(
                "GdprMode.Strict requires encryption to be configured. " +
                "Supply a CryptoOptions or EncryptionCoordinator to the BLiteEngine constructor. " +
                $"(EventId {GdprStrictEncryptionMissing})");
        }

        // ── 2. Audit sink — warn if absent ───────────────────────────────────
        // The audit module (IBLiteAuditSink / ConfigureAudit) is implemented.
        // Log a warning when no sink has been registered so the operator is aware.
        if (engine.Storage.AuditSink is null)
        {
            TraceWarning(GdprStrictAuditMissing,
                "GdprMode.Strict: no audit sink is registered. " +
                "Call ConfigureAudit() with a non-null IBLiteAuditSink to satisfy Art. 30 logging requirements.");
        }

        // ── 3. Secure erase on delete — not yet a BLiteKvOptions setting ─────
        // SecureEraseOnDelete as an engine-level toggle has not been implemented.
        // Emit a single warning so the operator is aware of the gap.
        TraceWarning(GdprStrictSecureEraseUnavailable,
            "GdprMode.Strict: SecureEraseOnDelete is not yet available as an engine-level setting. " +
            "Deleted document slots are not currently zeroed at the page layer. " +
            "Track MISSING_FEATURES.md §3 for the planned implementation.");

        // ── 4. Retention — warn per [PersonalData] collection without policy ──
        try
        {
            foreach (var meta in engine.Storage.GetAllCollectionsMetadata())
            {
                if (meta.GeneralRetentionPolicy is not null)
                    continue;   // retention is configured — OK

                var pdFields = PersonalDataResolver.ResolveByCollectionName(meta.Name);
                if (pdFields.Count > 0)
                {
                    TraceWarning(GdprStrictRetentionWarning,
                        $"GdprMode.Strict: collection '{meta.Name}' contains [PersonalData] fields " +
                        $"({string.Join(", ", pdFields.Select(f => f.PropertyName))}) " +
                        "but has no retention policy configured. " +
                        "Configure via EntityTypeBuilder<T>.HasRetentionPolicy(...).");
                }
            }
        }
        catch (Exception ex)
        {
            // Retention inspection is best-effort; never block engine startup.
            Trace.TraceWarning(
                $"[BLite GDPR WP3] Retention check skipped due to unexpected error: {ex.Message}");
        }
    }

    // ── Internal helpers ─────────────────────────────────────────────────────

    private static void TraceWarning(int eventId, string message)
        => Trace.TraceWarning($"[BLite GDPR EventId:{eventId}] {message}");
}
