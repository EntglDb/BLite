using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using BLite.Core.KeyValue;
using BLite.Core.Storage;

namespace BLite.Core.GDPR;

/// <summary>
/// Privacy-by-default validation for <see cref="GdprMode.Strict"/> (Art. 25).
/// Invoked at <see cref="BLite.Core.BLiteEngine"/> and <see cref="BLite.Core.DocumentDbContext"/>
/// construction end, once per engine/context.
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

    /// <summary><c>PersonalDataResolver</c> raised an unexpected error during retention inspection; check skipped.</summary>
    internal const int GdprStrictInspectionError = 9008;

    // ── Entry point ──────────────────────────────────────────────────────────

    /// <summary>
    /// Validates a Strict-mode engine or context configuration.
    /// Accepts the <see cref="StorageEngine"/> directly so the same logic covers both
    /// <see cref="BLite.Core.BLiteEngine"/> (dynamic path) and
    /// <see cref="BLite.Core.DocumentDbContext"/> (typed path).
    /// </summary>
    /// <param name="storage">The storage engine backing the engine or context.</param>
    /// <param name="resolvedMode">
    /// The resolved GDPR mode. For <see cref="BLite.Core.BLiteEngine"/> this equals
    /// <see cref="BLiteKvOptions.DefaultGdprMode"/>. For
    /// <see cref="BLite.Core.DocumentDbContext"/> callers should pass the effective mode
    /// (engine-wide default OR the strictest per-collection mode from the model).
    /// </param>
    /// <param name="options">Engine key-value options.</param>
    /// <exception cref="InvalidOperationException">
    /// Thrown when <paramref name="resolvedMode"/> is <see cref="GdprMode.Strict"/> and
    /// encryption is not configured.
    /// </exception>
    [UnconditionalSuppressMessage("Trimming", "IL2026",
        Justification = "PersonalDataResolver is a reflection fallback; source-gen path is preferred.")]
    internal static void Apply(
        StorageEngine storage,
        GdprMode resolvedMode,
        BLiteKvOptions? options)
    {
        if (resolvedMode != GdprMode.Strict)
            return;

        // ── 1. Encryption — required ─────────────────────────────────────────
        if (!storage.IsEncryptionEnabled)
        {
            throw new InvalidOperationException(
                "GdprMode.Strict requires encryption to be configured. " +
                "Supply a CryptoOptions or EncryptionCoordinator to the BLiteEngine constructor " +
                "or DocumentDbContext constructor. " +
                $"(EventId {GdprStrictEncryptionMissing})");
        }

        // ── 2. Audit sink — warn if absent ───────────────────────────────────
        // ConfigureAudit() is a post-construction method, so the audit sink is never
        // available at this point.  Emit an actionable reminder so the operator ensures
        // ConfigureAudit() is called immediately after construction.
        if (storage.AuditSink is null)
        {
            TraceWarning(GdprStrictAuditMissing,
                "GdprMode.Strict: no audit sink is registered. " +
                "Ensure ConfigureAudit() with a non-null IBLiteAuditSink is called immediately " +
                "after construction to satisfy Art. 30 logging requirements.");
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
            foreach (var meta in storage.GetAllCollectionsMetadata())
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
                        "Configure via EntityTypeBuilder<T>.HasRetentionPolicy(...). " +
                        "Note: only collections with a generated source-gen mapper are checked by this inspector.");
                }
            }
        }
        catch (Exception ex)
        {
            // Retention inspection is best-effort; never block engine startup.
            TraceWarning(GdprStrictInspectionError,
                $"GdprMode.Strict: retention inspection skipped due to an unexpected error ({ex.GetType().Name}: {ex.Message}). " +
                "Verify that BLite.Core and its source-generated mappers are compatible.");
        }
    }

    // ── Internal helpers ─────────────────────────────────────────────────────

    private static void TraceWarning(int eventId, string message)
        => Trace.TraceWarning($"[BLite GDPR EventId:{eventId}] {message}");
}
