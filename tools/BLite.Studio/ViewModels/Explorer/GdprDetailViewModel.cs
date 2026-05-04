using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BLite.Bson;
using BLite.Core;
using BLite.Core.GDPR;
using CommunityToolkit.Mvvm.ComponentModel;

namespace BLite.Studio.ViewModels.Explorer;

/// <summary>
/// Row model for a collection entry in the GDPR inspection panel.
/// </summary>
public sealed record GdprCollectionRow(
    string Name,
    string PersonalFieldsDisplay,
    bool   HasPersonalData);

/// <summary>
/// Detail view-model for the GDPR sidebar item.
/// Exposes:
/// <list type="bullet">
///   <item>Art. 30 inspection report (encryption status, audit, layout, collections).</item>
///   <item>Art. 15 / Art. 20 subject-data export form.</item>
/// </list>
/// </summary>
[RequiresUnreferencedCode("GdprDetailViewModel calls InspectDatabase which uses PersonalDataResolver.")]
public sealed partial class GdprDetailViewModel : ObservableObject
{
    private readonly BLiteEngine _engine;

    [RequiresUnreferencedCode("GdprDetailViewModel calls InspectDatabase which uses PersonalDataResolver.")]
    public GdprDetailViewModel(BLiteEngine engine)
    {
        _engine = engine;
        Report  = engine.InspectDatabase();

        CollectionRows = Report.Collections
            .OrderBy(c => c.Name)
            .Select(c => new GdprCollectionRow(
                c.Name,
                c.PersonalDataFields.Count == 0
                    ? "—"
                    : string.Join(", ", c.PersonalDataFields),
                c.PersonalDataFields.Count > 0))
            .ToList();
    }

    // ── Art. 30 — Inspection report ──────────────────────────────────────────

    public DatabaseInspectionReport     Report         { get; }
    public IReadOnlyList<GdprCollectionRow> CollectionRows { get; }

    public string EncryptionStatus => Report.IsEncrypted
        ? "✔  AES-256-GCM enabled"
        : "✗  Not encrypted";

    public string AuditStatus => Report.IsAuditEnabled
        ? "✔  Audit sink registered"
        : "✗  No audit sink";

    public string LayoutStatus => Report.IsMultiFileMode ? "Multi-file" : "Single-file";

    // ── Art. 15 / 20 — Subject-data export ───────────────────────────────────

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(CanExport))]
    private string _subjectFieldName = "email";

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(CanExport))]
    private string _subjectFieldValue = string.Empty;

    public List<string> AvailableFormats { get; } = ["JSON", "CSV", "BSON"];

    [ObservableProperty] private string _selectedFormat = "JSON";

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(CanExport))]
    private bool _isExporting;

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(StatusIsOk))]
    private string _statusMessage = string.Empty;

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(StatusIsOk))]
    private bool _statusIsError;

    public bool StatusIsOk => !StatusIsError && !string.IsNullOrEmpty(StatusMessage);

    public bool CanExport =>
        !string.IsNullOrWhiteSpace(SubjectFieldName) &&
        !string.IsNullOrWhiteSpace(SubjectFieldValue) &&
        !IsExporting;

    /// <summary>
    /// Executes the subject-data export to <paramref name="filePath"/>.
    /// Called from <c>MainWindow.axaml.cs</c> after the user selects a save path.
    /// </summary>
    public async Task ExportAsync(string filePath, CancellationToken ct = default)
    {
        IsExporting   = true;
        StatusIsError = false;
        StatusMessage = "Exporting…";

        try
        {
            var format = SelectedFormat switch
            {
                "CSV"  => SubjectExportFormat.Csv,
                "BSON" => SubjectExportFormat.Bson,
                _      => SubjectExportFormat.Json,
            };

            var query = new SubjectQuery
            {
                FieldName  = SubjectFieldName.Trim(),
                FieldValue = BsonValue.FromString(SubjectFieldValue.Trim()),
                Format     = format,
            };

            await using var report = await _engine.ExportSubjectDataAsync(query, ct).ConfigureAwait(false);
            await report.WriteToFileAsync(filePath, format, ct).ConfigureAwait(false);

            var total = report.DataByCollection.Values.Sum(v => v.Count);
            StatusMessage = $"Exported {total} document(s) → {System.IO.Path.GetFileName(filePath)}";
            StatusIsError = false;
        }
        catch (OperationCanceledException)
        {
            StatusMessage = "Export cancelled.";
            StatusIsError = false;
        }
        catch (Exception ex)
        {
            StatusMessage = $"Export failed: {ex.Message}";
            StatusIsError = true;
        }
        finally
        {
            IsExporting = false;
        }
    }
}
