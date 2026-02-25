using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using BLite.Bson;
using BLite.Core;
using BLite.Core.Indexing;
using BLite.Core.Query.Blql;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;

namespace BLite.Studio.ViewModels.Explorer;

public sealed partial class CollectionDetailViewModel : ObservableObject
{
    private const int PageSize = 25;

    private readonly BLiteEngine       _engine;
    private readonly DynamicCollection _collection;

    public CollectionDetailViewModel(string name, BLiteEngine engine)
    {
        Name        = name;
        _engine     = engine;
        _collection = engine.GetOrCreateCollection(name);

        RefreshMetadata();
        RefreshIndexes();
        LoadPage();
    }

    // ── Metadata ──────────────────────────────────────────────────────────────
    public string Name   { get; }
    public string IdType => _collection.IdType.ToString();

    // ── Tabs ──────────────────────────────────────────────────────────────────
    [ObservableProperty] private int _selectedTabIndex;

    // ── Index list ────────────────────────────────────────────────────────────
    public ObservableCollection<DynamicIndexDescriptor> IndexList { get; } = [];

    public bool HasNoIndexes => IndexList.Count == 0;

    [ObservableProperty] private string _newIndexField  = "";
    [ObservableProperty] private string _newIndexName   = "";
    [ObservableProperty] private bool   _isNewIndexUnique;

    [ObservableProperty] private bool _isBTreeIndexType = true;
    [ObservableProperty] private bool _isVectorIndexType;
    [ObservableProperty] private bool _isSpatialIndexType;

    [ObservableProperty] private int    _newIndexDimensions = 1536;
    [ObservableProperty] private string _newIndexMetric     = "Cosine";

    public List<string> AvailableMetrics { get; } = ["Cosine", "L2", "DotProduct"];

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(IndexIsOk))]
    private string _indexMessage = "";

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(IndexIsOk))]
    private bool _indexIsError;

    public bool IndexIsOk => !IndexIsError && !string.IsNullOrEmpty(IndexMessage);

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(CurrentPageDisplay))]
    [NotifyPropertyChangedFor(nameof(CanGoPrev))]
    [NotifyPropertyChangedFor(nameof(CanGoNext))]
    private int _documentCount;

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(CurrentPageDisplay))]
    [NotifyPropertyChangedFor(nameof(CanGoPrev))]
    [NotifyPropertyChangedFor(nameof(CanGoNext))]
    private int _totalPages;

    // ── Pagination ────────────────────────────────────────────────────────────
    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(CurrentPageDisplay))]
    [NotifyPropertyChangedFor(nameof(CanGoPrev))]
    [NotifyPropertyChangedFor(nameof(CanGoNext))]
    private int _currentPage;

    public string CurrentPageDisplay => DocumentCount == 0
        ? "No documents"
        : $"Page {CurrentPage + 1} of {TotalPages}  ({DocumentCount} total)";

    public bool CanGoPrev => CurrentPage > 0;
    public bool CanGoNext => CurrentPage < TotalPages - 1;

    // ── Documents grid ────────────────────────────────────────────────────────
    public ObservableCollection<DocumentRowViewModel> Documents { get; } = [];

    // ── Editor panel ──────────────────────────────────────────────────────────
    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(IsEditorOpen))]
    [NotifyPropertyChangedFor(nameof(EditorTitle))]
    private DocumentRowViewModel? _selectedRow;

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(IsEditorOpen))]
    [NotifyPropertyChangedFor(nameof(EditorTitle))]
    private bool _isInsertMode;

    public bool   IsEditorOpen => SelectedRow is not null || IsInsertMode;
    public string EditorTitle  => IsInsertMode ? "New document" : (SelectedRow?.Id ?? "");

    [ObservableProperty] private string _editorJson    = "";
    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(EditorIsOk))]
    private string _editorMessage = "";
    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(EditorIsOk))]
    private bool _editorIsError;

    public bool EditorIsOk => !EditorIsError && !string.IsNullOrEmpty(EditorMessage);

    // ── Schema / Vector Source ────────────────────────────────────────────────
    public ObservableCollection<SchemaFieldViewModel> SchemaFields { get; } = [];
    public ObservableCollection<VectorSourceFieldViewModel> VectorSourceFields { get; } = [];
    [ObservableProperty] private string _vectorSourceSeparator = " ";

    [ObservableProperty] private string _newVectorSourceFieldPath = "";
    [ObservableProperty] private string? _newVectorSourcePrefix;
    [ObservableProperty] private string? _newVectorSourceSuffix;

    [ObservableProperty] private string _newSchemaFieldName = "";
    [ObservableProperty] private BsonType _newSchemaFieldType = BsonType.String;

    public List<BsonType> AvailableBsonTypes { get; } = Enum.GetValues<BsonType>().ToList();

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(SchemaIsOk))]
    private string _schemaMessage = "";
    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(SchemaIsOk))]
    private bool _schemaIsError;

    public bool SchemaIsOk => !SchemaIsError && !string.IsNullOrEmpty(SchemaMessage);

    [ObservableProperty] private bool _hasSchema;

    // ── BLQL Query ────────────────────────────────────────────────────────────
    [ObservableProperty] private string _blqlFilterJson = "";
    [ObservableProperty] private string _blqlSortJson   = "";
    [ObservableProperty] private int    _blqlSkip       = 0;
    [ObservableProperty] private int    _blqlTake       = 100;

    public ObservableCollection<DocumentRowViewModel> BlqlResults { get; } = [];

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(BlqlStatusIsOk))]
    private string _blqlStatusMessage = "";

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(BlqlStatusIsOk))]
    private bool _blqlIsError;

    public bool BlqlStatusIsOk => !BlqlIsError && !string.IsNullOrEmpty(BlqlStatusMessage);

    [ObservableProperty] private bool _blqlHasResults;
    [ObservableProperty] private int  _blqlResultCount;

    // ── TimeSeries ────────────────────────────────────────────────────────────
    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(TsRetentionLabel))]
    private bool _isTimeSeries;

    [ObservableProperty] private string _tsTtlFieldName = "";
    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(TsRetentionLabel))]
    private double _tsRetentionDays = 30;

    public string TsRetentionLabel => IsTimeSeries
        ? $"Retention: {TsRetentionDays:0.##} day(s)  ({TsRetentionDays * 24 * 60:0} minutes)"
        : "Not configured";

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(TsIsOk))]
    private string _tsMessage = "";
    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(TsIsOk))]
    private bool _tsIsError;
    public bool TsIsOk => !TsIsError && !string.IsNullOrEmpty(TsMessage);

    // ── Commands ──────────────────────────────────────────────────────────────
    [RelayCommand]
    private void PrevPage() { CurrentPage--; LoadPage(); }

    [RelayCommand]
    private void NextPage() { CurrentPage++; LoadPage(); }

    [RelayCommand]
    private void RunBlqlQuery()
    {
        BlqlStatusMessage = "";
        BlqlIsError       = false;
        BlqlResults.Clear();
        BlqlHasResults    = false;
        BlqlResultCount   = 0;

        try
        {
            var filter = string.IsNullOrWhiteSpace(BlqlFilterJson)
                ? BlqlFilter.Empty
                : BlqlFilterParser.Parse(BlqlFilterJson);

            var query = _collection.Query(filter);

            if (!string.IsNullOrWhiteSpace(BlqlSortJson))
                query = query.Sort(BlqlSortJson);

            if (BlqlSkip > 0)  query = query.Skip(BlqlSkip);
            if (BlqlTake > 0)  query = query.Take(BlqlTake);

            foreach (var doc in query.AsEnumerable())
            {
                var fields = doc.EnumerateFields();
                doc.TryGetId(out var bsonId);
                var idStr   = fields.FirstOrDefault(f => f.Name == "_id").Value.ToString() ?? "";
                var content = string.Join("  |  ",
                    fields.Where(f => f.Name != "_id").Select(f => $"{f.Name}: {f.Value}"));
                BlqlResults.Add(new DocumentRowViewModel
                {
                    BsonId     = bsonId,
                    Id         = idStr,
                    Content    = content,
                    SizeBytes  = doc.Size
                });
            }

            BlqlResultCount   = BlqlResults.Count;
            BlqlHasResults    = BlqlResultCount > 0;
            BlqlStatusMessage = BlqlResultCount == 0
                ? "No documents matched."
                : $"{BlqlResultCount} document{(BlqlResultCount == 1 ? "" : "s")} matched.";
        }
        catch (Exception ex)
        {
            BlqlStatusMessage = ex.Message;
            BlqlIsError       = true;
        }
    }

    [RelayCommand]
    private void ClearBlqlQuery()
    {
        BlqlFilterJson    = "";
        BlqlSortJson      = "";
        BlqlSkip          = 0;
        BlqlTake          = 100;
        BlqlStatusMessage = "";
        BlqlIsError       = false;
        BlqlResults.Clear();
        BlqlHasResults  = false;
        BlqlResultCount = 0;
    }

    [RelayCommand]
    private void SaveTimeSeries()
    {
        TsMessage = "";
        TsIsError = false;
        if (TsRetentionDays <= 0)
        {
            TsMessage = "Retention must be greater than zero.";
            TsIsError = true;
            return;
        }
        try
        {
            var ttlField = TsTtlFieldName.Trim();
            _collection.SetTimeSeries(ttlField, TimeSpan.FromDays(TsRetentionDays));
            _engine.Commit();
            IsTimeSeries = true;
            var fieldLabel = string.IsNullOrEmpty(ttlField) ? "insertion time" : $"field '{ttlField}'";
            TsMessage = $"TimeSeries enabled. Timestamp: {fieldLabel}. Retention: {TsRetentionDays:0.##} day(s).";
        }
        catch (Exception ex)
        {
            TsMessage = ex.Message;
            TsIsError = true;
        }
    }

    [RelayCommand]
    private void ForcePrune()
    {
        TsMessage = "";
        TsIsError = false;
        try
        {
            _collection.ForcePrune();
            _engine.Commit();
            RefreshMetadata();
            LoadPage();
            TsMessage = "Pruning completed.";
        }
        catch (Exception ex)
        {
            TsMessage = ex.Message;
            TsIsError = true;
        }
    }

    [RelayCommand]
    private void CreateIndex()
    {
        IndexMessage = "";
        IndexIsError = false;
        var field = NewIndexField.Trim();
        if (string.IsNullOrEmpty(field))
        {
            IndexMessage = "Field path is required.";
            IndexIsError = true;
            return;
        }
        try
        {
            var name = string.IsNullOrWhiteSpace(NewIndexName) ? null : NewIndexName.Trim();

            if (IsVectorIndexType)
            {
                var metric = NewIndexMetric switch
                {
                    "L2"         => VectorMetric.L2,
                    "DotProduct" => VectorMetric.DotProduct,
                    _            => VectorMetric.Cosine
                };
                _collection.CreateVectorIndex(field, NewIndexDimensions, metric, name);
            }
            else if (IsSpatialIndexType)
            {
                _collection.CreateSpatialIndex(field, name);
            }
            else
            {
                _collection.CreateIndex(field, name, IsNewIndexUnique);
            }

            _engine.Commit();

            NewIndexField    = "";
            NewIndexName     = "";
            IsNewIndexUnique = false;
            RefreshIndexes();
            IndexMessage = $"Index created.";
        }
        catch (Exception ex)
        {
            IndexMessage = ex.Message;
            IndexIsError = true;
        }
    }

    [RelayCommand]
    private void DropIndex(string? indexName)
    {
        if (string.IsNullOrEmpty(indexName)) return;
        IndexMessage = "";
        IndexIsError = false;
        try
        {
            var ok = _collection.DropIndex(indexName);
            _engine.Commit();
            RefreshIndexes();
            IndexMessage = ok ? $"Index '{indexName}' dropped." : $"Index '{indexName}' not found.";
            IndexIsError = !ok;
        }
        catch (Exception ex)
        {
            IndexMessage = ex.Message;
            IndexIsError = true;
        }
    }

    [RelayCommand]
    private void OpenRow(DocumentRowViewModel? row)
    {
        if (row is null) return;
        if (SelectedRow == row) { CloseEditor(); return; }

        IsInsertMode   = false;
        EditorMessage  = "";
        EditorIsError  = false;
        SelectedRow    = row;

        var doc = _collection.FindById(row.BsonId);
        EditorJson = doc is not null ? BsonJsonConverter.ToJson(doc) : "{ }";
    }

    [RelayCommand]
    private void NewDocument()
    {
        SelectedRow   = null;
        IsInsertMode  = true;
        EditorMessage = "";
        EditorIsError = false;
        EditorJson    = "{\n  \"field\": \"value\"\n}";
    }

    [RelayCommand]
    private void CloseEditor()
    {
        SelectedRow  = null;
        IsInsertMode = false;
        EditorJson   = "";
        EditorMessage = "";
    }

    [RelayCommand]
    private void SaveDocument()
    {
        EditorMessage = "";
        EditorIsError = false;
        try
        {
            var keyMap    = (ConcurrentDictionary<string, ushort>)_engine.GetKeyMap();
            var reverseMap = (ConcurrentDictionary<ushort, string>)_engine.GetKeyReverseMap();
            var doc       = BsonJsonConverter.FromJson(EditorJson, keyMap, reverseMap);

            if (IsInsertMode)
            {
                _collection.Insert(doc);
                _engine.Commit();
                EditorMessage = "Document inserted.";
                CloseEditor();
            }
            else if (SelectedRow is not null)
            {
                var ok = _collection.Update(SelectedRow.BsonId, doc);
                _engine.Commit();
                EditorMessage = ok ? "Saved." : "Document not found.";
                EditorIsError = !ok;
            }

            RefreshMetadata();
            LoadPage();
        }
        catch (Exception ex)
        {
            EditorMessage = ex.Message;
            EditorIsError = true;
        }
    }

    [RelayCommand]
    private void DeleteDocument(DocumentRowViewModel? row)
    {
        if (row is null) return;
        EditorMessage = "";
        EditorIsError = false;
        try
        {
            _collection.Delete(row.BsonId);
            _engine.Commit();
            if (SelectedRow == row) CloseEditor();
            RefreshMetadata();
            LoadPage();
        }
        catch (Exception ex)
        {
            EditorMessage = ex.Message;
            EditorIsError = true;
        }
    }

    [RelayCommand]
    private void AddVectorSourceField()
    {
        if (string.IsNullOrWhiteSpace(NewVectorSourceFieldPath)) return;
        VectorSourceFields.Add(new VectorSourceFieldViewModel
        {
            Path = NewVectorSourceFieldPath.Trim(),
            Prefix = NewVectorSourcePrefix,
            Suffix = NewVectorSourceSuffix
        });
        NewVectorSourceFieldPath = "";
        NewVectorSourcePrefix = null;
        NewVectorSourceSuffix = null;
    }

    [RelayCommand]
    private void RemoveVectorSourceField(VectorSourceFieldViewModel? field)
    {
        if (field is not null) VectorSourceFields.Remove(field);
    }

    [RelayCommand]
    private void AddSchemaField()
    {
        if (string.IsNullOrWhiteSpace(NewSchemaFieldName)) return;
        SchemaFields.Add(new SchemaFieldViewModel 
        { 
            Name = NewSchemaFieldName.Trim(), 
            Type = NewSchemaFieldType 
        });
        NewSchemaFieldName = "";
    }

    [RelayCommand]
    private void RemoveSchemaField(SchemaFieldViewModel? field)
    {
        if (field is not null) SchemaFields.Remove(field);
    }

    [RelayCommand]
    private void InferSchema()
    {
        try
        {
            var docs = _collection.FindAll().Take(20).ToList();
            if (docs.Count == 0)
            {
                SchemaMessage = "No documents found to infer schema.";
                SchemaIsError = true;
                return;
            }

            foreach (var doc in docs)
            {
                foreach (var field in doc.EnumerateFields())
                {
                    if (field.Name == "_id") continue;
                    if (SchemaFields.Any(f => f.Name == field.Name)) continue;
                    
                    SchemaFields.Add(new SchemaFieldViewModel
                    {
                        Name = field.Name,
                        Type = field.Value.Type,
                        IsNullable = true
                    });
                }
            }
            SchemaMessage = "Schema inferred from first 20 documents.";
            SchemaIsError = false;
        }
        catch (Exception ex)
        {
            SchemaMessage = "Inference failed: " + ex.Message;
            SchemaIsError = true;
        }
    }

    [RelayCommand]
    private void AddSourcedFieldFromSchema(SchemaFieldViewModel? field)
    {
        if (field is null) return;
        
        // Prevent duplicates
        if (VectorSourceFields.Any(v => v.Path == field.Name)) return;

        VectorSourceFields.Add(new VectorSourceFieldViewModel
        {
            Path = field.Name,
            Prefix = $"{field.Name}: "
        });
    }

    [RelayCommand]
    private void SaveSchema()
    {
        SchemaMessage = "";
        SchemaIsError = false;
        try
        {
            // Update BSON Schema
            if (SchemaFields.Count > 0)
            {
                var bsonSchema = new BsonSchema { Title = Name, Version = 1 };
                foreach (var s in SchemaFields)
                {
                    bsonSchema.Fields.Add(new BsonField
                    {
                        Name = s.Name,
                        Type = s.Type,
                        IsNullable = s.IsNullable
                    });
                }
                _collection.SetSchema(bsonSchema);
                HasSchema = true;
            }

            // Update Vector Source config
            var config = new BLite.Core.Storage.VectorSourceConfig
            {
                Separator = VectorSourceSeparator ?? " "
            };
            foreach (var vm in VectorSourceFields)
            {
                config.Fields.Add(new BLite.Core.Storage.VectorSourceField
                {
                    Path = vm.Path,
                    Prefix = vm.Prefix,
                    Suffix = vm.Suffix
                });
            }
            _collection.SetVectorSource(config);

            _engine.Commit();
            SchemaMessage = "Schema saved.";
        }
        catch (Exception ex)
        {
            SchemaMessage = ex.Message;
            SchemaIsError = true;
        }
    }

    // ── Internal ──────────────────────────────────────────────────────────────
    private void RefreshMetadata()
    {
        DocumentCount = _collection.Count();
        TotalPages    = Math.Max(1, (DocumentCount + PageSize - 1) / PageSize);
        if (CurrentPage >= TotalPages) CurrentPage = Math.Max(0, TotalPages - 1);
        RefreshSchema();
        RefreshTimeSeries();
    }

    private void RefreshSchema()
    {
        VectorSourceFields.Clear();
        var config = _collection.GetVectorSource();
        if (config != null)
        {
            VectorSourceSeparator = config.Separator;
            foreach (var field in config.Fields)
            {
                VectorSourceFields.Add(new VectorSourceFieldViewModel
                {
                    Path = field.Path,
                    Prefix = field.Prefix,
                    Suffix = field.Suffix
                });
            }
        }
        else
        {
            VectorSourceSeparator = " ";
        }

        SchemaFields.Clear();
        var schemas = _collection.GetSchemas();
        HasSchema = schemas.Count > 0;
        if (HasSchema)
        {
            // Just take the latest (last) version of the schema
            var latest = schemas[schemas.Count - 1];
            foreach (var f in latest.Fields)
            {
                SchemaFields.Add(new SchemaFieldViewModel
                {
                    Name = f.Name,
                    Type = f.Type,
                    IsNullable = f.IsNullable
                });
            }
        }
    }

    private void RefreshTimeSeries()
    {
        IsTimeSeries = _collection.IsTimeSeries;
        if (IsTimeSeries)
        {
            var (retMs, ttlField) = _collection.GetTimeSeriesConfig();
            TsTtlFieldName   = ttlField ?? "";
            TsRetentionDays  = retMs > 0 ? retMs / 86_400_000.0 : 30;
        }
    }

    private void RefreshIndexes()
    {
        IndexList.Clear();
        foreach (var idx in _collection.GetIndexDescriptors())
            IndexList.Add(idx);
        OnPropertyChanged(nameof(HasNoIndexes));
    }

    private void LoadPage()
    {
        Documents.Clear();

        var rows = _collection.FindAll()
            .Skip(CurrentPage * PageSize)
            .Take(PageSize);

        foreach (var doc in rows)
        {
            var fields = doc.EnumerateFields();
            doc.TryGetId(out var bsonId);

            var idStr = fields.FirstOrDefault(f => f.Name == "_id").Value.ToString() ?? "";
            var content = string.Join("  |  ",
                fields.Where(f => f.Name != "_id").Select(f => $"{f.Name}: {f.Value}"));

            Documents.Add(new DocumentRowViewModel { BsonId = bsonId, Id = idStr, Content = content, SizeBytes = doc.Size });
        }
    }
}
