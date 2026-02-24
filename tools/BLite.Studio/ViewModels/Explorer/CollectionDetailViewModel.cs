using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using BLite.Bson;
using BLite.Core;
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
    public ObservableCollection<string> IndexList { get; } = [];

    public bool HasNoIndexes => IndexList.Count == 0;

    [ObservableProperty] private string _newIndexField  = "";
    [ObservableProperty] private string _newIndexName   = "";
    [ObservableProperty] private bool   _isNewIndexUnique;

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
            _collection.CreateIndex(field, name, IsNewIndexUnique);
            _engine.Commit();
            NewIndexField   = "";
            NewIndexName    = "";
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

    // ── Internal ──────────────────────────────────────────────────────────────
    private void RefreshMetadata()
    {
        DocumentCount = _collection.Count();
        TotalPages    = Math.Max(1, (DocumentCount + PageSize - 1) / PageSize);
        if (CurrentPage >= TotalPages) CurrentPage = Math.Max(0, TotalPages - 1);
    }

    private void RefreshIndexes()
    {
        IndexList.Clear();
        foreach (var idx in _collection.ListIndexes())
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
