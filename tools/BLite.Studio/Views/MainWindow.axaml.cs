using Avalonia.Controls;
using Avalonia.Interactivity;
using Avalonia.Platform.Storage;
using BLite.Studio.ViewModels;
using BLite.Studio.ViewModels.Explorer;

namespace BLite.Studio.Views;

public partial class MainWindow : Window
{
    public MainWindow()
    {
        InitializeComponent();
    }

    private async void Browse_Click(object? sender, RoutedEventArgs e)
    {
        var topLevel = TopLevel.GetTopLevel(this);
        if (topLevel is null) return;

        var files = await topLevel.StorageProvider.OpenFilePickerAsync(new FilePickerOpenOptions
        {
            Title = "Apri database BLite",
            AllowMultiple = false,
            // Nessun filtro: qualsiasi estensione è accettata
            FileTypeFilter =
            [
                new FilePickerFileType("Database BLite") { Patterns = ["*.db", "*.blite", "*.blt"] },
                new FilePickerFileType("Tutti i file")   { Patterns = ["*.*"] },
            ]
        });

        if (files.Count > 0 && DataContext is MainWindowViewModel vm)
        {
            vm.DatabasePath = files[0].Path.LocalPath;
        }
    }

    private async void GdprExport_Click(object? sender, RoutedEventArgs e)
    {
        var topLevel = TopLevel.GetTopLevel(this);
        if (topLevel is null) return;
        if (DataContext is not MainWindowViewModel mainVm) return;
        if (mainVm.Explorer?.DetailContent is not GdprDetailViewModel gdprVm) return;
        if (string.IsNullOrWhiteSpace(gdprVm.SubjectFieldValue)) return;

        var ext = gdprVm.SelectedFormat switch { "CSV" => "csv", "BSON" => "bson", _ => "json" };

        var file = await topLevel.StorageProvider.SaveFilePickerAsync(new FilePickerSaveOptions
        {
            Title = "Export subject data",
            SuggestedFileName = $"subject-export.{ext}",
            FileTypeChoices =
            [
                new FilePickerFileType("Export file") { Patterns = [$"*.{ext}"] },
                new FilePickerFileType("All files")   { Patterns = ["*.*"] },
            ]
        });

        if (file is null) return;
        await gdprVm.ExportAsync(file.Path.LocalPath);
    }
}
