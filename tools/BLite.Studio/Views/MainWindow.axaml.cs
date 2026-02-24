using Avalonia.Controls;
using Avalonia.Interactivity;
using Avalonia.Platform.Storage;
using BLite.Studio.ViewModels;

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
}
