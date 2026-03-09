using System;
using System.Globalization;
using Avalonia.Controls;
using Avalonia.Data.Converters;

namespace BLite.Studio.Converters;

/// <summary>
/// Returns <c>GridLength(1, Star)</c> when the bound bool is <c>true</c>,
/// or <c>GridLength(0, Pixel)</c> when <c>false</c>.
/// Used to collapse a Grid column to zero without needing a visibility-based layout trick.
/// </summary>
public sealed class BoolToStarGridLengthConverter : IValueConverter
{
    public static readonly BoolToStarGridLengthConverter Instance = new();

    public object Convert(object? value, Type targetType, object? parameter, CultureInfo culture)
        => value is true
            ? new GridLength(1, GridUnitType.Star)
            : new GridLength(0, GridUnitType.Pixel);

    public object ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture)
        => throw new NotSupportedException();
}
