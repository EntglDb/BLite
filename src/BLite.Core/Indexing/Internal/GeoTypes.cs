namespace BLite.Core.Indexing.Internal;

/// <summary>
/// Basic spatial point (Latitude/Longitude)
/// Internal primitive for R-Tree logic.
/// </summary>
internal record struct GeoPoint(double Latitude, double Longitude)
{
    public static GeoPoint Empty => new(0, 0);
}

/// <summary>
/// Minimum Bounding Box (MBR) for spatial indexing
/// Internal primitive for R-Tree logic.
/// </summary>
internal record struct GeoBox(double MinLat, double MinLon, double MaxLat, double MaxLon)
{
    public static GeoBox Empty => new(double.MaxValue, double.MaxValue, double.MinValue, double.MinValue);

    public bool Contains(GeoPoint point)
    {
        return point.Latitude >= MinLat && point.Latitude <= MaxLat &&
               point.Longitude >= MinLon && point.Longitude <= MaxLon;
    }

    public bool Intersects(GeoBox other)
    {
        return !(other.MinLat > MaxLat || other.MaxLat < MinLat ||
                 other.MinLon > MaxLon || other.MaxLon < MinLon);
    }

    public static GeoBox FromPoint(GeoPoint point)
    {
        return new GeoBox(point.Latitude, point.Longitude, point.Latitude, point.Longitude);
    }

    public GeoBox ExpandTo(GeoPoint point)
    {
        return new GeoBox(
            Math.Min(MinLat, point.Latitude),
            Math.Min(MinLon, point.Longitude),
            Math.Max(MaxLat, point.Latitude),
            Math.Max(MaxLon, point.Longitude));
    }

    public GeoBox ExpandTo(GeoBox other)
    {
        return new GeoBox(
            Math.Min(MinLat, other.MinLat),
            Math.Min(MinLon, other.MinLon),
            Math.Max(MaxLat, other.MaxLat),
            Math.Max(MaxLon, other.MaxLon));
    }

    public double Area => Math.Max(0, MaxLat - MinLat) * Math.Max(0, MaxLon - MinLon);
}
