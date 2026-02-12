using BLite.Core.Indexing.Internal;

namespace BLite.Core.Indexing;

public static class SpatialMath
{
    private const double EarthRadiusKm = 6371.0;

    /// <summary>
    /// Calculates distance between two points on Earth using Haversine formula.
    /// Result in kilometers.
    /// </summary>
    internal static double DistanceKm(GeoPoint p1, GeoPoint p2) => DistanceKm(p1.Latitude, p1.Longitude, p2.Latitude, p2.Longitude);

    public static double DistanceKm(double lat1, double lon1, double lat2, double lon2)
    {
        double dLat = ToRadians(lat2 - lat1);
        double dLon = ToRadians(lon2 - lon1);

        double a = Math.Sin(dLat / 2) * Math.Sin(dLat / 2) +
                   Math.Cos(ToRadians(lat1)) * Math.Cos(ToRadians(lat2)) *
                   Math.Sin(dLon / 2) * Math.Sin(dLon / 2);

        double c = 2 * Math.Atan2(Math.Sqrt(a), Math.Sqrt(1 - a));
        return EarthRadiusKm * c;
    }

    /// <summary>
    /// Creates a Bounding Box (MBR) centered at a point with a given radius in km.
    /// </summary>
    internal static GeoBox BoundingBox(GeoPoint center, double radiusKm) => BoundingBox(center.Latitude, center.Longitude, radiusKm);

    internal static GeoBox InternalBoundingBox(double lat, double lon, double radiusKm) => BoundingBox(lat, lon, radiusKm);

    internal static GeoBox BoundingBox(double lat, double lon, double radiusKm)
    {
        double dLat = ToDegrees(radiusKm / EarthRadiusKm);
        double dLon = ToDegrees(radiusKm / (EarthRadiusKm * Math.Cos(ToRadians(lat))));

        return new GeoBox(
            lat - dLat,
            lon - dLon,
            lat + dLat,
            lon + dLon);
    }

    private static double ToRadians(double degrees) => degrees * Math.PI / 180.0;
    private static double ToDegrees(double radians) => radians * 180.0 / Math.PI;
}
