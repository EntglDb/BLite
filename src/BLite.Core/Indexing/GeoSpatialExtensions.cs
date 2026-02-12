namespace BLite.Core;

public static class GeoSpatialExtensions
{
    /// <summary>
    /// Performs a geospatial proximity search (Near) on a coordinate tuple property.
    /// This method is a marker for the LINQ query provider and is optimized using R-Tree indexes if available.
    /// </summary>
    /// <param name="point">The coordinate tuple (Latitude, Longitude) property of the entity.</param>
    /// <param name="center">The center point (Latitude, Longitude) for the proximity search.</param>
    /// <param name="radiusKm">The radius in kilometers.</param>
    /// <returns>True if the point is within the specified radius.</returns>
    public static bool Near(this (double Latitude, double Longitude) point, (double Latitude, double Longitude) center, double radiusKm)
    {
        return true; 
    }

    /// <summary>
    /// Performs a geospatial bounding box search (Within) on a coordinate tuple property.
    /// This method is a marker for the LINQ query provider and is optimized using R-Tree indexes if available.
    /// </summary>
    /// <param name="point">The coordinate tuple (Latitude, Longitude) property of the entity.</param>
    /// <param name="min">The minimum (Latitude, Longitude) of the bounding box.</param>
    /// <param name="max">The maximum (Latitude, Longitude) of the bounding box.</param>
    /// <returns>True if the point is within the specified bounding box.</returns>
    public static bool Within(this (double Latitude, double Longitude) point, (double Latitude, double Longitude) min, (double Latitude, double Longitude) max)
    {
        return true;
    }
}
