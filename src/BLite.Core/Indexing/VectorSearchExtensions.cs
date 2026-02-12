namespace BLite.Core;

public static class VectorSearchExtensions
{
    /// <summary>
    /// Performs a similarity search on a vector property.
    /// This method is a marker for the LINQ query provider and is optimized using HNSW indexes if available.
    /// </summary>
    /// <param name="vector">The vector property of the entity.</param>
    /// <param name="query">The query vector to compare against.</param>
    /// <param name="k">Number of nearest neighbors to return.</param>
    /// <returns>True if the document is part of the top-k results (always returns true when evaluated in memory for compilation purposes).</returns>
    public static bool VectorSearch(this float[] vector, float[] query, int k)
    {
        return true;
    }

    /// <summary>
    /// Performs a similarity search on a collection of vector properties.
    /// Used for entities with multiple vectors per document.
    /// </summary>
    public static bool VectorSearch(this IEnumerable<float[]> vectors, float[] query, int k)
    {
        return true;
    }
}
