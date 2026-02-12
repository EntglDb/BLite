using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using System.Runtime.InteropServices;
using System.Numerics;

namespace BLite.Core.Indexing;

/// <summary>
/// Optimized vector math utilities using SIMD if available.
/// </summary>
public static class VectorMath
{
    public static float Distance(ReadOnlySpan<float> v1, ReadOnlySpan<float> v2, VectorMetric metric)
    {
        return metric switch
        {
            VectorMetric.Cosine => 1.0f - CosineSimilarity(v1, v2),
            VectorMetric.L2 => EuclideanDistanceSquared(v1, v2),
            VectorMetric.DotProduct => -DotProduct(v1, v2), // HNSW uses "distance" so smaller is better
            _ => throw new ArgumentException($"Unsupported metric: {metric}")
        };
    }

    public static float CosineSimilarity(ReadOnlySpan<float> v1, ReadOnlySpan<float> v2)
    {
        float dot = DotProduct(v1, v2);
        float mag1 = DotProduct(v1, v1);
        float mag2 = DotProduct(v2, v2);

        if (mag1 == 0 || mag2 == 0) return 0;
        return dot / (MathF.Sqrt(mag1) * MathF.Sqrt(mag2));
    }

    public static float DotProduct(ReadOnlySpan<float> v1, ReadOnlySpan<float> v2)
    {
        if (v1.Length != v2.Length)
            throw new ArgumentException("Vectors must have same length");

        float dot = 0;
        int i = 0;

        // SIMD Optimization for .NET 
        if (Vector.IsHardwareAccelerated && v1.Length >= Vector<float>.Count)
        {
            var vDot = Vector<float>.Zero;
            var v1Span = MemoryMarshal.Cast<float, Vector<float>>(v1);
            var v2Span = MemoryMarshal.Cast<float, Vector<float>>(v2);

            foreach (var chunk in Enumerable.Range(0, v1Span.Length))
            {
                vDot += v1Span[chunk] * v2Span[chunk];
            }
            
            dot = Vector.Dot(vDot, Vector<float>.One);
            i = v1Span.Length * Vector<float>.Count;
        }

        // Remaining elements
        for (; i < v1.Length; i++)
        {
            dot += v1[i] * v2[i];
        }

        return dot;
    }

    public static float EuclideanDistanceSquared(ReadOnlySpan<float> v1, ReadOnlySpan<float> v2)
    {
        if (v1.Length != v2.Length)
            throw new ArgumentException("Vectors must have same length");

        float dist = 0;
        int i = 0;

        if (Vector.IsHardwareAccelerated && v1.Length >= Vector<float>.Count)
        {
            var vDist = Vector<float>.Zero;
            var v1Span = MemoryMarshal.Cast<float, Vector<float>>(v1);
            var v2Span = MemoryMarshal.Cast<float, Vector<float>>(v2);

            foreach (var chunk in Enumerable.Range(0, v1Span.Length))
            {
                var diff = v1Span[chunk] - v2Span[chunk];
                vDist += diff * diff;
            }

            dist = Vector.Dot(vDist, Vector<float>.One);
            i = v1Span.Length * Vector<float>.Count;
        }

        for (; i < v1.Length; i++)
        {
            float diff = v1[i] - v2[i];
            dist += diff * diff;
        }

        return dist;
    }
}
