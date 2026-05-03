// BLite.Core — Compiler polyfills for netstandard2.1
// Provides BCL types required by C# 9+ / 10+ / 11+ language features
// (init, record, record struct, required) that are not built into netstandard2.1.

#if NETSTANDARD2_1

namespace System.Runtime.CompilerServices
{
    // Required by init-only setters (C# 9) and record types.
    internal static class IsExternalInit { }

    // Required by the `required` keyword on members (C# 11).
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct | AttributeTargets.Field | AttributeTargets.Property, AllowMultiple = false, Inherited = false)]
    internal sealed class RequiredMemberAttribute : Attribute { }

    // Required by the compiler when the `required` keyword or ref-fields are used (C# 11+).
    [AttributeUsage(AttributeTargets.All, AllowMultiple = true, Inherited = false)]
    internal sealed class CompilerFeatureRequiredAttribute : Attribute
    {
        public CompilerFeatureRequiredAttribute(string featureName) => FeatureName = featureName;
        public string FeatureName { get; }
        public const string RefStructs = nameof(RefStructs);
        public const string RequiredMembers = nameof(RequiredMembers);
    }
}

namespace System.Diagnostics.CodeAnalysis
{
    // Required on constructors that initialise all `required` members (C# 11).
    [AttributeUsage(AttributeTargets.Constructor, AllowMultiple = false, Inherited = false)]
    internal sealed class SetsRequiredMembersAttribute : Attribute { }

    // AOT/trim analysis attributes — available in .NET 5+ natively.
    [AttributeUsage(AttributeTargets.Constructor | AttributeTargets.Method, Inherited = false)]
    internal sealed class RequiresDynamicCodeAttribute : Attribute
    {
        public RequiresDynamicCodeAttribute(string message) => Message = message;
        public string Message { get; }
    }

    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Constructor | AttributeTargets.Method, Inherited = false)]
    internal sealed class RequiresUnreferencedCodeAttribute : Attribute
    {
        public RequiresUnreferencedCodeAttribute(string message) => Message = message;
        public string Message { get; }
    }

    [AttributeUsage(AttributeTargets.All, AllowMultiple = true)]
    internal sealed class UnconditionalSuppressMessageAttribute : Attribute
    {
        public UnconditionalSuppressMessageAttribute(string category, string checkId) { }
        public string? Justification { get; set; }
    }
}

namespace System.Collections.Generic
{
    /// <summary>
    /// Minimal binary min-heap priority queue for netstandard2.1 compatibility.
    /// Mirrors the public API surface of System.Collections.Generic.PriorityQueue{TElement,TPriority}
    /// introduced in .NET 6.
    /// </summary>
    internal sealed class PriorityQueue<TElement, TPriority>
    {
        private readonly List<(TElement Element, TPriority Priority)> _heap = new();
        private readonly Comparer<TPriority> _comparer = Comparer<TPriority>.Default;

        public int Count => _heap.Count;

        public void Enqueue(TElement element, TPriority priority)
        {
            _heap.Add((element, priority));
            SiftUp(_heap.Count - 1);
        }

        public TElement Dequeue()
        {
            if (_heap.Count == 0) throw new InvalidOperationException("Queue is empty.");
            var min = _heap[0].Element;
            int last = _heap.Count - 1;
            _heap[0] = _heap[last];
            _heap.RemoveAt(last);
            if (_heap.Count > 0) SiftDown(0);
            return min;
        }

        public bool TryPeek(out TElement element, out TPriority priority)
        {
            if (_heap.Count == 0) { element = default!; priority = default!; return false; }
            element = _heap[0].Element;
            priority = _heap[0].Priority;
            return true;
        }

        private void SiftUp(int i)
        {
            while (i > 0)
            {
                int parent = (i - 1) / 2;
                if (_comparer.Compare(_heap[parent].Priority, _heap[i].Priority) <= 0) break;
                var tmp = _heap[i]; _heap[i] = _heap[parent]; _heap[parent] = tmp;
                i = parent;
            }
        }

        private void SiftDown(int i)
        {
            int n = _heap.Count;
            while (true)
            {
                int smallest = i;
                int l = 2 * i + 1, r = 2 * i + 2;
                if (l < n && _comparer.Compare(_heap[l].Priority, _heap[smallest].Priority) < 0) smallest = l;
                if (r < n && _comparer.Compare(_heap[r].Priority, _heap[smallest].Priority) < 0) smallest = r;
                if (smallest == i) break;
                var tmp = _heap[i]; _heap[i] = _heap[smallest]; _heap[smallest] = tmp;
                i = smallest;
            }
        }
    }
}

#endif

