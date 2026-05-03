using BLite.Core;
using BLite.Core.Collections;

namespace BLite.Shared;

/// <summary>
/// Minimal <see cref="DocumentDbContext"/> used by <c>CdcMaskingTests</c> to exercise
/// WP2 CDC field-masking rules on a typed collection whose entity has a
/// <c>[PersonalData]</c>-annotated field.
/// </summary>
public partial class CdcMaskingTestContext : DocumentDbContext
{
    public DocumentCollection<int, GdprPerson> GdprPeople { get; set; } = null!;

    public CdcMaskingTestContext(string databasePath) : base(databasePath)
    {
        InitializeCollections();
    }
}
