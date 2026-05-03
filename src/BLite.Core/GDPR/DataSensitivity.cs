namespace BLite.Core.GDPR;

/// <summary>
/// Classifies the sensitivity tier of a personal-data field.
/// The tiers mirror the GDPR distinction between ordinary personal data (Art. 4(1))
/// and special-category data (Art. 9).
/// </summary>
public enum DataSensitivity : byte
{
    /// <summary>Ordinary personal data — name, email, address (Art. 4(1)).</summary>
    Personal = 1,

    /// <summary>Sensitive personal data — health, financial, SSN.</summary>
    Sensitive = 2,

    /// <summary>Special-category data — race, religion, biometrics, etc. (Art. 9).</summary>
    Special = 3,
}
