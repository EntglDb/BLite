using System;

namespace BLite.Bson
{
    [AttributeUsage(AttributeTargets.Property)]
    public class BsonIdAttribute : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class BsonIgnoreAttribute : Attribute
    {
    }
}
