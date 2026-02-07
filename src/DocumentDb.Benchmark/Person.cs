using DocumentDb.Bson;
using System;

namespace DocumentDb.Benchmark;

public class Person
{
    public ObjectId Id { get; set; }
    public string FirstName { get; set; } = string.Empty;
    public string LastName { get; set; } = string.Empty;
    public int Age { get; set; }
    public string Bio { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; }
}
