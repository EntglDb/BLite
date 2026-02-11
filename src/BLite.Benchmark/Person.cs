using BLite.Bson;
using System;
using System.Collections.Generic;

namespace BLite.Benchmark;

public class Address
{
    public string Street { get; set; } = string.Empty;
    public string City { get; set; } = string.Empty;
    public string ZipCode { get; set; } = string.Empty;
}

public class WorkHistory
{
    public string CompanyName { get; set; } = string.Empty;
    public string Title { get; set; } = string.Empty;
    public int DurationYears { get; set; }
    public List<string> Tags { get; set; } = new();
}

public class Person
{
    public ObjectId Id { get; set; }
    public string FirstName { get; set; } = string.Empty;
    public string LastName { get; set; } = string.Empty;
    public int Age { get; set; }
    public string? Bio { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; }
    
    // Complex fields
    public decimal Balance { get; set; }
    public Address HomeAddress { get; set; } = new();
    public List<WorkHistory> EmploymentHistory { get; set; } = new();
}
