using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BLite.Bson;
using System.Text.Json;

namespace BLite.Benchmark;

[InProcess]
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[HtmlExporter]
[JsonExporterAttribute.Full]
public class SerializationBenchmarks
{
    private const int BatchSize = 10000;
    private Person _person = null!;
    private List<Person> _people = null!;
    private PersonMapper _mapper = new PersonMapper();
    private byte[] _bsonData = Array.Empty<byte>();
    private byte[] _jsonData = Array.Empty<byte>();
    
    private List<byte[]> _bsonDataList = new();
    private List<byte[]> _jsonDataList = new();
    
    private byte[] _serializeBuffer = Array.Empty<byte>();

    [GlobalSetup]
    public void Setup()
    {
        _person = CreatePerson(0);
        _people = new List<Person>(BatchSize);
        for (int i = 0; i < BatchSize; i++)
        {
            _people.Add(CreatePerson(i));
        }
        
        // Pre-allocate buffer for BSON serialization
        _serializeBuffer = new byte[8192];
        
        // Single item data
        var len = _mapper.Serialize(_person, _serializeBuffer);
        _bsonData = _serializeBuffer.AsSpan(0, len).ToArray();
        _jsonData = JsonSerializer.SerializeToUtf8Bytes(_person);

        // List data
        foreach (var p in _people)
        {
            len = _mapper.Serialize(p, _serializeBuffer);
            _bsonDataList.Add(_serializeBuffer.AsSpan(0, len).ToArray());
            _jsonDataList.Add(JsonSerializer.SerializeToUtf8Bytes(p));
        }
    }

    private Person CreatePerson(int i)
    {
        var p = new Person
        {
            Id = ObjectId.NewObjectId(),
            FirstName = $"First_{i}",
            LastName = $"Last_{i}",
            Age = 25,
            Bio = null, 
            CreatedAt = DateTime.UtcNow,
            Balance = 1000.50m,
            HomeAddress = new Address 
            {
                Street = $"{i} Main St",
                City = "Tech City",
                ZipCode = "12345"
            }
        };

        for(int j=0; j<10; j++)
        {
            p.EmploymentHistory.Add(new WorkHistory
            {
                CompanyName = $"TechCorp_{i}_{j}",
                Title = "Developer",
                DurationYears = j,
                Tags = new List<string> { "C#", "BSON", "Performance", "Database", "Complex" }
            });
        }
        return p;
    }

    [Benchmark(Description = "Serialize Single (BSON)")]
    [BenchmarkCategory("Single")]
    public void Serialize_Bson()
    {
        _mapper.Serialize(_person, _serializeBuffer);
    }

    [Benchmark(Description = "Serialize Single (JSON)")]
    [BenchmarkCategory("Single")]
    public void Serialize_Json()
    {
        JsonSerializer.SerializeToUtf8Bytes(_person);
    }
    
    [Benchmark(Description = "Deserialize Single (BSON)")]
    [BenchmarkCategory("Single")]
    public Person Deserialize_Bson()
    {
        return _mapper.Deserialize(_bsonData);
    }

    [Benchmark(Description = "Deserialize Single (JSON)")]
    [BenchmarkCategory("Single")]
    public Person? Deserialize_Json()
    {
        return JsonSerializer.Deserialize<Person>(_jsonData);
    }

    [Benchmark(Description = "Serialize List 10k (BSON loop)")]
    [BenchmarkCategory("Batch")]
    public void Serialize_List_Bson()
    {
        foreach (var p in _people)
        {
            _mapper.Serialize(p, _serializeBuffer);
        }
    }

    [Benchmark(Description = "Serialize List 10k (JSON loop)")]
    [BenchmarkCategory("Batch")]
    public void Serialize_List_Json()
    {
        foreach (var p in _people)
        {
            JsonSerializer.SerializeToUtf8Bytes(p);
        }
    }

    [Benchmark(Description = "Deserialize List 10k (BSON loop)")]
    [BenchmarkCategory("Batch")]
    public void Deserialize_List_Bson()
    {
        foreach (var data in _bsonDataList)
        {
            _mapper.Deserialize(data);
        }
    }

    [Benchmark(Description = "Deserialize List 10k (JSON loop)")]
    [BenchmarkCategory("Batch")]
    public void Deserialize_List_Json()
    {
        foreach (var data in _jsonDataList)
        {
            JsonSerializer.Deserialize<Person>(data);
        }
    }
}
