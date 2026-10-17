using System.Diagnostics;
using System.Reflection;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.StaticFiles;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Logging;
using Microsoft.Playwright;

namespace BLite.Wasm.BrowserTests;

/// <summary>
/// Publishes the browser host once, serves it over HTTP on localhost (a secure
/// context, which OPFS needs) and launches the browser.
/// <para>Environment variables:</para>
/// <list type="bullet">
/// <item><c>BLITE_WASM_VARIANT</c>: <c>trimmed</c> (default; Release publish, TrimMode=partial), <c>fulltrim</c>, <c>untrimmed</c> or <c>aot</c> (needs the wasm-tools workload).</item>
/// <item><c>BLITE_WASM_PUBLISH_DIR</c>: serve an existing publish folder instead of publishing.</item>
/// <item><c>BLITE_WASM_BROWSER</c>: <c>chromium</c> (default), <c>firefox</c> or <c>webkit</c>.</item>
/// <item><c>BLITE_WASM_REPORT</c>: JSON-lines file each scenario outcome is appended to.</item>
/// </list>
/// </summary>
public sealed class BrowserFixture : IAsyncLifetime
{
    public string Variant { get; } = Environment.GetEnvironmentVariable("BLITE_WASM_VARIANT") ?? "trimmed";
    public string BrowserName { get; } = Environment.GetEnvironmentVariable("BLITE_WASM_BROWSER") ?? "chromium";
    public string BaseUrl { get; private set; } = string.Empty;
    public IBrowser Browser { get; private set; } = null!;
    public string ReportPath { get; private set; } = string.Empty;
    public string PublishLog { get; private set; } = string.Empty;

    private IPlaywright? _playwright;
    private WebApplication? _server;

    public async Task InitializeAsync()
    {
        var wwwroot = Path.Combine(await PublishAsync(), "wwwroot");
        if (!File.Exists(Path.Combine(wwwroot, "index.html")))
            throw new InvalidOperationException($"No index.html under {wwwroot}.");

        ReportPath = Environment.GetEnvironmentVariable("BLITE_WASM_REPORT")
            ?? Path.Combine(AppContext.BaseDirectory, "TestResults", $"wasm-browser-{BrowserName}-{Variant}.jsonl");
        Directory.CreateDirectory(Path.GetDirectoryName(ReportPath)!);

        _server = StartServer(wwwroot);
        await _server.StartAsync();
        BaseUrl = _server.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First()
            .Replace("127.0.0.1", "localhost");

        // Downloads the browser on first use; a no-op once it is installed.
        var exit = Microsoft.Playwright.Program.Main(["install", BrowserName]);
        if (exit != 0)
            throw new InvalidOperationException($"playwright install {BrowserName} exited with {exit}.");

        _playwright = await Playwright.CreateAsync();
        var type = BrowserName switch
        {
            "firefox" => _playwright.Firefox,
            "webkit" => _playwright.Webkit,
            _ => _playwright.Chromium
        };
        Browser = await type.LaunchAsync(new BrowserTypeLaunchOptions { Headless = true });
    }

    public async Task DisposeAsync()
    {
        if (Browser != null) await Browser.CloseAsync();
        _playwright?.Dispose();
        if (_server != null) await _server.DisposeAsync();
        WriteSummary();
    }

    public void Record(object row)
    {
        lock (this)
            File.AppendAllText(ReportPath, JsonSerializer.Serialize(row) + Environment.NewLine);
    }

    private readonly List<(string Row, string Status, Candidate? Candidate, string? Message)> _rows = new();

    /// <summary>
    /// One line per test row: <c>pass</c>, <c>fail</c>, <c>known</c> (a registered
    /// candidate with an issue) or <c>fixed</c> (a registered row that passed).
    /// </summary>
    public void RecordRow(string row, string status, Candidate? candidate, string? message)
    {
        lock (this) _rows.Add((row, status, candidate, message));
        Record(new
        {
            browser = BrowserName,
            variant = Variant,
            row,
            status,
            candidate = candidate?.Id,
            issue = candidate?.Issue,
            message
        });
    }

    /// <summary>
    /// <c>wasm-browser-&lt;browser&gt;-&lt;variant&gt;.summary.txt</c> next to the report: the
    /// counts per status and one line per row that is not a plain pass.
    /// </summary>
    private void WriteSummary()
    {
        List<(string Row, string Status, Candidate? Candidate, string? Message)> rows;
        lock (this) rows = _rows.ToList();
        if (rows.Count == 0) return;
        var counts = string.Join(", ", new[] { "pass", "known", "fail", "fixed" }
            .Select(s => $"{s} {rows.Count(r => r.Status == s)}"));
        var lines = new List<string>
        {
            $"BLite.Wasm browser tests: {BrowserName}, {Variant}: {rows.Count} rows: {counts}"
        };
        foreach (var r in rows.Where(r => r.Status != "pass").OrderBy(r => r.Status, StringComparer.Ordinal).ThenBy(r => r.Row, StringComparer.Ordinal))
            lines.Add(r.Status.ToUpperInvariant().PadRight(8) + r.Row
                + (r.Candidate != null ? $" [known candidate: {r.Candidate.Id}, {(string.IsNullOrEmpty(r.Candidate.Issue) ? "no issue" : r.Candidate.Issue)}]" : "")
                + (r.Message != null ? ": " + r.Message : ""));
        File.WriteAllLines(Path.ChangeExtension(ReportPath, ".summary.txt"), lines);
    }

    private async Task<string> PublishAsync()
    {
        var existing = Environment.GetEnvironmentVariable("BLITE_WASM_PUBLISH_DIR");
        if (!string.IsNullOrEmpty(existing))
            return existing;

        var project = typeof(BrowserFixture).Assembly.GetCustomAttributes<AssemblyMetadataAttribute>()
            .Single(a => a.Key == "HostProjectPath").Value!;
        var output = Path.Combine(AppContext.BaseDirectory, "wasm-host", Variant);
        // Each variant builds in its own artifacts folder: an incremental publish otherwise
        // reuses the trimmed assemblies of an earlier variant (obj/.../linked).
        var artifacts = Path.Combine(AppContext.BaseDirectory, "wasm-artifacts", Variant);
        var extra = Variant switch
        {
            "untrimmed" => "-p:PublishTrimmed=false",
            "fulltrim" => "-p:TrimMode=full",
            "aot" => "-p:RunAOTCompilation=true",
            "trimmed" => "",
            _ => throw new InvalidOperationException($"Unknown BLITE_WASM_VARIANT '{Variant}'.")
        };
        if (Directory.Exists(output))
            Directory.Delete(output, recursive: true);

        var psi = new ProcessStartInfo("dotnet",
            $"publish \"{project}\" -c Release --artifacts-path \"{artifacts}\" -o \"{output}\" -nologo {extra}")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };
        using var process = Process.Start(psi)!;
        var stdout = process.StandardOutput.ReadToEndAsync();
        var stderr = process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync();
        PublishLog = await stdout + await stderr;
        File.WriteAllText(Path.Combine(AppContext.BaseDirectory, $"publish-{Variant}.log"), PublishLog);
        if (process.ExitCode != 0)
            throw new InvalidOperationException($"Publishing the browser host failed:\n{PublishLog}");
        return output;
    }

    private static WebApplication StartServer(string wwwroot)
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        var app = builder.Build();

        var types = new FileExtensionContentTypeProvider();
        types.Mappings[".mjs"] = "text/javascript";
        types.Mappings[".wasm"] = "application/wasm";
        types.Mappings[".dat"] = "application/octet-stream";
        types.Mappings[".blat"] = "application/octet-stream";
        types.Mappings[".webcil"] = "application/octet-stream";

        var files = new PhysicalFileProvider(wwwroot);
        app.UseDefaultFiles(new DefaultFilesOptions { FileProvider = files });
        app.UseStaticFiles(new StaticFileOptions
        {
            FileProvider = files,
            ContentTypeProvider = types,
            ServeUnknownFileTypes = true,
            // Every test gets the build under test, never a cached one.
            OnPrepareResponse = ctx => ctx.Context.Response.Headers.CacheControl = "no-store"
        });
        return app;
    }
}

[CollectionDefinition(Name)]
public sealed class BrowserCollection : ICollectionFixture<BrowserFixture>
{
    public const string Name = "browser";
}
