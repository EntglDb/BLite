using BLite.BlazorWasm;
using BLite.BlazorWasm.Services;
using BLite.Wasm;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.AspNetCore.Components.WebAssembly.Hosting;

var builder = WebAssemblyHostBuilder.CreateDefault(args);
builder.RootComponents.Add<App>("#app");
builder.RootComponents.Add<HeadOutlet>("head::after");

// Register the BLite counter service as a singleton.
// The service initialises BLite lazily on first use, auto-selecting
// the best available storage backend (OPFS → IndexedDB → in-memory).
builder.Services.AddSingleton<CounterStore>();

await builder.Build().RunAsync();
