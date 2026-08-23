using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.IO.Pipes;
using ATLAS_AM.Shared;

var apiBaseUrl = "http://localhost:5000";

const string PipeName = "ATLASAgentPipe";
const string ServerHost = ".";

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

using var pipe = new NamedPipeClientStream(
    ServerHost,
    PipeName,
    PipeDirection.InOut,
    PipeOptions.Asynchronous
    );

await pipe.ConnectAsync(5002, cts.Token);

var duplex = new PipeDuplex();
duplex.MessageReceived += msg =>
{
    Console.WriteLine("Message received from server: " + msg);
};

using var client = new HttpClient
{
    BaseAddress = new Uri(apiBaseUrl)
};

await duplex.RunAsync(pipe, cts.Token);

static async Task<string> AskAsync(HttpClient client, string query)
{
    var payload = new { query };
    var json = JsonSerializer.Serialize(payload);
    var content = new StringContent(json, Encoding.UTF8, "application/json");

    var response = await client.PostAsync("/api/ask", content);
    response.EnsureSuccessStatusCode();

    var responseBody = await response.Content.ReadAsStringAsync();
    using var doc = JsonDocument.Parse(responseBody);

    return doc.RootElement.GetProperty("response").GetString() ?? "(no response)";
}