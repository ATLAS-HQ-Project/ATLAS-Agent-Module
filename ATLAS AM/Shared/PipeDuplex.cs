using System.IO.Pipes;
using System.Threading.Channels;

namespace ATLAS_AM.Shared;

/// <summary>
/// Runs independent read and write loops over a connected PipeStream so both
/// ends can send and receive at any time, not just in request/response order.
/// </summary>
public class PipeDuplex
{
    // Messages queued here get written out to the pipe.
    public Channel<string> Outgoing { get; } = Channel.CreateUnbounded<string>();

    // Fired whenever a full line is received from the other end.
    public event Action<string>? MessageReceived;

    public async Task RunAsync(PipeStream pipe, CancellationToken ct)
    {
        var reader = new StreamReader(pipe);
        var writer = new StreamWriter(pipe) { AutoFlush = true };

        var readTask = ReadLoopAsync(reader, ct);
        var writeTask = WriteLoopAsync(writer, ct);

        // Exits as soon as either side breaks (disconnect, cancellation, error).
        await Task.WhenAny(readTask, writeTask);
    }

    private async Task ReadLoopAsync(StreamReader reader, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            string? line;
            try
            {
                line = await reader.ReadLineAsync(ct);
            }
            catch (IOException)
            {
                break; // pipe broke
            }

            if (line is null)
                break; // other end disconnected

            MessageReceived?.Invoke(line);
        }
    }

    private async Task WriteLoopAsync(StreamWriter writer, CancellationToken ct)
    {
        try
        {
            await foreach (var msg in Outgoing.Reader.ReadAllAsync(ct))
            {
                await writer.WriteLineAsync(msg);
            }
        }
        catch (IOException)
        {
            // pipe broke while writing
        }
    }

    public void Send(string message) => Outgoing.Writer.TryWrite(message);
}
