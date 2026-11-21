using System.Buffers;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;

namespace Pernix.OutboxRelay.Client;

// Writes validated header entries as one JSON object. Per-thread scratch, used only from the synchronous Build().
internal static class HeadersJsonWriter
{
    public const string EmptyObject = "{}";

    internal const int RetainedCapacity = 4096;

    private static readonly JsonWriterOptions Options = new() { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping };

    [ThreadStatic]
    private static ArrayBufferWriter<byte>? buffer;

    [ThreadStatic]
    private static Utf8JsonWriter? writer;

    internal static int ScratchCapacity => buffer?.Capacity ?? 0;

    public static string Write(KeyValuePair<string, string>[] entries)
    {
        if (entries.Length == 0)
        {
            return EmptyObject;
        }

        var output = buffer ??= new ArrayBufferWriter<byte>(256);
        output.ResetWrittenCount();

        var json = writer;
        if (json is null)
        {
            json = writer = new Utf8JsonWriter(output, Options);
        }
        else
        {
            json.Reset(output);
        }

        try
        {
            json.WriteStartObject();
            for (var i = 0; i < entries.Length; i++)
            {
                json.WriteString(entries[i].Key, entries[i].Value);
            }

            json.WriteEndObject();
            json.Flush();
            return Encoding.UTF8.GetString(output.WrittenSpan);
        }
        finally
        {
            if (output.Capacity > RetainedCapacity)
            {
                json.Dispose();
                buffer = null;
                writer = null;
            }
            else
            {
                json.Reset(output);
                output.ResetWrittenCount();
            }
        }
    }
}
