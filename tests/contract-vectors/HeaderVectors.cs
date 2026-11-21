using System.Globalization;
using System.Text.Json;

namespace Pernix.OutboxRelay.ContractVectors;

// Null members mean "not applicable" for that side.
internal sealed class HeaderVector
{
    public required string Id { get; init; }

    public required string Case { get; init; }

    public IReadOnlyList<KeyValuePair<string, string>>? Client { get; init; }

    public string? StoredJson { get; init; }

    public bool? ClientAccepts { get; init; }

    public bool? RelayAccepts { get; init; }

    public IReadOnlyList<KeyValuePair<string, string>>? KafkaHeaders { get; init; }

    public override string ToString() => Id;
}

// Linked into each consuming test project. No copies of this file or of headers.json.
internal static class HeaderVectors
{
    public const int SupportedVersion = 1;

    public static readonly string FilePath = Path.Combine(AppContext.BaseDirectory, "contract-vectors", "headers.json");

    private static readonly Lazy<IReadOnlyList<HeaderVector>> Loaded = new(() => Parse(File.ReadAllText(FilePath)));

    // Throws when the file is missing or malformed, so dependent tests fail, never skip.
    public static IReadOnlyList<HeaderVector> All => Loaded.Value;

    public static HeaderVector Get(string id)
    {
        foreach (var vector in All)
        {
            if (vector.Id == id)
            {
                return vector;
            }
        }

        throw new KeyNotFoundException($"Header vector {id} is not in {FilePath}.");
    }

    public static IReadOnlyList<HeaderVector> Parse(string json)
    {
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        var version = root.GetProperty("version").GetInt32();
        if (version != SupportedVersion)
        {
            throw new InvalidDataException($"Header vector file version {version} is not {SupportedVersion}.");
        }

        var vectors = new List<HeaderVector>();
        foreach (var element in root.GetProperty("vectors").EnumerateArray())
        {
            vectors.Add(new HeaderVector
            {
                Id = element.GetProperty("id").GetString() ?? throw Invalid("id is null"),
                Case = element.GetProperty("case").GetString() ?? throw Invalid("case is null"),
                Client = HeaderList(element.GetProperty("client")),
                StoredJson = element.GetProperty("storedJson").GetString(),
                ClientAccepts = NullableBool(element.GetProperty("clientAccepts")),
                RelayAccepts = NullableBool(element.GetProperty("relayAccepts")),
                KafkaHeaders = HeaderList(element.GetProperty("kafkaHeaders")),
            });
        }

        return vectors;
    }

    // Text field union: JSON string | {"utf16": "D800 0041"} | {"repeat": "x", "count": 65536}.
    public static string Text(JsonElement element)
    {
        if (element.ValueKind == JsonValueKind.String)
        {
            return element.GetString()!;
        }

        if (element.ValueKind != JsonValueKind.Object)
        {
            throw Invalid($"text field is {element.ValueKind}");
        }

        if (element.TryGetProperty("utf16", out var units))
        {
            var hex = units.GetString() ?? throw Invalid("utf16 is null");
            var parts = hex.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            var chars = new char[parts.Length];
            for (var i = 0; i < parts.Length; i++)
            {
                chars[i] = (char)ushort.Parse(parts[i], NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture);
            }

            return new string(chars);
        }

        if (element.TryGetProperty("repeat", out var repeat))
        {
            var unit = repeat.GetString() ?? throw Invalid("repeat is null");
            var count = element.GetProperty("count").GetInt32();
            return string.Concat(Enumerable.Repeat(unit, count));
        }

        throw Invalid("text field object has neither utf16 nor repeat");
    }

    private static List<KeyValuePair<string, string>>? HeaderList(JsonElement element)
    {
        if (element.ValueKind == JsonValueKind.Null)
        {
            return null;
        }

        var headers = new List<KeyValuePair<string, string>>();
        foreach (var header in element.EnumerateArray())
        {
            headers.Add(new(Text(header.GetProperty("name")), Text(header.GetProperty("value"))));
        }

        return headers;
    }

    private static bool? NullableBool(JsonElement element) =>
        element.ValueKind == JsonValueKind.Null ? null : element.GetBoolean();

    private static InvalidDataException Invalid(string reason) => new($"Header vector file is malformed: {reason}.");
}
