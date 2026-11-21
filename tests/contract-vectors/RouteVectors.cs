using System.Text.Json;

namespace Pernix.OutboxRelay.ContractVectors;

internal sealed class RouteVector
{
    public required string Id { get; init; }

    public required string Case { get; init; }

    public required string Route { get; init; }

    public required bool Accepts { get; init; }

    public override string ToString() => Id;
}

// Linked into each consuming test project. No copies of this file or of routes.json.
internal static class RouteVectors
{
    public const int SupportedVersion = 1;

    public static readonly string FilePath = Path.Combine(AppContext.BaseDirectory, "contract-vectors", "routes.json");

    private static readonly Lazy<IReadOnlyList<RouteVector>> Loaded = new(Load);

    // Throws when the file is missing or malformed, so dependent tests fail, never skip.
    public static IReadOnlyList<RouteVector> All => Loaded.Value;

    public static RouteVector Get(string id)
    {
        foreach (var vector in All)
        {
            if (vector.Id == id)
            {
                return vector;
            }
        }

        throw new KeyNotFoundException($"Route vector {id} is not in {FilePath}.");
    }

    public static IReadOnlyList<RouteVector> Load() => Parse(File.ReadAllText(FilePath));

    public static IReadOnlyList<RouteVector> Parse(string json)
    {
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        var version = root.GetProperty("version").GetInt32();
        if (version != SupportedVersion)
        {
            throw new InvalidDataException($"Route vector file version {version} is not {SupportedVersion}.");
        }

        var vectors = new List<RouteVector>();
        foreach (var element in root.GetProperty("vectors").EnumerateArray())
        {
            vectors.Add(new RouteVector
            {
                Id = element.GetProperty("id").GetString() ?? throw Invalid("id is null"),
                Case = element.GetProperty("case").GetString() ?? throw Invalid("case is null"),
                Route = element.GetProperty("route").GetString() ?? throw Invalid("route is null"),
                Accepts = element.GetProperty("accepts").GetBoolean(),
            });
        }

        return vectors;
    }

    private static InvalidDataException Invalid(string reason) => new($"Route vector file is malformed: {reason}.");
}
