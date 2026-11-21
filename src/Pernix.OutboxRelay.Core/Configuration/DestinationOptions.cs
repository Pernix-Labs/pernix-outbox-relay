using Microsoft.Extensions.Configuration;

namespace Pernix.OutboxRelay.Core.Configuration;

public sealed class DestinationOptions
{
    public static IReadOnlyList<string> CoreKeys { get; } = [nameof(Name), nameof(Transport)];

    public string? Name { get; set; }
    public string? Transport { get; set; }

    public IConfigurationSection? Settings { get; internal set; }
}

public sealed class DatabaseOptions
{
    public string? Provider { get; set; }
    public string? Connection { get; set; }
    public string? Table { get; set; }
}
