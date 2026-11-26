using System.Text.Json;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests;

public sealed class PackageGraphTests
{
    private static readonly string[] AllowedPernixPackages =
    [
    ];

    [Fact]
    public void Only_allowed_pernix_packages_are_referenced()
    {
        var depsPath = Path.Combine(AppContext.BaseDirectory, $"{typeof(PackageGraphTests).Assembly.GetName().Name}.deps.json");
        using var deps = JsonDocument.Parse(File.ReadAllText(depsPath));

        // Library keys are "Name/Version". Project entries (this repo's own assemblies) are not packages.
        var pernixPackages = deps.RootElement.GetProperty("libraries").EnumerateObject()
            .Where(lib => lib.Value.GetProperty("type").GetString() == "package")
            .Select(lib => lib.Name.Split('/')[0])
            .Where(name => name.StartsWith("Pernix.", StringComparison.Ordinal))
            .Order(StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(AllowedPernixPackages.Order(StringComparer.Ordinal), pernixPackages);
    }

    [Fact]
    public void Http_destination_depends_on_core_only_at_lockstep_version()
    {
        var depsPath = Path.Combine(AppContext.BaseDirectory, $"{typeof(PackageGraphTests).Assembly.GetName().Name}.deps.json");
        using var deps = JsonDocument.Parse(File.ReadAllText(depsPath));

        var target = deps.RootElement.GetProperty("targets").EnumerateObject().Single().Value;
        var core = target.EnumerateObject().Single(lib => lib.Name.StartsWith("Pernix.OutboxRelay.Core/", StringComparison.Ordinal));
        var http = target.EnumerateObject().Single(lib => lib.Name.StartsWith("Pernix.OutboxRelay.Destination.Http/", StringComparison.Ordinal));
        var coreVersion = core.Name.Split('/')[1];

        Assert.Equal(coreVersion, http.Name.Split('/')[1]);

        // No Hosting, no Destination.Kafka, no third-party package: Core is the only dependency.
        var dependencies = http.Value.GetProperty("dependencies").EnumerateObject()
            .Select(dep => (dep.Name, Version: dep.Value.GetString()))
            .ToArray();

        Assert.Equal([("Pernix.OutboxRelay.Core", coreVersion)], dependencies);
    }
}
