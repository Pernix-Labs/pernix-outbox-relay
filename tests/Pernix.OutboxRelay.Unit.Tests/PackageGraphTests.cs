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
}
