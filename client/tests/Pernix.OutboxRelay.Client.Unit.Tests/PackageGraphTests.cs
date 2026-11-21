using System.Text.Json;
using System.Xml.Linq;
using Xunit;

namespace Pernix.OutboxRelay.Client.Unit.Tests;

// Reads the declared (packed) dependencies from each packable project's assets "project" section, not this test's resolved graph.
public sealed class PackageGraphTests
{
    private const string TargetFramework = "net8.0";
    private const string CorePackage = "Pernix.OutboxRelay.Client";
    private const string ProviderPackage = "Pernix.OutboxRelay.Client.PostgreSql";
    private const string EntityFrameworkCorePackage = "Pernix.OutboxRelay.Client.EntityFrameworkCore";

    private static readonly SortedDictionary<string, string> ProviderPackageFloors = new(StringComparer.Ordinal)
    {
        ["Microsoft.Extensions.DependencyInjection.Abstractions"] = "[8.0.0, )",
        ["Npgsql"] = "[8.0.3, )",
    };

    private static readonly SortedDictionary<string, string> EntityFrameworkCorePackageFloors = new(StringComparer.Ordinal)
    {
        ["Microsoft.EntityFrameworkCore.Relational"] = "[8.0.10, )",
    };

    public static TheoryData<string> PackableProjects => new() { CorePackage, ProviderPackage, EntityFrameworkCorePackage };

    [Fact]
    public void Core_declares_no_package_dependency()
    {
        Assert.Empty(DeclaredPackages(CorePackage));
        Assert.Empty(DeclaredProjectReferences(CorePackage));
    }

    [Fact]
    public void Provider_declares_exactly_npgsql_and_di_abstractions_at_their_floors()
    {
        Assert.Equal(ProviderPackageFloors, DeclaredPackages(ProviderPackage));
    }

    [Fact]
    public void Provider_references_only_core_project()
    {
        Assert.Equal([CorePackage], DeclaredProjectReferences(ProviderPackage));
    }

    [Fact]
    public void EntityFrameworkCore_declares_exactly_ef_relational_at_its_floor()
    {
        Assert.Equal(EntityFrameworkCorePackageFloors, DeclaredPackages(EntityFrameworkCorePackage));
    }

    [Fact]
    public void EntityFrameworkCore_references_only_core_project()
    {
        Assert.Equal([CorePackage], DeclaredProjectReferences(EntityFrameworkCorePackage));
    }

    [Theory]
    [MemberData(nameof(PackableProjects))]
    public void No_client_package_declares_a_pernix_package(string project)
    {
        Assert.DoesNotContain(DeclaredPackages(project).Keys, name => name.StartsWith("Pernix.", StringComparison.OrdinalIgnoreCase));
    }

    // CPM transitive pinning lands here and dotnet pack writes these as nuspec dependencies.
    [Theory]
    [MemberData(nameof(PackableProjects))]
    public void No_client_package_pins_a_transitive_package(string project)
    {
        Assert.Empty(PinnedTransitivePackages(project));
    }

    [Fact]
    public void No_relay_project_references_the_client()
    {
        var repoRoot = FindRepoRoot();
        var clientDir = Path.Combine(repoRoot, "client") + Path.DirectorySeparatorChar;
        var relayProjects = Directory.GetFiles(Path.Combine(repoRoot, "src"), "*.csproj", SearchOption.AllDirectories);
        Assert.NotEmpty(relayProjects);

        var offenders = new List<string>();
        foreach (var csproj in relayProjects)
        {
            var projectDir = Path.GetDirectoryName(csproj)!;
            foreach (var element in XDocument.Load(csproj).Descendants())
            {
                foreach (var attributeName in (string[])["Include", "Project"])
                {
                    var value = element.Attribute(attributeName)?.Value;
                    if (string.IsNullOrWhiteSpace(value) || value.Contains("$(", StringComparison.Ordinal))
                    {
                        continue;
                    }

                    var full = Path.GetFullPath(Path.Combine(projectDir, value.Replace('\\', Path.DirectorySeparatorChar)));
                    if (full.StartsWith(clientDir, StringComparison.Ordinal))
                    {
                        offenders.Add($"{Path.GetRelativePath(repoRoot, csproj)}: {element.Name.LocalName} {value}");
                    }
                }
            }
        }

        Assert.Empty(offenders);
    }

    private static SortedDictionary<string, string> DeclaredPackages(string project)
    {
        var packages = new SortedDictionary<string, string>(StringComparer.Ordinal);
        if (!ProjectFramework(project).TryGetProperty("dependencies", out var dependencies))
        {
            return packages;
        }

        foreach (var dependency in dependencies.EnumerateObject())
        {
            // PrivateAssets=all references never reach the nuspec.
            if (dependency.Value.TryGetProperty("suppressParent", out var suppress)
                && string.Equals(suppress.GetString(), "All", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            packages.Add(dependency.Name, dependency.Value.GetProperty("version").GetString()!);
        }

        return packages;
    }

    private static string[] PinnedTransitivePackages(string project)
    {
        using var assets = LoadAssets(project);
        var names = new List<string>();
        if (assets.RootElement.TryGetProperty("centralTransitiveDependencyGroups", out var groups)
            && groups.TryGetProperty(TargetFramework, out var group))
        {
            foreach (var dependency in group.EnumerateObject())
            {
                names.Add($"{dependency.Name} {dependency.Value.GetProperty("version").GetString()}");
            }
        }

        names.Sort(StringComparer.Ordinal);
        return [.. names];
    }

    private static string[] DeclaredProjectReferences(string project)
    {
        using var assets = LoadAssets(project);
        var references = assets.RootElement.GetProperty("project").GetProperty("restore")
            .GetProperty("frameworks").GetProperty(TargetFramework).GetProperty("projectReferences");

        var names = new List<string>();
        foreach (var reference in references.EnumerateObject())
        {
            names.Add(Path.GetFileNameWithoutExtension(reference.Name.Replace('\\', '/').Split('/')[^1]));
        }

        names.Sort(StringComparer.Ordinal);
        return [.. names];
    }

    private static JsonElement ProjectFramework(string project)
    {
        using var assets = LoadAssets(project);
        var frameworks = assets.RootElement.GetProperty("project").GetProperty("frameworks");
        return frameworks.GetProperty(TargetFramework).Clone();
    }

    private static JsonDocument LoadAssets(string project)
    {
        var path = Path.Combine(AppContext.BaseDirectory, $"{project}.assets.json");
        Assert.True(File.Exists(path), $"Package graph guard input missing: {path}. client/Directory.Build.targets must copy it.");
        return JsonDocument.Parse(File.ReadAllText(path));
    }

    private static string FindRepoRoot()
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
        {
            if (File.Exists(Path.Combine(dir.FullName, "OutboxRelay.slnx")))
            {
                return dir.FullName;
            }
        }

        Assert.Fail("Repo root with OutboxRelay.slnx not found above " + AppContext.BaseDirectory);
        return null!;
    }
}
