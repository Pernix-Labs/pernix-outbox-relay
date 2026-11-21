using Xunit;

namespace Pernix.OutboxRelay.Client.Docs.Unit.Tests;

internal static class RepoFiles
{
    private static readonly Lazy<string> RootPath = new(FindRepoRoot);

    public static string Root => RootPath.Value;

    public const string Guide = "docs/client-packages.md";

    public const string Readme = "README.md";

    public static string ReadGuide() => ReadDocument(Guide);

    public static string ReadDocument(string relativePath) => File.ReadAllText(Path.Combine(Root, relativePath));

    public static string[] EnumerateSnippetFiles()
    {
        var dir = Path.Combine(Root, "client", "tests", "Pernix.OutboxRelay.Client.Docs.Unit.Tests", "Snippets");
        var files = Directory.GetFiles(dir, "*.cs", SearchOption.TopDirectoryOnly);
        Array.Sort(files, StringComparer.Ordinal);
        return files;
    }

    public static string[] EnumerateClientSourceFiles()
    {
        var srcDir = Path.Combine(Root, "client", "src");
        var files = new List<string>();
        foreach (var file in Directory.EnumerateFiles(srcDir, "*.cs", SearchOption.AllDirectories))
        {
            var segments = Path.GetRelativePath(srcDir, file).Split(Path.DirectorySeparatorChar);
            if (Array.IndexOf(segments, "obj") < 0 && Array.IndexOf(segments, "bin") < 0)
            {
                files.Add(file);
            }
        }

        files.Sort(StringComparer.Ordinal);
        return [.. files];
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
