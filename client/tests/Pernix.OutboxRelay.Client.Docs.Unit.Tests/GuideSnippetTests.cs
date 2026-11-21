using System.Text;
using System.Text.RegularExpressions;
using Xunit;

namespace Pernix.OutboxRelay.Client.Docs.Unit.Tests;

// Every csharp block of docs/client-packages.md and README.md is a copy of one #region docs:<id> in Snippets/*.cs.
public sealed partial class GuideSnippetTests
{
    private const string Fence = "```";

    private static readonly string[] CheckedDocuments = [RepoFiles.Guide, RepoFiles.Readme];

    private static readonly Lazy<Dictionary<string, Guide>> ParsedDocuments = new(
        () => CheckedDocuments.ToDictionary(d => d, d => ParseGuide(RepoFiles.ReadDocument(d)), StringComparer.Ordinal));

    private static readonly Lazy<Regions> ParsedRegions = new(ParseRegions);

    public static TheoryData<string> Documents => new(CheckedDocuments);

    [Theory]
    [MemberData(nameof(Documents))]
    public void Every_csharp_block_equals_its_region(string document)
    {
        var failures = new StringBuilder();
        foreach (var block in ParsedDocuments.Value[document].Blocks)
        {
            if (block.Id is null || !ParsedRegions.Value.ById.TryGetValue(block.Id, out var region))
            {
                continue;
            }

            if (!string.Equals(block.Text, region.Text, StringComparison.Ordinal))
            {
                failures.Append(document).Append(" line ").Append(block.Line).Append(": block 'snippet: ").Append(block.Id)
                    .Append("' differs from its region in ").Append(region.File).Append(". Expected block:\n")
                    .Append(Fence).Append("csharp\n").Append(region.Text).Append('\n').Append(Fence).Append("\n\n");
            }
        }

        Assert.True(failures.Length == 0, failures.ToString());
    }

    [Theory]
    [MemberData(nameof(Documents))]
    public void Every_csharp_fence_has_a_marker(string document)
    {
        var unmarked = ParsedDocuments.Value[document].Blocks.Where(b => b.Id is null).Select(b => $"line {b.Line}").ToList();

        Assert.True(unmarked.Count == 0, document + ": csharp fence without a '<!-- snippet: <id> -->' line directly before it: " + string.Join(", ", unmarked));
    }

    [Theory]
    [MemberData(nameof(Documents))]
    public void Every_marker_is_followed_by_a_csharp_fence(string document)
    {
        var dangling = ParsedDocuments.Value[document].DanglingMarkers;

        Assert.True(dangling.Count == 0, document + ": snippet marker not directly followed by a csharp fence: " + string.Join(", ", dangling));
    }

    [Theory]
    [MemberData(nameof(Documents))]
    public void Every_marker_has_a_region(string document)
    {
        var missing = ParsedDocuments.Value[document].Blocks
            .Where(b => b.Id is not null && !ParsedRegions.Value.ById.ContainsKey(b.Id))
            .Select(b => $"'{b.Id}' (line {b.Line})")
            .ToList();

        Assert.True(missing.Count == 0, document + ": snippet marker without a '#region docs:<id>' in Snippets/*.cs: " + string.Join(", ", missing));
    }

    [Theory]
    [MemberData(nameof(Documents))]
    public void Every_marker_id_is_used_once(string document)
    {
        var duplicates = ParsedDocuments.Value[document].Blocks
            .Where(b => b.Id is not null)
            .GroupBy(b => b.Id, StringComparer.Ordinal)
            .Where(g => g.Count() > 1)
            .Select(g => g.Key)
            .ToList();

        Assert.True(duplicates.Count == 0, document + ": snippet id used by more than one block: " + string.Join(", ", duplicates));
    }

    [Fact]
    public void Every_region_is_used_by_a_checked_document()
    {
        var used = ParsedDocuments.Value.Values
            .SelectMany(d => d.Blocks)
            .Where(b => b.Id is not null)
            .Select(b => b.Id!)
            .ToHashSet(StringComparer.Ordinal);
        var orphans = ParsedRegions.Value.ById.Values
            .Where(r => !used.Contains(r.Id))
            .Select(r => $"'{r.Id}' ({r.File})")
            .ToList();

        Assert.True(orphans.Count == 0, "Region not used by " + string.Join(" or ", CheckedDocuments) + " (orphan): " + string.Join(", ", orphans));
    }

    [Fact]
    public void Regions_are_well_formed()
    {
        Assert.True(ParsedRegions.Value.Errors.Count == 0, string.Join("\n", ParsedRegions.Value.Errors));
    }

    [Theory]
    [MemberData(nameof(Documents))]
    public void Csharp_fences_use_the_csharp_tag(string document)
    {
        var wrongTags = ParsedDocuments.Value[document].WrongTags;

        Assert.True(wrongTags.Count == 0, document + ": use ```csharp, not ```cs, ```c# or another case: " + string.Join(", ", wrongTags));
    }

    [Theory]
    [InlineData("CSharp")]
    [InlineData("CSHARP")]
    [InlineData("cs")]
    [InlineData("CS")]
    [InlineData("c#")]
    [InlineData("C#")]
    public void A_csharp_fence_with_another_tag_spelling_is_reported(string tag)
    {
        var guide = ParseGuide($"<!-- snippet: x -->\n{Fence}{tag}\nvar x = 1;\n{Fence}\n");

        Assert.Equal(["line 2"], guide.WrongTags);
    }

    private static Guide ParseGuide(string markdown)
    {
        var lines = SplitLines(markdown);
        var blocks = new List<GuideBlock>();
        var dangling = new List<string>();
        var wrongTags = new List<string>();

        for (var i = 0; i < lines.Length; i++)
        {
            var marker = MarkerRegex().Match(lines[i]);
            if (marker.Success && !(i + 1 < lines.Length && FenceTag(lines[i + 1]) == "csharp"))
            {
                dangling.Add($"'{marker.Groups["id"].Value}' (line {i + 1})");
            }

            var tag = FenceTag(lines[i]);
            if (tag is null)
            {
                continue;
            }

            // Any C# spelling other than exactly 'csharp' would skip the region check, so report it.
            var isCsharpSpelling = tag.Equals("csharp", StringComparison.OrdinalIgnoreCase)
                || tag.Equals("cs", StringComparison.OrdinalIgnoreCase)
                || tag.Equals("c#", StringComparison.OrdinalIgnoreCase);
            if (isCsharpSpelling && !string.Equals(tag, "csharp", StringComparison.Ordinal))
            {
                wrongTags.Add($"line {i + 1}");
            }

            var open = i;
            var body = new List<string>();
            for (i++; i < lines.Length && lines[i].Trim() != Fence; i++)
            {
                body.Add(lines[i]);
            }

            if (tag != "csharp")
            {
                continue;
            }

            var id = open > 0 && MarkerRegex().Match(lines[open - 1]) is { Success: true } m ? m.Groups["id"].Value : null;
            blocks.Add(new GuideBlock(id, open + 1, Normalise(body)));
        }

        return new Guide(blocks, dangling, wrongTags);
    }

    private static Regions ParseRegions()
    {
        var byId = new Dictionary<string, Region>(StringComparer.Ordinal);
        var errors = new List<string>();

        foreach (var path in RepoFiles.EnumerateSnippetFiles())
        {
            var file = Path.GetFileName(path);
            var lines = SplitLines(File.ReadAllText(path));
            string? id = null;
            var start = 0;
            var body = new List<string>();

            for (var i = 0; i < lines.Length; i++)
            {
                var trimmed = lines[i].Trim();
                if (trimmed.StartsWith("#region", StringComparison.Ordinal))
                {
                    if (id is not null)
                    {
                        errors.Add($"{file} line {i + 1}: region nested in 'docs:{id}'.");
                        continue;
                    }

                    var match = RegionRegex().Match(trimmed);
                    if (!match.Success)
                    {
                        errors.Add($"{file} line {i + 1}: region is not '#region docs:<id>' with <id> = [a-z0-9-]+.");
                        continue;
                    }

                    id = match.Groups["id"].Value;
                    start = i + 1;
                    body.Clear();
                }
                else if (trimmed.StartsWith("#endregion", StringComparison.Ordinal) && id is not null)
                {
                    if (!byId.TryAdd(id, new Region(id, file, Normalise(body))))
                    {
                        errors.Add($"{file} line {start}: duplicate region id '{id}' (also in {byId[id].File}).");
                    }

                    id = null;
                }
                else if (id is not null)
                {
                    body.Add(lines[i]);
                }
            }

            if (id is not null)
            {
                errors.Add($"{file} line {start}: region 'docs:{id}' has no #endregion.");
            }
        }

        return new Regions(byId, errors);
    }

    // Rule 3: trailing whitespace per line removed, leading/trailing blank lines removed, common indentation removed.
    private static string Normalise(List<string> lines)
    {
        var trimmed = lines.Select(l => l.TrimEnd()).ToList();
        while (trimmed.Count > 0 && trimmed[0].Length == 0)
        {
            trimmed.RemoveAt(0);
        }

        while (trimmed.Count > 0 && trimmed[^1].Length == 0)
        {
            trimmed.RemoveAt(trimmed.Count - 1);
        }

        var indent = trimmed.Where(l => l.Length > 0).Select(l => l.Length - l.TrimStart().Length).DefaultIfEmpty(0).Min();
        return string.Join('\n', trimmed.Select(l => l.Length == 0 ? l : l[indent..]));
    }

    private static string[] SplitLines(string text) => text.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n');

    // Null when the line opens no fence. Otherwise the info string's first word (empty for a bare fence).
    private static string? FenceTag(string line)
    {
        var trimmed = line.TrimStart();
        if (!trimmed.StartsWith(Fence, StringComparison.Ordinal))
        {
            return null;
        }

        var info = trimmed[Fence.Length..].Trim();
        var space = info.IndexOf(' ', StringComparison.Ordinal);
        return space < 0 ? info : info[..space];
    }

    [GeneratedRegex(@"^<!-- snippet: (?<id>[a-z0-9-]+) -->$")]
    private static partial Regex MarkerRegex();

    [GeneratedRegex(@"^#region docs:(?<id>[a-z0-9-]+)$")]
    private static partial Regex RegionRegex();

    private sealed record GuideBlock(string? Id, int Line, string Text);

    private sealed record Guide(List<GuideBlock> Blocks, List<string> DanglingMarkers, List<string> WrongTags);

    private sealed record Region(string Id, string File, string Text);

    private sealed record Regions(Dictionary<string, Region> ById, List<string> Errors);
}
