using System.Text.RegularExpressions;
using Xunit;

namespace Pernix.OutboxRelay.Client.Docs.Unit.Tests;

// Every relative link in README.md and the docs/ reference pages resolves to an existing file or folder, and a '#anchor' on a Markdown target to one of its headings.
public sealed partial class ReadmeLinkTests
{
    private const string Fence = "```";

    [Theory]
    [InlineData(RepoFiles.Readme)]
    [InlineData("docs/configuration.md")]
    [InlineData("docs/delivery.md")]
    [InlineData("docs/development.md")]
    [InlineData("docs/README.md")]
    public void Every_relative_link_resolves(string document)
    {
        var failures = new List<string>();
        foreach (var link in ParseLinks(RepoFiles.ReadDocument(document)))
        {
            if (Check(RepoFiles.Root, document, link.Target) is { } failure)
            {
                failures.Add($"line {link.Line}: '{link.Target}' {failure}");
            }
        }

        Assert.True(failures.Count == 0, document + " links that do not resolve:\n" + string.Join("\n", failures));
    }

    [Theory]
    [InlineData("## Delivery (`Ordering.Mode` `None`)", "delivery-orderingmode-none")]
    [InlineData("### `outbox.pending` error bounds", "outboxpending-error-bounds")]
    [InlineData("## Broker-outage test (opt-in)", "broker-outage-test-opt-in")]
    [InlineData("## EF Core: explicit transaction", "ef-core-explicit-transaction")]
    [InlineData("## 3. Database prerequisites", "3-database-prerequisites")]
    public void Heading_slugs_follow_github(string heading, string slug)
    {
        Assert.Contains(slug, Anchors(heading));
    }

    [Fact]
    public void A_repeated_heading_gets_a_numbered_slug()
    {
        Assert.Equal(new[] { "usage", "usage-1" }, Anchors("## Usage\n## Usage"));
    }

    [Fact]
    public void A_missing_file_and_a_missing_anchor_are_reported()
    {
        Assert.NotNull(Check(RepoFiles.Root, RepoFiles.Readme, "docs/no-such-page.md"));
        Assert.NotNull(Check(RepoFiles.Root, RepoFiles.Readme, "docs/client-packages.md#no-such-heading"));
        Assert.Null(Check(RepoFiles.Root, RepoFiles.Readme, "docs/client-packages.md#ef-core-queue"));
    }

    private static string? Check(string root, string fromDocument, string target)
    {
        var hash = target.IndexOf('#', StringComparison.Ordinal);
        var pathPart = hash < 0 ? target : target[..hash];
        var anchor = hash < 0 ? null : target[(hash + 1)..];

        var fromDir = Path.GetDirectoryName(Path.Combine(root, fromDocument))!;
        var full = pathPart.Length == 0 ? Path.Combine(root, fromDocument) : Path.GetFullPath(Path.Combine(fromDir, Uri.UnescapeDataString(pathPart)));

        if (File.Exists(full))
        {
            if (anchor is null || !full.EndsWith(".md", StringComparison.OrdinalIgnoreCase))
            {
                return null;
            }

            return Anchors(File.ReadAllText(full)).Contains(anchor) ? null : "has no heading for the anchor";
        }

        if (Directory.Exists(full) && anchor is null)
        {
            return null;
        }

        return "does not exist";
    }

    private static List<Link> ParseLinks(string markdown)
    {
        var links = new List<Link>();
        var lines = markdown.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n');
        var inFence = false;
        for (var i = 0; i < lines.Length; i++)
        {
            if (lines[i].TrimStart().StartsWith(Fence, StringComparison.Ordinal))
            {
                inFence = !inFence;
                continue;
            }

            if (inFence)
            {
                continue;
            }

            foreach (Match match in LinkRegex().Matches(lines[i]))
            {
                var target = match.Groups["target"].Value;
                if (!target.Contains(':', StringComparison.Ordinal))
                {
                    links.Add(new Link(i + 1, target));
                }
            }
        }

        return links;
    }

    private static List<string> Anchors(string markdown)
    {
        var anchors = new List<string>();
        var seen = new Dictionary<string, int>(StringComparer.Ordinal);
        var inFence = false;
        foreach (var line in markdown.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n'))
        {
            if (line.TrimStart().StartsWith(Fence, StringComparison.Ordinal))
            {
                inFence = !inFence;
                continue;
            }

            var heading = inFence ? Match.Empty : HeadingRegex().Match(line);
            if (!heading.Success)
            {
                continue;
            }

            var text = LinkRegex().Replace(heading.Groups["text"].Value.Trim(), m => m.Groups["text"].Value);
            var slug = string.Concat(text.ToLowerInvariant().Where(c => char.IsLetterOrDigit(c) || c is ' ' or '-' or '_')).Replace(' ', '-');
            if (seen.TryGetValue(slug, out var count))
            {
                seen[slug] = count + 1;
                anchors.Add($"{slug}-{count + 1}");
            }
            else
            {
                seen[slug] = 0;
                anchors.Add(slug);
            }
        }

        return anchors;
    }

    [GeneratedRegex(@"\[(?<text>[^\]]*)\]\((?<target>[^)\s]+)\)")]
    private static partial Regex LinkRegex();

    [GeneratedRegex(@"^#{1,6} (?<text>.+)$")]
    private static partial Regex HeadingRegex();

    private sealed record Link(int Line, string Target);
}
