using System.Text;
using System.Text.RegularExpressions;
using Xunit;

namespace Pernix.OutboxRelay.Client.Docs.Unit.Tests;

// Every message quoted in the guide's error tables occurs in client/src, split at <placeholder> tokens.
public sealed partial class GuideErrorTextTests
{
    private const string OpenMarker = "<!-- docs-check: error-messages -->";
    private const string CloseMarker = "<!-- /docs-check: error-messages -->";
    private const string DotNetText = ".NET text";

    private static readonly Lazy<List<ErrorRow>> Rows = new(() => ParseRows(RepoFiles.ReadGuide()));

    [Fact]
    public void Every_quoted_message_piece_occurs_in_client_source()
    {
        var source = string.Join('\n', RepoFiles.EnumerateClientSourceFiles().Select(File.ReadAllText));
        var failures = new StringBuilder();

        foreach (var row in Rows.Value)
        {
            if (row.Message == DotNetText)
            {
                continue;
            }

            var quotes = QuoteRegex().Matches(row.Message);
            if (quotes.Count == 0)
            {
                failures.Append(row.Code).Append(": message cell is neither a backticked quote nor '").Append(DotNetText).Append("'.\n");
                continue;
            }

            foreach (Match quote in quotes)
            {
                foreach (var piece in PlaceholderRegex().Split(quote.Groups["text"].Value))
                {
                    if (piece.Length > 0 && !source.Contains(piece, StringComparison.Ordinal))
                    {
                        failures.Append(row.Code).Append(": '").Append(piece).Append("' not found in client/src/**/*.cs.\n");
                    }
                }
            }
        }

        Assert.True(failures.Length == 0, failures.ToString());
    }

    [Theory]
    [InlineData("E1")]
    [InlineData("E2")]
    [InlineData("E3")]
    [InlineData("E4")]
    [InlineData("E5")]
    [InlineData("E6")]
    [InlineData("E7")]
    [InlineData("E8")]
    [InlineData("CE1")]
    [InlineData("CE2")]
    [InlineData("CE3")]
    [InlineData("CE4")]
    [InlineData("CE5")]
    [InlineData("CE6")]
    [InlineData("CE7")]
    [InlineData("CE8")]
    [InlineData("CE9")]
    [InlineData("CE10")]
    [InlineData("CE11")]
    [InlineData("CE12")]
    [InlineData("CE13")]
    [InlineData("CE14")]
    [InlineData("Warning")]
    public void Error_table_has_one_row_per_code(string code)
    {
        Assert.Single(Rows.Value, r => string.Equals(r.Code, code, StringComparison.Ordinal));
    }

    private static List<ErrorRow> ParseRows(string markdown)
    {
        var rows = new List<ErrorRow>();
        var lines = markdown.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n');
        var inside = false;
        var sections = 0;

        foreach (var line in lines)
        {
            if (line.Trim() == OpenMarker)
            {
                Assert.False(inside, "Nested " + OpenMarker);
                inside = true;
                sections++;
                continue;
            }

            if (line.Trim() == CloseMarker)
            {
                Assert.True(inside, CloseMarker + " without " + OpenMarker);
                inside = false;
                continue;
            }

            if (!inside || !line.StartsWith('|'))
            {
                continue;
            }

            var cells = line.Trim().Trim('|').Split('|').Select(c => c.Trim()).ToArray();
            if (cells[0] == "code" || cells[0].StartsWith("---", StringComparison.Ordinal))
            {
                continue;
            }

            Assert.True(cells.Length == 5, $"Error table row needs 5 cells (code | exception | message | cause | fix): {line}");
            rows.Add(new ErrorRow(cells[0], cells[2]));
        }

        Assert.False(inside, OpenMarker + " without " + CloseMarker);
        Assert.True(sections > 0, "docs/client-packages.md has no " + OpenMarker + " section.");
        return rows;
    }

    [GeneratedRegex("`(?<text>[^`]+)`")]
    private static partial Regex QuoteRegex();

    [GeneratedRegex("<[^>]+>")]
    private static partial Regex PlaceholderRegex();

    private sealed record ErrorRow(string Code, string Message);
}
