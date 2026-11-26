using Pernix.OutboxRelay.Destination.Http;
using Pernix.OutboxRelay.Destination.Http.Options;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Http;

public sealed class StatusTableTests
{
    private const string Key = "Destinations[Sink].Http.StatusCodes";

    [Theory]
    [InlineData(200, "Delivered")]
    [InlineData(204, "Delivered")]
    [InlineData(299, "Delivered")]
    [InlineData(400, "Rejected")]
    [InlineData(413, "Rejected")]
    [InlineData(414, "Rejected")]
    [InlineData(415, "Rejected")]
    [InlineData(422, "Rejected")]
    [InlineData(100, "Paused")]
    [InlineData(301, "Paused")]
    [InlineData(401, "Paused")]
    [InlineData(404, "Paused")]
    [InlineData(409, "Paused")]
    [InlineData(429, "Paused")]
    [InlineData(500, "Paused")]
    [InlineData(503, "Paused")]
    public void Defaults(int code, string className)
    {
        var expected = Enum.Parse<StatusClass>(className);
        Assert.Equal(expected, StatusTable.Default.Classify(code));
        Assert.Equal(expected, Create(new StatusCodesOptions()).Classify(code));
    }

    [Fact]
    public void Default_retried_set_is_empty()
    {
        Assert.DoesNotContain(Enumerable.Range(0, 1000), code => StatusTable.Default.Classify(code) == StatusClass.Retried);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(0)]
    [InlineData(99)]
    [InlineData(600)]
    [InlineData(999)]
    public void Codes_outside_100_599_are_paused(int code)
    {
        Assert.Equal(StatusClass.Paused, Create(new StatusCodesOptions { Delivered = ["1xx", "2xx", "5xx"] }).Classify(code));
    }

    [Fact]
    public void Listed_delivered_replaces_its_default_set()
    {
        var table = Create(new StatusCodesOptions { Delivered = ["204"] });

        Assert.Equal(StatusClass.Delivered, table.Classify(204));
        Assert.Equal(StatusClass.Paused, table.Classify(200));
        Assert.Equal(StatusClass.Rejected, table.Classify(422));
    }

    [Fact]
    public void Listed_rejected_replaces_its_default_set()
    {
        var table = Create(new StatusCodesOptions { Rejected = ["400"] });

        Assert.Equal(StatusClass.Rejected, table.Classify(400));
        Assert.Equal(StatusClass.Paused, table.Classify(422));
        Assert.Equal(StatusClass.Delivered, table.Classify(200));
    }

    [Fact]
    public void Server_error_can_move_to_retried()
    {
        var table = Create(new StatusCodesOptions { Retried = ["500"] });

        Assert.Equal(StatusClass.Retried, table.Classify(500));
        Assert.Equal(StatusClass.Paused, table.Classify(503));
    }

    [Fact]
    public void Delivered_may_take_a_default_rejected_code()
    {
        var table = Create(new StatusCodesOptions { Delivered = ["2xx", "422"] });

        Assert.Equal(StatusClass.Delivered, table.Classify(422));
        Assert.Equal(StatusClass.Rejected, table.Classify(400));
    }

    [Fact]
    public void Class_and_range_entries()
    {
        var table = Create(new StatusCodesOptions { Retried = ["500-504", "4XX"], Rejected = ["300-301"] });

        Assert.Equal(StatusClass.Retried, table.Classify(500));
        Assert.Equal(StatusClass.Retried, table.Classify(504));
        Assert.Equal(StatusClass.Paused, table.Classify(505));
        Assert.Equal(StatusClass.Retried, table.Classify(400));
        Assert.Equal(StatusClass.Retried, table.Classify(499));
        Assert.Equal(StatusClass.Rejected, table.Classify(301));
    }

    [Fact]
    public void Listed_paused_takes_codes_from_a_default_set()
    {
        var table = Create(new StatusCodesOptions { Paused = ["204"] });

        Assert.Equal(StatusClass.Paused, table.Classify(204));
        Assert.Equal(StatusClass.Delivered, table.Classify(200));
    }

    [Theory]
    [InlineData("")]
    [InlineData("abc")]
    [InlineData("99")]
    [InlineData("099")]
    [InlineData("600")]
    [InlineData("6xx")]
    [InlineData("0xx")]
    [InlineData("2x")]
    [InlineData("2xxx")]
    [InlineData("500-")]
    [InlineData("-500")]
    [InlineData("504-500")]
    [InlineData("500-600")]
    [InlineData("5xx-599")]
    public void Entry_that_does_not_parse_fails_without_its_value(string entry)
    {
        var failures = new List<string>();

        Assert.Null(StatusTable.TryCreate(new StatusCodesOptions { Retried = ["500", entry] }, Key, failures));

        var failure = Assert.Single(failures);
        Assert.Equal($"{Key}.Retried[1]: must be a code (NNN), a class (Nxx) or a range (NNN-NNN) within 100-599", failure);
    }

    [Theory]
    [InlineData("2xx", "204", "Rejected")]
    [InlineData("200-204", "204-299", "Rejected")]
    [InlineData("409", "400-499", "Rejected")]
    public void Code_in_two_classes_fails(string delivered, string rejected, string second)
    {
        var failures = new List<string>();

        Assert.Null(StatusTable.TryCreate(new StatusCodesOptions { Delivered = [delivered], Rejected = [rejected] }, Key, failures));

        Assert.Equal($"{Key}.{second}: shares a status code with Delivered; a code may be in one class only", Assert.Single(failures));
    }

    [Fact]
    public void Overlap_is_reported_for_each_pair()
    {
        var failures = new List<string>();

        StatusTable.TryCreate(new StatusCodesOptions { Delivered = ["200"], Retried = ["200"], Paused = ["200"] }, Key, failures);

        Assert.Equal(
            [
                $"{Key}.Paused: shares a status code with Delivered; a code may be in one class only",
                $"{Key}.Paused: shares a status code with Retried; a code may be in one class only",
                $"{Key}.Retried: shares a status code with Delivered; a code may be in one class only",
            ],
            failures.Order(StringComparer.Ordinal));
    }

    [Fact]
    public void Repeated_code_in_one_class_is_allowed()
    {
        var table = Create(new StatusCodesOptions { Retried = ["500", "5xx", "500-503"] });

        Assert.Equal(StatusClass.Retried, table.Classify(500));
    }

    private static StatusTable Create(StatusCodesOptions options)
    {
        var failures = new List<string>();
        var table = StatusTable.TryCreate(options, Key, failures);
        Assert.Empty(failures);
        return table!;
    }
}
