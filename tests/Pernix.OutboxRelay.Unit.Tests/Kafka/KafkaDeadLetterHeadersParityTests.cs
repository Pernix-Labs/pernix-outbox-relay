using System.Globalization;
using System.Text;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Kafka;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Kafka;

// Expected output is KafkaDeadLetterHeadersLegacy (2bb3ac1), never the branch.
public sealed class KafkaDeadLetterHeadersParityTests
{
    private static readonly byte[] Key = [1];
    private static readonly byte[] Payload = [2];

    private static readonly OutgoingMessage Base = new()
    {
        Source = new MessageSource("Orders", 42, "OrdersDestination"),
        Key = Key,
        Payload = Payload,
        Headers = MessageHeaders.Empty,
        DeadLetter = new DeadLetterInfo("InvalidRecord: bad", 3, new DateTimeOffset(2026, 11, 3, 4, 5, 6, 789, TimeSpan.FromHours(2))),
    };

    private static readonly Dictionary<string, OutgoingMessage> Cases = new(StringComparer.Ordinal)
    {
        ["attempts 0"] = WithAttempts(0),
        ["attempts 1"] = WithAttempts(1),
        ["attempts -1"] = WithAttempts(-1),
        ["attempts int.MinValue"] = WithAttempts(int.MinValue),
        ["attempts int.MaxValue"] = WithAttempts(int.MaxValue),
        ["id 0"] = WithId(0),
        ["id 1"] = WithId(1),
        ["id long.MinValue"] = WithId(long.MinValue),
        ["id long.MaxValue"] = WithId(long.MaxValue),
        ["failed-at +05:30 ms 0"] = WithFailedAt(new DateTimeOffset(2026, 1, 1, 0, 0, 0, 0, new TimeSpan(5, 30, 0))),
        ["failed-at +05:30 ms 999"] = WithFailedAt(new DateTimeOffset(2026, 12, 31, 23, 59, 59, 999, new TimeSpan(5, 30, 0))),
        ["failed-at -05:00 ms 0"] = WithFailedAt(new DateTimeOffset(2026, 11, 3, 23, 59, 58, 0, TimeSpan.FromHours(-5))),
        ["failed-at -05:00 ms 999"] = WithFailedAt(new DateTimeOffset(2026, 12, 31, 20, 0, 0, 999, TimeSpan.FromHours(-5))),
        ["failed-at MinValue"] = WithFailedAt(DateTimeOffset.MinValue),
        ["failed-at MaxValue"] = WithFailedAt(DateTimeOffset.MaxValue),
        ["reason empty"] = WithReason(""),
        ["reason ascii"] = WithReason("MsgSizeTooLarge: too big"),
        ["reason non-ascii"] = WithReason("Ungültig: ß € 漢字 🚀"),
        ["route null"] = Base with { Route = null },
        ["route empty"] = Base with { Route = "" },
        ["route non-ascii"] = Base with { Route = "orders.eu-ü-漢" },
        ["outbox ascii destination ascii"] = WithNames("Payments", "PaymentsTopic"),
        ["outbox non-ascii destination ascii"] = WithNames("Bestellungen-ü", "PaymentsTopic"),
        ["outbox ascii destination non-ascii"] = WithNames("Payments", "Ziel-漢字"),
        ["outbox non-ascii destination non-ascii"] = WithNames("Bestellungen-ü", "Ziel-漢字"),
        ["headers none"] = Base with { Headers = MessageHeaders.Empty },
        ["headers all names mixed case"] = WithHeaders(
            "type", "X-Dead-Letter-Reason", "X-RETRY-ATTEMPTS", "x-Original-Destination", "X-original-OUTBOX", "X-Original-Id", "X-Failed-At", "X-ORIGINAL-ROUTE"),
        ["headers interleaved"] = WithHeaders("a", "x-original-id", "b-ü", "X-FAILED-AT", "c", "x-original-route", "traceparent") with { Route = "alerts" },
    };

    private static readonly string[] Cultures = ["", "th-TH", "ar-SA"];

    public static TheoryData<string, string> CaseIds()
    {
        var data = new TheoryData<string, string>();
        foreach (var id in Cases.Keys)
        {
            foreach (var culture in Cultures)
            {
                data.Add(id, culture);
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(CaseIds))]
    public void Merge_matches_legacy_bytes_names_and_order(string caseId, string culture)
    {
        var message = Cases[caseId];
        MessageHeaders expected, actual;
        var (savedCulture, savedUiCulture) = (CultureInfo.CurrentCulture, CultureInfo.CurrentUICulture);
        try
        {
            if (culture.Length > 0)
            {
                CultureInfo.CurrentCulture = CultureInfo.CurrentUICulture = new CultureInfo(culture);
            }

            expected = KafkaDeadLetterHeadersLegacy.Merge(message);
            actual = KafkaDeadLetterHeaders.Merge(message);
        }
        finally
        {
            CultureInfo.CurrentCulture = savedCulture;
            CultureInfo.CurrentUICulture = savedUiCulture;
        }

        Assert.Equal(expected.Count, actual.Count);
        for (var i = 0; i < expected.Count; i++)
        {
            Assert.Equal(expected[i].Name, actual[i].Name, StringComparer.Ordinal);
            Assert.True(expected[i].Value.AsSpan().SequenceEqual(actual[i].Value), $"value of {expected[i].Name} at {i} differs");
        }

        Assert.Equal(KafkaRecordSize.UpperBound(Key, Payload, expected), KafkaRecordSize.UpperBound(Key, Payload, actual));
    }

    [Fact]
    public void Null_message_throws_as_legacy()
    {
        var expected = Assert.ThrowsAny<Exception>(() => KafkaDeadLetterHeadersLegacy.Merge(null!));
        var actual = Assert.ThrowsAny<Exception>(() => KafkaDeadLetterHeaders.Merge(null!));

        Assert.Equal(expected.GetType(), actual.GetType());
        Assert.Equal(expected.Message, actual.Message);
    }

    [Fact]
    public void Missing_dead_letter_throws_as_legacy()
    {
        var message = Base with { DeadLetter = null };

        var expected = Assert.ThrowsAny<Exception>(() => KafkaDeadLetterHeadersLegacy.Merge(message));
        var actual = Assert.ThrowsAny<Exception>(() => KafkaDeadLetterHeaders.Merge(message));

        Assert.Equal(expected.GetType(), actual.GetType());
        Assert.Equal(expected.Message, actual.Message);
    }

    private static OutgoingMessage WithAttempts(int attempts) => Base with { DeadLetter = Base.DeadLetter! with { Attempts = attempts } };

    private static OutgoingMessage WithId(long id) => Base with { Source = Base.Source with { Id = id } };

    private static OutgoingMessage WithFailedAt(DateTimeOffset failedAt) => Base with { DeadLetter = Base.DeadLetter! with { FailedAt = failedAt } };

    private static OutgoingMessage WithReason(string reason) => Base with { DeadLetter = Base.DeadLetter! with { Reason = reason } };

    private static OutgoingMessage WithNames(string outbox, string destination) =>
        Base with { Source = Base.Source with { Outbox = outbox, Destination = destination } };

    private static OutgoingMessage WithHeaders(params string[] names) => Base with
    {
        Headers = new MessageHeaders([.. names.Select(n => new MessageHeader(n, Encoding.UTF8.GetBytes("v-" + n)))]),
    };
}
