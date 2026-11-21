using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Kafka;
using Confluent.Kafka;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Kafka;

public sealed class KafkaErrorClassifierTests
{
    private const string Permanent = "permanent";
    private const string Destination = "destination";

    // In no set: falls to the default DestinationFault.
    private const string Default = "default";

    public static TheoryData<string, string> Table => new()
    {
        { "MsgSizeTooLarge", Permanent },
        { "InvalidRecord", Permanent },
        { "InvalidMsg", Permanent },
        { "RecordListTooLarge", Permanent },
        { "Local_MsgTimedOut", Default },
        { "Local_Transport", Destination },
        { "Local_AllBrokersDown", Destination },
        { "Local_UnknownTopic", Destination },
        { "Local_UnknownPartition", Destination },
        { "UnknownTopicOrPart", Destination },
        { "TopicAuthorizationFailed", Destination },
        { "ClusterAuthorizationFailed", Destination },
        { "NotEnoughReplicas", Destination },
        { "NotEnoughReplicasAfterAppend", Destination },
        { "LeaderNotAvailable", Destination },
        { "NotLeaderForPartition", Destination },
        { "BrokerNotAvailable", Destination },
        { "RequestTimedOut", Destination },
        { "Local_QueueFull", Destination },
        { "Local_Fatal", Destination },
        { "Local_Destroy", Destination },
    };

    [Theory]
    [MemberData(nameof(Table))]
    public void Table_entry_is_classified(string name, string kind)
    {
        var code = Enum.Parse<ErrorCode>(name);

        var result = KafkaErrorClassifier.Classify(new Error(code, "reason"));

        object expected = kind switch
        {
            Permanent => new DeliveryResult.MessageFault($"{name}: reason", Permanent: true),
            _ => new DeliveryResult.DestinationFault($"{name}: reason"),
        };
        Assert.Equal(expected, result);
    }

    [Fact]
    public void Classifier_sets_hold_exactly_the_table()
    {
        var table = Table.Select(row => (row.Data.Item1, row.Data.Item2)).ToList();

        Assert.Equal(Codes(table, Permanent), KafkaErrorClassifier.PermanentMessageFaults.Order());
        Assert.Equal(Codes(table, Destination), KafkaErrorClassifier.DestinationFaults.Order());
    }

    [Theory]
    [InlineData(ErrorCode.Unknown)]
    [InlineData(ErrorCode.InvalidConfig)]
    [InlineData(ErrorCode.Local_Fail)]
    [InlineData(ErrorCode.OutOfOrderSequenceNumber)]
    public void Code_not_in_table_is_destination_fault(ErrorCode code)
    {
        Assert.Equal(new DeliveryResult.DestinationFault($"{code}: client reason"), KafkaErrorClassifier.Classify(new Error(code, "client reason")));
    }

    [Fact]
    public void Retriable_flag_is_ignored()
    {
        Assert.IsType<DeliveryResult.DestinationFault>(KafkaErrorClassifier.Classify(new Error(ErrorCode.Local_MsgTimedOut, "x")));
        Assert.IsType<DeliveryResult.DestinationFault>(KafkaErrorClassifier.Classify(new Error(ErrorCode.Local_Fatal, "x", isFatal: true)));
    }

    [Fact]
    public void Local_MsgTimedOut_is_destination_fault()
    {
        Assert.Equal(
            new DeliveryResult.DestinationFault("Local_MsgTimedOut: Message timed out"),
            KafkaErrorClassifier.Classify(new Error(ErrorCode.Local_MsgTimedOut, "Message timed out")));
    }

    [Fact]
    public void No_error_code_yields_transient_message_fault()
    {
        var transient = Enum.GetValues<ErrorCode>()
            .Where(code => KafkaErrorClassifier.Classify(new Error(code, "reason")) is DeliveryResult.MessageFault { Permanent: false })
            .ToList();

        Assert.Empty(transient);
    }

    private static IEnumerable<ErrorCode> Codes(List<(string Name, string Kind)> table, string kind) =>
        table.Where(row => row.Kind == kind).Select(row => Enum.Parse<ErrorCode>(row.Name)).Order();
}
