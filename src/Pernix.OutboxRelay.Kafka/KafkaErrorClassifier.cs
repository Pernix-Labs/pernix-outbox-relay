using Pernix.OutboxRelay.Core.Abstractions;
using Confluent.Kafka;

namespace Pernix.OutboxRelay.Kafka;

internal static class KafkaErrorClassifier
{
    // CorruptMessage in the Kafka protocol is ErrorCode.InvalidMsg in Confluent.Kafka.
    internal static readonly IReadOnlySet<ErrorCode> PermanentMessageFaults = new HashSet<ErrorCode>
    {
        ErrorCode.MsgSizeTooLarge,
        ErrorCode.InvalidRecord,
        ErrorCode.InvalidMsg,
        ErrorCode.RecordListTooLarge,
    };

    internal static readonly IReadOnlySet<ErrorCode> DestinationFaults = new HashSet<ErrorCode>
    {
        ErrorCode.Local_Transport,
        ErrorCode.Local_AllBrokersDown,
        ErrorCode.Local_UnknownTopic,
        ErrorCode.Local_UnknownPartition,
        ErrorCode.UnknownTopicOrPart,
        ErrorCode.TopicAuthorizationFailed,
        ErrorCode.ClusterAuthorizationFailed,
        ErrorCode.NotEnoughReplicas,
        ErrorCode.NotEnoughReplicasAfterAppend,
        ErrorCode.LeaderNotAvailable,
        ErrorCode.NotLeaderForPartition,
        ErrorCode.BrokerNotAvailable,
        ErrorCode.RequestTimedOut,
        ErrorCode.Local_QueueFull,
        ErrorCode.Local_Fatal,
        ErrorCode.Local_Destroy,
    };

    public static DeliveryResult Classify(Error error)
    {
        ArgumentNullException.ThrowIfNull(error);

        var text = $"{error.Code}: {error.Reason}";
        if (PermanentMessageFaults.Contains(error.Code))
        {
            return new DeliveryResult.MessageFault(text, Permanent: true);
        }

        // Local_MsgTimedOut lands here: the broker did not confirm, which says nothing about the message.
        return new DeliveryResult.DestinationFault(text);
    }
}
