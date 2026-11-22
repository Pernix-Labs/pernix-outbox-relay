using Pernix.OutboxRelay.Core.Abstractions;
using NanovHeaders = Nanov.Kafka.KafkaHeaders;

namespace Pernix.OutboxRelay.Destination.Kafka;

internal static class KafkaHeaders
{
    internal const int Inline = 8; // Nanov.Kafka.KafkaHeaders inline capacity at 2.16.0.3

    public static bool Fill(MessageHeaders headers, ref NanovHeaders first, ref NanovHeaders second)
    {
        for (var i = 0; i < headers.Count; i++)
        {
            var header = headers[i];
            if (header.Value.Length == 0)
            {
                return false;
            }

            if (i < Inline)
            {
                first.Add(header.Name, header.Value);
            }
            else
            {
                second.Add(header.Name, header.Value);
            }
        }

        return true;
    }
}
