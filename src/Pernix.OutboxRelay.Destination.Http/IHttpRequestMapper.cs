using Pernix.OutboxRelay.Core.Abstractions;

namespace Pernix.OutboxRelay.Destination.Http;

public interface IHttpRequestMapper
{
    // Mutates request (URL, headers, content). Null: send. Non-null: return it, nothing sent.
    DeliveryResult? Map(OutgoingMessage message, HttpRequestMessage request);
}
