using Pernix.OutboxRelay.Core.Abstractions;

namespace Pernix.OutboxRelay.Destination.Http;

// Replaces the StatusCodes table. bodyPrefix: up to the first 4 KB of the body.
public interface IHttpResponseClassifier
{
    DeliveryResult Classify(HttpResponseMessage response, ReadOnlySpan<byte> bodyPrefix);
}
