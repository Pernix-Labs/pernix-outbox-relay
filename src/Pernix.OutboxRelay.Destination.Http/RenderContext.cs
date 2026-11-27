using Pernix.OutboxRelay.Core.Abstractions;

namespace Pernix.OutboxRelay.Destination.Http;

// The per-row values a ValueTemplate reads. Outbox, destination and route are folded at compile, so they are absent here.
internal readonly struct RenderContext(long id, byte[]? key, MessageHeaders headers)
{
    public long Id { get; } = id;

    public byte[]? Key { get; } = key;

    public MessageHeaders Headers { get; } = headers;

    public static RenderContext From(OutgoingMessage message) => new(message.Source.Id, message.Key, message.Headers);
}
