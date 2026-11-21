using Pernix.OutboxRelay.Core.Abstractions;

namespace Pernix.OutboxRelay.Benchmarks.Infrastructure;

internal sealed class NullDestination : IDestination
{
    private static readonly DeliveryResult Done = DeliveryResult.Delivered.Instance;

    public TimeSpan? SendBound => TimeSpan.FromSeconds(30);

    public IReadOnlySet<string>? Routes => null;

    public Task ValidateAsync(CancellationToken ct) => Task.CompletedTask;

    public ValueTask<DeliveryResult> SendAsync(OutgoingMessage message, CancellationToken ct) => new(Done);

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

internal sealed class PermanentFaultDestination : IDestination
{
    private static readonly DeliveryResult Fault = new DeliveryResult.MessageFault("MsgSizeTooLarge: bench", Permanent: true);

    public TimeSpan? SendBound => TimeSpan.FromSeconds(30);

    public IReadOnlySet<string>? Routes => null;

    public Task ValidateAsync(CancellationToken ct) => Task.CompletedTask;

    public ValueTask<DeliveryResult> SendAsync(OutgoingMessage message, CancellationToken ct) => new(Fault);

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
