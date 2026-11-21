using System.Collections.Concurrent;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Pernix.OutboxRelay.Integration.Tests.Delivery;

// Test seam for "producer disposed": wraps the registered transport and records Create and DisposeAsync for non-probe destinations.
public sealed class RecordingDestinationFactory(
    IDestinationFactory inner,
    DestinationRecorder recorder) : IDestinationFactory
{
    public string Transport => inner.Transport;

    public IReadOnlyList<string> Validate(DestinationOptions destination, string path, RelayOptions relay) =>
        inner.Validate(destination, path, relay);

    public IDestination Create(DestinationOptions destination, DestinationContext context)
    {
        var created = inner.Create(destination, context);

        // Records non-probe destinations only. Startup probes never send.
        return context.IsProbe ? created : recorder.Track(created);
    }

    public static RelayFactory Decorate(RelayFactory factory, DestinationRecorder recorder) =>
        factory.WithServices(services =>
        {
            var registered = services.Where(d => d.ServiceType == typeof(IDestinationFactory)).ToList();
            foreach (var descriptor in registered)
            {
                services.Remove(descriptor);
                services.AddSingleton<IDestinationFactory>(sp => new RecordingDestinationFactory(Resolve(sp, descriptor), recorder));
            }
        });

    private static IDestinationFactory Resolve(IServiceProvider services, ServiceDescriptor descriptor) => descriptor switch
    {
        { ImplementationInstance: IDestinationFactory instance } => instance,
        { ImplementationFactory: { } create } => (IDestinationFactory)create(services),
        { ImplementationType: { } type } => (IDestinationFactory)ActivatorUtilities.CreateInstance(services, type),
        _ => throw new InvalidOperationException($"Cannot resolve destination factory registration {descriptor}."),
    };
}

public sealed class DestinationRecorder
{
    private readonly ConcurrentQueue<RecordedDestination> destinations = new();

    // One sequence for every destination of this recorder, so send logs of different instances compare.
    private long sequence;

    public IReadOnlyList<RecordedDestination> Destinations => [.. destinations];

    internal IDestination Track(IDestination inner)
    {
        var recorded = new RecordedDestination(inner, DateTimeOffset.UtcNow, this);
        destinations.Enqueue(recorded);
        return recorded;
    }

    internal long Next() => Interlocked.Increment(ref sequence);
}

public readonly record struct RecordedSend(long Id, byte[]? Key, string? Route, long Start, long End);

public sealed class RecordedDestination(IDestination inner, DateTimeOffset createdAt, DestinationRecorder? recorder = null) : IDestination
{
    private readonly ConcurrentQueue<RecordedSend> sends = new();
    private long ownSequence;
    private long disposedAtTicks;
    private int inFlight;

    public DateTimeOffset CreatedAt => createdAt;

    public DateTimeOffset? DisposedAt => Interlocked.Read(ref disposedAtTicks) is var ticks and > 0 ? new DateTimeOffset(ticks, TimeSpan.Zero) : null;

    public int InFlight => Volatile.Read(ref inFlight);

    public TimeSpan? SendBound => inner.SendBound;

    public IReadOnlySet<string>? Routes => inner.Routes;

    public IReadOnlyList<RecordedSend> Sends => [.. sends];

    public Task ValidateAsync(CancellationToken ct) => inner.ValidateAsync(ct);

    public async ValueTask<DeliveryResult> SendAsync(OutgoingMessage message, CancellationToken ct)
    {
        var start = Next();
        Interlocked.Increment(ref inFlight);
        try
        {
            return await inner.SendAsync(message, ct);
        }
        finally
        {
            // Before this method's task completes, so Core observes completion only after End.
            sends.Enqueue(new RecordedSend(message.Source.Id, message.Key, message.Route, start, Next()));
            Interlocked.Decrement(ref inFlight);
        }
    }

    private long Next() => recorder?.Next() ?? Interlocked.Increment(ref ownSequence);

    public async ValueTask DisposeAsync()
    {
        await inner.DisposeAsync();
        Interlocked.CompareExchange(ref disposedAtTicks, DateTimeOffset.UtcNow.UtcTicks, 0);
    }
}
