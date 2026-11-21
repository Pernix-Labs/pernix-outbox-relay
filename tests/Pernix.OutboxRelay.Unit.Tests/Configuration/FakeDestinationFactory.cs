using System.Collections.Concurrent;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;

namespace Pernix.OutboxRelay.Unit.Tests.Configuration;

internal sealed class FakeDestinationFactory(string transport = "Kafka") : IDestinationFactory
{
    public string Transport { get; } = transport;

    public Dictionary<string, IReadOnlyList<string>> ValidateFailures { get; } = new(StringComparer.OrdinalIgnoreCase);

    public Dictionary<string, Func<CancellationToken, Task>> Probes { get; } = new(StringComparer.OrdinalIgnoreCase);

    // SendBound of term-shaped instances per destination name; probe instances always declare null.
    public Dictionary<string, Func<DestinationContext, TimeSpan?>> Bounds { get; } = new(StringComparer.OrdinalIgnoreCase);

    // Routes of created instances per destination name; default null (one address).
    public Dictionary<string, IReadOnlySet<string>?> Routes { get; } = new(StringComparer.OrdinalIgnoreCase);

    public ConcurrentQueue<(string Destination, string Path)> Validated { get; } = new();

    public ConcurrentQueue<(string Destination, DestinationContext Context)> Created { get; } = new();

    public ConcurrentQueue<string> ProbeStarted { get; } = new();

    public ConcurrentQueue<string> Disposed { get; } = new();

    public IReadOnlyList<string> Validate(DestinationOptions destination, string path, RelayOptions relay)
    {
        Validated.Enqueue((destination.Name ?? string.Empty, path));
        return ValidateFailures.TryGetValue(destination.Name ?? string.Empty, out var failures) ? failures : [];
    }

    public IDestination Create(DestinationOptions destination, DestinationContext context)
    {
        Created.Enqueue((destination.Name!, context));
        var sendBound = !context.IsProbe && Bounds.TryGetValue(destination.Name!, out var bounds) ? bounds(context) : default;
        var routes = Routes.TryGetValue(destination.Name!, out var configured) ? configured : null;
        return new Probe(this, destination.Name!, sendBound, routes);
    }

    private sealed class Probe(FakeDestinationFactory factory, string name, TimeSpan? sendBound, IReadOnlySet<string>? routes) : IDestination
    {
        public TimeSpan? SendBound => sendBound;

        public IReadOnlySet<string>? Routes => routes;

        public async Task ValidateAsync(CancellationToken ct)
        {
            factory.ProbeStarted.Enqueue(name);
            if (factory.Probes.TryGetValue(name, out var probe))
            {
                await probe(ct);
            }
        }

        public ValueTask<DeliveryResult> SendAsync(OutgoingMessage message, CancellationToken ct) =>
            throw new NotSupportedException("A probe never sends.");

        public ValueTask DisposeAsync()
        {
            factory.Disposed.Enqueue(name);
            return ValueTask.CompletedTask;
        }
    }
}
