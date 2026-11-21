using System.Collections.Concurrent;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Pernix.OutboxRelay.Integration.Tests.Delivery;

// Test seam for faults Kafka cannot produce on demand.
public sealed class FaultInjectingDestinationFactory(IDestinationFactory inner, FaultRules rules) : IDestinationFactory
{
    public string Transport => inner.Transport;

    public IReadOnlyList<string> Validate(DestinationOptions destination, string path, RelayOptions relay) =>
        inner.Validate(destination, path, relay);

    public IDestination Create(DestinationOptions destination, DestinationContext context)
    {
        var created = inner.Create(destination, context);

        return context.IsProbe ? created : new FaultInjectingDestination(created, destination.Name!, rules);
    }

    public static RelayFactory Decorate(RelayFactory factory, FaultRules rules) =>
        factory.WithServices(services =>
        {
            var registered = services.Where(d => d.ServiceType == typeof(IDestinationFactory)).ToList();
            foreach (var descriptor in registered)
            {
                services.Remove(descriptor);
                services.AddSingleton<IDestinationFactory>(sp => new FaultInjectingDestinationFactory(Resolve(sp, descriptor), rules));
            }
        });

    private static IDestinationFactory Resolve(IServiceProvider services, ServiceDescriptor descriptor) => descriptor switch
    {
        { ImplementationInstance: IDestinationFactory instance } => instance,
        { ImplementationFactory: { } create } => (IDestinationFactory)create(services),
        { ImplementationType: { } type } => (IDestinationFactory)ActivatorUtilities.CreateInstance(services, type),
        _ => throw new InvalidOperationException($"Cannot resolve destination factory registration {descriptor}."),
    };

    private sealed class FaultInjectingDestination(IDestination inner, string name, FaultRules rules) : IDestination
    {
        public TimeSpan? SendBound => inner.SendBound;

        public IReadOnlySet<string>? Routes => inner.Routes;

        public Task ValidateAsync(CancellationToken ct) => inner.ValidateAsync(ct);

        public ValueTask<DeliveryResult> SendAsync(OutgoingMessage message, CancellationToken ct)
        {
            if (rules.Match(name, message) is not { } rule || rule.TryInject() is not { } injected)
            {
                return inner.SendAsync(message, ct);
            }

            return rule.DelayFor(message) is var delay && delay > TimeSpan.Zero ? new ValueTask<DeliveryResult>(DelayedAsync(injected, delay, ct)) : new ValueTask<DeliveryResult>(injected);
        }

        public ValueTask DisposeAsync() => inner.DisposeAsync();

        private static async Task<DeliveryResult> DelayedAsync(DeliveryResult injected, TimeSpan delay, CancellationToken ct)
        {
            await Task.Delay(delay, ct).ConfigureAwait(false);
            return injected;
        }
    }
}

public sealed class FaultRules
{
    public const string TagHeader = "fault-tag";

    private readonly ConcurrentDictionary<(string Destination, string Tag), FaultRule> rules = new();

    // delay: optional wait per message before the injected result returns.
    public FaultRule Add(string destination, string tag, int times, DeliveryResult result, Func<OutgoingMessage, TimeSpan>? delay = null)
    {
        var rule = new FaultRule(times, result, delay);
        if (!rules.TryAdd((destination, tag), rule))
        {
            throw new InvalidOperationException($"Rule for destination '{destination}' and tag '{tag}' already exists.");
        }

        return rule;
    }

    internal FaultRule? Match(string destination, OutgoingMessage message) =>
        message.Headers.ToStrings().TryGetValue(TagHeader, out var tag) && rules.TryGetValue((destination, tag), out var rule) ? rule : null;
}

public sealed class FaultRule(int times, DeliveryResult result, Func<OutgoingMessage, TimeSpan>? delay = null)
{
    private int sends;

    public int Sends => Volatile.Read(ref sends);

    internal DeliveryResult? TryInject() => Interlocked.Increment(ref sends) <= times ? result : null;

    internal TimeSpan DelayFor(OutgoingMessage message) => delay?.Invoke(message) ?? TimeSpan.Zero;
}
