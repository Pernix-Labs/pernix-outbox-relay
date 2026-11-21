using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Pernix.OutboxRelay.Integration.Tests.Delivery;

public enum StatsFaultMode
{
    PassThrough,
    Throw,
    Hang,
}

public sealed class StatsFault(StatsFaultMode mode = StatsFaultMode.PassThrough)
{
    public const string Reason = "scripted fault";

    private int mode = (int)mode;
    private int calls;

    public StatsFaultMode Mode
    {
        get => (StatsFaultMode)Volatile.Read(ref mode);
        set => Volatile.Write(ref mode, (int)value);
    }

    public int Calls => Volatile.Read(ref calls);

    internal Task<OutboxStats> GetStatsAsync(IValidatedOutboxStore inner, CancellationToken ct)
    {
        Interlocked.Increment(ref calls);
        return Mode switch
        {
            StatsFaultMode.Throw => Task.FromException<OutboxStats>(new OutboxStatsUnavailableException("test", Reason)),

            // Never completes and ignores ct, like a provider that does not honour its own deadline.
            StatsFaultMode.Hang => new TaskCompletionSource<OutboxStats>(TaskCreationOptions.RunContinuationsAsynchronously).Task,
            _ => inner.GetStatsAsync(ct),
        };
    }
}

// Stats isolation seam: delegates everything, GetStatsAsync follows the StatsFault script.
public sealed class StatsFaultingStoreProvider(IOutboxStoreProvider inner, StatsFault fault) : IOutboxStoreProvider
{
    public string Name => inner.Name;

    public IReadOnlySet<OrderingMode> SupportedModes => inner.SupportedModes;

    public IOutboxStore CreateStore(OutboxOptions options) => new Store(inner.CreateStore(options), fault);

    public static RelayFactory Decorate(RelayFactory factory, StatsFault fault) =>
        factory.WithServices(services =>
        {
            var registered = services.Where(d => d.ServiceType == typeof(IOutboxStoreProvider)).ToList();
            foreach (var descriptor in registered)
            {
                services.Remove(descriptor);
                services.AddSingleton<IOutboxStoreProvider>(sp => new StatsFaultingStoreProvider(Resolve(sp, descriptor), fault));
            }
        });

    private static IOutboxStoreProvider Resolve(IServiceProvider services, ServiceDescriptor descriptor) => descriptor switch
    {
        { ImplementationInstance: IOutboxStoreProvider instance } => instance,
        { ImplementationFactory: { } create } => (IOutboxStoreProvider)create(services),
        { ImplementationType: { } type } => (IOutboxStoreProvider)ActivatorUtilities.CreateInstance(services, type),
        _ => throw new InvalidOperationException($"Cannot resolve outbox store provider registration {descriptor}."),
    };

    private sealed class Store(IOutboxStore inner, StatsFault fault) : IOutboxStore
    {
        public async Task<IValidatedOutboxStore> ValidateAsync(CancellationToken ct) =>
            new ValidatedStore(await inner.ValidateAsync(ct), fault);
    }

    private sealed class ValidatedStore(IValidatedOutboxStore inner, StatsFault fault) : IValidatedOutboxStore
    {
        public Task<IOutboxSession> OpenSessionAsync(CancellationToken ct) => inner.OpenSessionAsync(ct);

        public Task<OutboxStats> GetStatsAsync(CancellationToken ct) => fault.GetStatsAsync(inner, ct);
    }
}
