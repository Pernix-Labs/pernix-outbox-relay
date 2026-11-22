using Pernix.OutboxRelay.Core.Abstractions;

namespace Pernix.OutboxRelay.Core.Configuration;

public sealed class DestinationFactoryResolver
{
    private readonly Dictionary<string, IDestinationFactory> byTransport = new(StringComparer.OrdinalIgnoreCase);

    internal DestinationFactoryResolver(IEnumerable<IDestinationFactory> factories)
    {
        ArgumentNullException.ThrowIfNull(factories);

        foreach (var factory in factories)
        {
            if (!byTransport.TryAdd(factory.Transport, factory))
            {
                throw new InvalidOperationException($"Destination transport '{factory.Transport}' is registered more than once.");
            }
        }
    }

    public IDestinationFactory? Resolve(DestinationOptions destination, string path, ICollection<string> failures)
    {
        ArgumentNullException.ThrowIfNull(destination);
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(failures);

        if (string.IsNullOrWhiteSpace(destination.Transport))
        {
            return null;
        }

        if (byTransport.TryGetValue(destination.Transport, out var factory))
        {
            return factory;
        }

        failures.Add($"{path}.Transport: transport '{destination.Transport}' is not registered");
        return null;
    }

    internal IDestinationFactory Get(DestinationOptions destination) =>
        destination.Transport is { } transport && byTransport.TryGetValue(transport, out var factory)
            ? factory
            : throw new InvalidOperationException($"Destination '{destination.Name}' has no resolved factory.");
}
