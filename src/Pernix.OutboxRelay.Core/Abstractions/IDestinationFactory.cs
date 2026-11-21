using Pernix.OutboxRelay.Core.Configuration;

namespace Pernix.OutboxRelay.Core.Abstractions;

public interface IDestinationFactory
{
    string Transport { get; }

    IReadOnlyList<string> Validate(DestinationOptions destination, string path, RelayOptions relay);

    IDestination Create(DestinationOptions destination, DestinationContext context);
}
