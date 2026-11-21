using Pernix.OutboxRelay.Core.Configuration;

namespace Pernix.OutboxRelay.Core.Abstractions;

public interface IOutboxStoreProvider
{
    string Name { get; }
    IReadOnlySet<OrderingMode> SupportedModes { get; }
    IOutboxStore CreateStore(OutboxOptions options);
}
