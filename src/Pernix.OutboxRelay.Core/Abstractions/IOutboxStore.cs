namespace Pernix.OutboxRelay.Core.Abstractions;

public interface IOutboxStore
{
    Task<IValidatedOutboxStore> ValidateAsync(CancellationToken ct);
}
