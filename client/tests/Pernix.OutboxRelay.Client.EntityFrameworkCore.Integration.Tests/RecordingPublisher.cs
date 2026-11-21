using System.Data.Common;

namespace Pernix.OutboxRelay.Client.EntityFrameworkCore.Integration.Tests;

public sealed class RecordingPublisher(IOutboxPublisher inner) : IOutboxPublisher
{
    private readonly List<int> batchSizes = [];

    public OutboxTable Table => inner.Table;

    public int Calls => batchSizes.Count;

    public IReadOnlyList<int> BatchSizes => batchSizes;

    public Task PublishAsync(DbTransaction transaction, OutboxMessage message, CancellationToken ct = default)
    {
        batchSizes.Add(1);
        return inner.PublishAsync(transaction, message, ct);
    }

    public Task PublishAsync(DbTransaction transaction, IReadOnlyList<OutboxMessage> messages, CancellationToken ct = default)
    {
        batchSizes.Add(messages.Count);
        return inner.PublishAsync(transaction, messages, ct);
    }

    public Task PublishEnlistedAsync(DbConnection connection, OutboxMessage message, CancellationToken ct = default)
    {
        batchSizes.Add(1);
        return inner.PublishEnlistedAsync(connection, message, ct);
    }

    public Task PublishEnlistedAsync(DbConnection connection, IReadOnlyList<OutboxMessage> messages, CancellationToken ct = default)
    {
        batchSizes.Add(messages.Count);
        return inner.PublishEnlistedAsync(connection, messages, ct);
    }
}
