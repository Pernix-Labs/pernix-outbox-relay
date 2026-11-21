using System.Data.Common;

namespace Pernix.OutboxRelay.Client.EntityFrameworkCore.Integration.Tests;

public sealed class ThrowingPublisher(IOutboxPublisher inner, Exception exception, int failCount = int.MaxValue) : IOutboxPublisher
{
    private int calls;

    public OutboxTable Table => inner.Table;

    public int Calls => calls;

    public Task PublishAsync(DbTransaction transaction, OutboxMessage message, CancellationToken ct = default) =>
        Fail() ? Task.FromException(exception) : inner.PublishAsync(transaction, message, ct);

    public Task PublishAsync(DbTransaction transaction, IReadOnlyList<OutboxMessage> messages, CancellationToken ct = default) =>
        Fail() ? Task.FromException(exception) : inner.PublishAsync(transaction, messages, ct);

    public Task PublishEnlistedAsync(DbConnection connection, OutboxMessage message, CancellationToken ct = default) =>
        Fail() ? Task.FromException(exception) : inner.PublishEnlistedAsync(connection, message, ct);

    public Task PublishEnlistedAsync(DbConnection connection, IReadOnlyList<OutboxMessage> messages, CancellationToken ct = default) =>
        Fail() ? Task.FromException(exception) : inner.PublishEnlistedAsync(connection, messages, ct);

    private bool Fail() => ++calls <= failCount;
}
