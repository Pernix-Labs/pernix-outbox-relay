using Microsoft.Extensions.Logging;

namespace Pernix.OutboxRelay.Client.EntityFrameworkCore;

internal static class OutboxLog
{
    public static readonly Action<ILogger, int, string, Exception?> Discarded = LoggerMessage.Define<int, string>(
        LogLevel.Warning,
        new EventId(1, "OutboxMessagesDiscarded"),
        "Discarded {Count} queued outbox messages for outbox table {OutboxTable}. The context was disposed or returned to the pool, or its EF transaction ended without CommitAsync or a rollback through EF.");
}
