using Pernix.OutboxRelay.Client.EntityFrameworkCore;
using Pernix.OutboxRelay.Client.PostgreSql;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace Pernix.OutboxRelay.Client.Unit.Tests.EntityFrameworkCore;

public sealed class RouteScopedQueueAllocationTests
{
    private const int WarmUp = 3;
    private const int Iterations = 1000;
    private const string Route = "alerts";

    private readonly PostgreSqlOutboxPublisher publisher = new(OutboxTable.Parse("s.t"));

    public enum MessageRoute
    {
        Null,
        Equal,
    }

    [Theory]
    [InlineData(MessageRoute.Null)]
    [InlineData(MessageRoute.Equal)]
    public void Keyed_Enqueue_allocates_nothing_after_first_call(MessageRoute messageRoute)
    {
        using var context = new OfflineContext(
            new DbContextOptionsBuilder<OfflineContext>().UseNpgsql("Host=unused").UseOutbox(publisher).Options);
        var outboxQueue = OutboxQueue.Find(context)!;
        var keyed = new RouteScopedOutboxQueue<OfflineContext>(context, Route);
        var message = messageRoute == MessageRoute.Null
            ? OutboxMessage.Create("k", "p")
            : OutboxMessage.Create("k", "p").WithRoute(Route);

        Run(keyed, outboxQueue, message, WarmUp);
        var before = GC.GetAllocatedBytesForCurrentThread();
        Run(keyed, outboxQueue, message, Iterations);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        TestContext.Current.TestOutputHelper?.WriteLine($"Keyed Enqueue ({messageRoute} route): {allocated} B over {Iterations} iterations");
        Assert.Equal(0, allocated);

        keyed.Enqueue(message);
        Assert.Same(Route, Assert.Single(outboxQueue.Messages).Route);
    }

    private static void Run(RouteScopedOutboxQueue<OfflineContext> keyed, OutboxQueue queue, OutboxMessage message, int count)
    {
        for (var i = 0; i < count; i++)
        {
            keyed.Enqueue(message);
            queue.Clear();
        }
    }

    public sealed class OfflineContext(DbContextOptions<OfflineContext> options) : DbContext(options);
}
