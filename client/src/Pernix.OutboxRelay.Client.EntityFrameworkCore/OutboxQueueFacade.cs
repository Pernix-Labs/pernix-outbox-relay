using Microsoft.EntityFrameworkCore;

namespace Pernix.OutboxRelay.Client.EntityFrameworkCore;

internal sealed class OutboxQueueFacade<TContext> : IOutboxQueue<TContext>
    where TContext : DbContext
{
    private readonly TContext context;

    public OutboxQueueFacade(TContext context)
    {
        this.context = context;
    }

    public void Enqueue(OutboxMessage message) => context.EnqueueOutbox(message);
}
