using Microsoft.EntityFrameworkCore;

namespace Pernix.OutboxRelay.Client.EntityFrameworkCore;

internal sealed class RouteScopedOutboxQueue<TContext>(TContext context, string route) : IOutboxQueue<TContext>
    where TContext : DbContext
{
    public void Enqueue(OutboxMessage message)
    {
        var resolved = RouteRules.Resolve(message.Route, route, -1);
        context.EnqueueOutbox(message.Route is null ? message.WithRoute(resolved) : message);
    }
}
