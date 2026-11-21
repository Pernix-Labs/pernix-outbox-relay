using Pernix.OutboxRelay.Client.PostgreSql;
using Microsoft.Extensions.DependencyInjection;

namespace Pernix.OutboxRelay.Client.Docs.Unit.Tests.Snippets;

internal static class RegistrationSnippets
{
    public static void RegisterPublisher(IServiceCollection services)
    {
        #region docs:register-publisher
        // Parses the table text now: a bad text fails at startup.
        services.AddPostgreSqlOutboxPublisher("sales.outbox");
        #endregion
    }

    public static void RegisterKeyedPublishers(IServiceCollection services)
    {
        #region docs:register-keyed-publishers
        services.AddKeyedPostgreSqlOutboxPublisher("sales", "sales.outbox");
        services.AddKeyedPostgreSqlOutboxPublisher("billing", "billing.outbox");
        #endregion
    }
}

#region docs:register-keyed-inject
public sealed class InvoiceService([FromKeyedServices("billing")] IOutboxPublisher publisher)
{
    public IOutboxPublisher Publisher => publisher;
}
#endregion
