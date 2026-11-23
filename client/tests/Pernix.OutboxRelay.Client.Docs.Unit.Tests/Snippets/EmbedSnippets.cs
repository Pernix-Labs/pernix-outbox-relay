#region docs:readme-embed-usings
using Pernix.OutboxRelay.Hosting;
using Pernix.OutboxRelay.Outbox.PostgreSql;
using Pernix.OutboxRelay.Destination.Kafka;
#endregion
using Microsoft.AspNetCore.Builder;

namespace Pernix.OutboxRelay.Client.Docs.Unit.Tests.Snippets;

internal static class EmbedSnippets
{
    public static WebApplication Register(WebApplicationBuilder builder)
    {
        #region docs:readme-embed-register
        builder.AddOutboxRelay().AddPostgreSqlOutbox().AddKafkaDestination();

        var app = builder.Build();
        app.MapOutboxRelayHealth();   // optional: /livez and /readyz
        #endregion
        return app;
    }
}
