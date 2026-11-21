using Microsoft.AspNetCore.Diagnostics.HealthChecks;

namespace Pernix.OutboxRelay.Host.Health;

public static class HealthTags
{
    public const string Live = "live";
    public const string Ready = "ready";
}

public static class HealthPaths
{
    public const string Live = "/livez";
    public const string Ready = "/readyz";

    public static readonly IReadOnlySet<string> All = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { Live, Ready };
}

public static class HealthRegistration
{
    public static WebApplicationBuilder AddRelayHealth(this WebApplicationBuilder builder)
    {
        builder.Services.AddHealthChecks()
            .AddCheck<HostLifetimeReadinessCheck>("host-lifetime", tags: [HealthTags.Ready])
            .AddCheck<OutboxDatabaseReadinessCheck>("outbox-database", tags: [HealthTags.Ready])
            .AddCheck<WorkerLoopLivenessCheck>("worker-loop", tags: [HealthTags.Live]);

        return builder;
    }

    public static WebApplication MapRelayHealth(this WebApplication app)
    {
        app.MapHealthChecks(HealthPaths.Live, new HealthCheckOptions
        {
            Predicate = check => check.Tags.Contains(HealthTags.Live),
        });

        app.MapHealthChecks(HealthPaths.Ready, new HealthCheckOptions
        {
            Predicate = check => check.Tags.Contains(HealthTags.Ready),
        });

        return app;
    }
}
