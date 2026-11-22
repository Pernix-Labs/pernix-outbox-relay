using Pernix.OutboxRelay.Hosting.Health;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.AspNetCore.Routing;

namespace Pernix.OutboxRelay.Hosting;

public static class OutboxRelayEndpointRouteBuilderExtensions
{
    public static IEndpointRouteBuilder MapOutboxRelayHealth(this IEndpointRouteBuilder endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);

        endpoints.MapHealthChecks(OutboxRelayHealthChecks.LivePath, new HealthCheckOptions
        {
            Predicate = check => check.Tags.Contains(OutboxRelayHealthChecks.LiveTag),
        });

        endpoints.MapHealthChecks(OutboxRelayHealthChecks.ReadyPath, new HealthCheckOptions
        {
            Predicate = check => check.Tags.Contains(OutboxRelayHealthChecks.ReadyTag),
        });

        return endpoints;
    }
}
