using Pernix.OutboxRelay.Host.Configuration;
using Pernix.OutboxRelay.Hosting;
using Pernix.OutboxRelay.Hosting.Health;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace Pernix.OutboxRelay.Host.Telemetry;

public static class TelemetryRegistration
{
    // Traces and metrics only. No UseOtlpExporter() or WithLogging: logs must not go to OTLP.
    public static WebApplicationBuilder AddRelayTelemetry(this WebApplicationBuilder builder, RelayIdentity id)
    {
        builder.Services.AddOpenTelemetry()
            // Env detector after AddService so OTEL_SERVICE_NAME / OTEL_RESOURCE_ATTRIBUTES win.
            .ConfigureResource(r => r
                .AddService(id.ServiceName, serviceVersion: id.Version)
                .AddEnvironmentVariableDetector())
            .WithTracing(t => t
                .AddOutboxRelayInstrumentation()
                .AddAspNetCoreInstrumentation(o => o.Filter = ctx => !OutboxRelayHealthChecks.Paths.Contains(ctx.Request.Path.Value ?? string.Empty))
                .AddOtlpExporter())
            .WithMetrics(m => m
                .AddOutboxRelayInstrumentation()
                .AddAspNetCoreInstrumentation()
                .AddRuntimeInstrumentation()
                .AddOtlpExporter());

        return builder;
    }
}
