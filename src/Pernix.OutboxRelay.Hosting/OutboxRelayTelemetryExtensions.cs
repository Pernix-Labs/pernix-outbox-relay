using Pernix.OutboxRelay.Core.Diagnostics;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

namespace Pernix.OutboxRelay.Hosting;

// OpenTelemetry.Api only. Exporters and other instrumentation are the host's choice.
public static class OutboxRelayTelemetryExtensions
{
    public static TracerProviderBuilder AddOutboxRelayInstrumentation(this TracerProviderBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.AddSource(RelayDiagnostics.Name);
    }

    public static MeterProviderBuilder AddOutboxRelayInstrumentation(this MeterProviderBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.AddMeter(RelayDiagnostics.Name);
    }
}
