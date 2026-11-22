using System.Diagnostics;
using Pernix.OutboxRelay.Core.Diagnostics;
using Pernix.OutboxRelay.Hosting;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Hosting;

public sealed class OutboxRelayTelemetryExtensionsTests
{
    [Fact]
    public void Tracer_helper_captures_relay_activities()
    {
        var exported = new List<Activity>();
        using (var provider = Sdk.CreateTracerProviderBuilder()
            .AddOutboxRelayInstrumentation()
            .AddInMemoryExporter(exported)
            .Build())
        {
            using var activity = RelayDiagnostics.ActivitySource.StartActivity("relay-telemetry-test");
            Assert.NotNull(activity);
        }

        Assert.Contains(exported, a => a.Source.Name == RelayDiagnostics.Name && a.OperationName == "relay-telemetry-test");
    }

    [Fact]
    public void Meter_helper_exports_relay_instruments()
    {
        // Unique name: RelayDiagnostics.Meter is process-wide and shared with other tests.
        var instrumentName = $"outbox.test.{Guid.NewGuid():N}";
        var exported = new List<Metric>();
        using var provider = Sdk.CreateMeterProviderBuilder()
            .AddOutboxRelayInstrumentation()
            .AddInMemoryExporter(exported)
            .Build();

        RelayDiagnostics.Meter.CreateCounter<long>(instrumentName).Add(1);
        provider.ForceFlush();

        Assert.Contains(exported, m => m.MeterName == RelayDiagnostics.Name && m.Name == instrumentName);
    }
}
