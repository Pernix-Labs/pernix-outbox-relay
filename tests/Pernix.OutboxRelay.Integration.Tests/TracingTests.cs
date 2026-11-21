using System.Diagnostics;
using Microsoft.AspNetCore.TestHost;
using OpenTelemetry.Trace;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests;

public sealed class TracingTests
{
    private const string AspNetCoreSource = "Microsoft.AspNetCore";
    private const string ControlPath = "/trace-control";

    [Fact]
    public async Task Health_request_records_no_aspnetcore_activity()
    {
        var exported = new List<Activity>();
        await using var factory = new RelayFactory().WithWebHostBuilder(b => b.ConfigureTestServices(s =>
            s.ConfigureOpenTelemetryTracerProvider(t => t.AddInMemoryExporter(exported))));
        using var client = factory.CreateClient();
        var ct = TestContext.Current.CancellationToken;

        (await client.GetAsync("/livez", ct)).Dispose();

        // Positive control: a non-health request must be exported, proving the pipeline records.
        (await client.GetAsync(ControlPath, ct)).Dispose();
        await WaitUntilAsync(() => Snapshot(exported).Any(a => IsAspNetCore(a, ControlPath)), ct);

        Assert.DoesNotContain(Snapshot(exported), a => IsAspNetCore(a, "/livez"));
    }

    private static bool IsAspNetCore(Activity activity, string path) =>
        activity.Source.Name == AspNetCoreSource && (activity.GetTagItem("url.path") as string) == path;

    private static Activity[] Snapshot(List<Activity> exported)
    {
        lock (exported)
        {
            return [.. exported];
        }
    }

    private static async Task WaitUntilAsync(Func<bool> condition, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "Control activity was not exported in time.");
            await Task.Delay(50, ct);
        }
    }
}
