using Pernix.OutboxRelay.Benchmarks.Infrastructure;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using BenchmarkDotNet.Attributes;

namespace Pernix.OutboxRelay.Benchmarks.Suites;

// Reused 500-message ring: Kafka does not dedupe.
[Config(typeof(BenchConfig))]
[WarmupCount(3)]
[IterationCount(10)]
[InvocationCount(16)]
public class KafkaSendBenchmarks
{
    private const int Window = 500;

    private readonly ValueTask<DeliveryResult>[] tasks = new ValueTask<DeliveryResult>[Window];
    private IDestination destination = null!;
    private OutgoingMessage[] messages = null!;

    [Params(0, 1, 4)]
    public int Headers { get; set; }

    [GlobalSetup]
    public async Task SetupAsync()
    {
        destination = KafkaMock.Create(OrderingMode.None);
        await destination.ValidateAsync(CancellationToken.None);
        messages = Workload.Messages(1, Window, Headers);
    }

    [GlobalCleanup]
    public async Task CleanupAsync() => await destination.DisposeAsync();

    [Benchmark(OperationsPerInvoke = Window)]
    public async Task Send()
    {
        for (var i = 0; i < Window; i++)
        {
            tasks[i] = destination.SendAsync(messages[i], CancellationToken.None);
        }

        for (var i = 0; i < Window; i++)
        {
            var result = await tasks[i];
            tasks[i] = default;
            if (result is not DeliveryResult.Delivered)
            {
                throw new InvalidOperationException("kafka send: message not delivered");
            }
        }
    }
}
