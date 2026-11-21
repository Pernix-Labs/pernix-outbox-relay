using Pernix.OutboxRelay.Benchmarks.Infrastructure;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;

namespace Pernix.OutboxRelay.Benchmarks.Scenarios;

internal static class KafkaOnlyScenario
{
    private const int Window = 500;

    public static Task RunAsync(HarnessOptions options) => CellsAsync(options, "kafka-only", "msg", deadLetter: null);

    public static Task DeadLetterAsync(HarnessOptions options) => CellsAsync(options, "kafka-dlq", "dlq-msg", Workload.DeadLetter);

    private static async Task CellsAsync(HarnessOptions options, string label, string unit, DeadLetterInfo? deadLetter)
    {
        foreach (var mode in options.Modes)
        {
            foreach (var headers in options.Headers)
            {
                var strict = mode == OrderingMode.Strict;
                var n = options.Count ?? (strict ? 20_000 : 200_000);
                var warm = options.Warm ?? (strict ? 5_000 : 50_000);
                await CellAsync(label, unit, deadLetter, mode, headers, strict ? 1 : Window, n, warm, options.Rounds);
            }
        }
    }

    private static async Task CellAsync(
        string label, string unit, DeadLetterInfo? deadLetter, OrderingMode mode, int headers, int window, long n, long warm, int rounds)
    {
        var name = $"{label} {mode} w{window} h{headers}";
        var sends = RoundUp(n, window);
        var messages = Workload.Messages(1, window, headers, deadLetter);
        var tasks = new ValueTask<DeliveryResult>[window];
        await using var destination = deadLetter is null ? KafkaMock.Create(mode) : KafkaMock.CreateDeadLetter(mode);
        await destination.ValidateAsync(CancellationToken.None);

        await RoundRunner.RunAsync(
            unit,
            () => SendAsync(destination, messages, tasks, RoundUp(warm, window)),
            rounds,
            async (round, reported) =>
            {
                // Suspend once first so the state machine box is allocated outside the window.
                await Task.Yield();
                var start = Snapshot.Take();
                for (long sent = 0; sent < sends; sent += window)
                {
                    for (var i = 0; i < window; i++)
                    {
                        tasks[i] = destination.SendAsync(messages[i], CancellationToken.None);
                    }

                    for (var i = 0; i < window; i++)
                    {
                        var result = await tasks[i];
                        tasks[i] = default;
                        Check(result);
                    }
                }

                var end = Snapshot.Take();
                return Measurement.Of(name, round, reported, sends, unit, start, end);
            });
    }

    private static async Task SendAsync(IDestination destination, OutgoingMessage[] messages, ValueTask<DeliveryResult>[] tasks, long count)
    {
        for (long sent = 0; sent < count; sent += messages.Length)
        {
            for (var i = 0; i < messages.Length; i++)
            {
                tasks[i] = destination.SendAsync(messages[i], CancellationToken.None);
            }

            for (var i = 0; i < messages.Length; i++)
            {
                var result = await tasks[i];
                tasks[i] = default;
                Check(result);
            }
        }
    }

    private static void Check(DeliveryResult result)
    {
        if (result is not DeliveryResult.Delivered)
        {
            throw new InvalidOperationException($"kafka-only send not delivered: {result}");
        }
    }

    private static long RoundUp(long count, int window) => (count + window - 1) / window * window;
}
