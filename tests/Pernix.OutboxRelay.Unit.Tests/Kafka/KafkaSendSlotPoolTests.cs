using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Destination.Kafka;
using Confluent.Kafka;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Kafka;

public sealed class KafkaSendSlotPoolTests
{
    private static readonly DeliveryResult Retired = new DeliveryResult.DestinationFault("Local_Fatal: producer failed fatally before delivery report");

    [Fact]
    public void Rent_creates_at_index_Created_and_Return_reuses()
    {
        using var pool = new KafkaSendSlotPool<Probe>(index => new Probe(index));

        var first = pool.Rent();
        var second = pool.Rent();
        Assert.Equal(0, first.Index);
        Assert.Equal(1, second.Index);
        Assert.Equal(2, pool.Created);
        Assert.Equal(0, pool.IdleCount);

        pool.Return(first);
        Assert.Equal(1, pool.IdleCount);
        Assert.Same(first, pool.Rent());
        Assert.Equal(2, pool.Created);
        Assert.Equal(0, pool.IdleCount);
    }

    [Fact]
    public void Return_after_dispose_is_not_pooled_and_idle_set_is_kept()
    {
        var pool = new KafkaSendSlotPool<Probe>(index => new Probe(index));
        var first = pool.Rent();
        var second = pool.Rent();
        pool.Return(first);

        pool.Dispose();
        Assert.Equal(1, pool.IdleCount);

        pool.Return(second);
        Assert.Equal(1, pool.IdleCount);
        Assert.Equal(2, pool.Created);
    }

    [Fact]
    public void Rent_and_Return_allocate_nothing_after_warm_up()
    {
        using var pool = new KafkaSendSlotPool<Probe>(index => new Probe(index));
        var slots = new Probe[4];
        Cycle(pool, slots);

        var start = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 100; i++)
        {
            Cycle(pool, slots);
        }

        Assert.Equal(0, GC.GetAllocatedBytesForCurrentThread() - start);
        Assert.Equal(4, pool.Created);
    }

    [Fact]
    public void Opaque_round_trips_index_and_generation()
    {
        using var pool = new KafkaSendSlotPool<Probe>(index => new Probe(index));
        var slots = new[] { pool.Rent(), pool.Rent(), pool.Rent(), pool.Rent(), pool.Rent() };

        foreach (var (index, generation) in new[] { (0, 1L), (4, 7L), (2, KafkaSendSlotPool<Probe>.MaxGeneration) })
        {
            Assert.Same(slots[index], pool.Find(KafkaSendSlotPool<Probe>.Handle(index, generation), out var found));
            Assert.Equal(generation, found);
        }

        Assert.Null(pool.Find(KafkaSendSlotPool<Probe>.Handle(KafkaSendSlotPool<Probe>.MaxSlots - 1, 1), out _));
    }

    [Fact]
    public void Generation_wraps_to_one()
    {
        Assert.Equal(1, KafkaSendSlotPool<Probe>.NextGeneration(0));
        Assert.Equal(2, KafkaSendSlotPool<Probe>.NextGeneration(1));
        Assert.Equal(1, KafkaSendSlotPool<Probe>.NextGeneration(KafkaSendSlotPool<Probe>.MaxGeneration));
    }

    [Theory]
    [InlineData(nameof(ErrorCode.Local_QueueFull))]
    [InlineData(nameof(ErrorCode.Local_UnknownTopic))]
    [InlineData(nameof(InvalidOperationException))]
    public async Task Sync_refusal_returns_slot_before_result(string thrown)
    {
        var fake = new FakeProducer();
        var harness = new KafkaHarness(_ => fake);
        await using var destination = harness.Create();
        await SendAndPersistAsync(destination, fake, KafkaHarness.Message([1], [1]));
        Assert.Equal(1, destination.FreeSlots);

        DeliveryResult? expected = null;
        Exception? expectedException = null;
        switch (thrown)
        {
            case nameof(ErrorCode.Local_QueueFull):
                fake.RefuseNext(new Error(ErrorCode.Local_QueueFull, "Local: Queue full"), 1);
                expected = new DeliveryResult.DestinationFault("Local_QueueFull: Local: Queue full");
                break;
            case nameof(ErrorCode.Local_UnknownTopic):
                fake.RefuseNext(new Error(ErrorCode.Local_UnknownTopic, "Local: Unknown topic"), 1);
                expected = new DeliveryResult.DestinationFault("Local_UnknownTopic: Local: Unknown topic");
                break;
            default:
                expectedException = new InvalidOperationException("refused");
                fake.ThrowNext(expectedException);
                break;
        }

        using var cts = new CancellationTokenSource();
        var refused = destination.SendAsync(KafkaHarness.Message([2], [2]), cts.Token);

        Assert.True(refused.IsCompleted);
        Assert.Equal(1, destination.FreeSlots);

        // Not slot-backed: a second read gives the same outcome instead of the consumed-token throw.
        if (expectedException is not null)
        {
            Assert.Same(expectedException, await Assert.ThrowsAsync<InvalidOperationException>(async () => await refused));
            Assert.Same(expectedException, await Assert.ThrowsAsync<InvalidOperationException>(async () => await refused));
        }
        else
        {
            Assert.Equal(expected, await refused);
            Assert.Equal(expected, await refused);
        }

        Assert.Single(fake.Sends);

        var next = destination.SendAsync(KafkaHarness.Message([3], [3]), CancellationToken.None);
        Assert.Equal(0, destination.FreeSlots);

        await cts.CancelAsync();
        Assert.False(next.IsCompleted);

        fake.Sends[^1].Persist();
        Assert.IsType<DeliveryResult.Delivered>(await next);
        Assert.Equal(1, destination.SlotsCreated);
        Assert.Equal(1, destination.FreeSlots);
    }

    [Fact]
    public async Task Local_QueueFull_burst_keeps_pool_flat()
    {
        const int refusals = 1_000;
        const int inFlight = 500;
        var harness = new KafkaHarness();
        await using var destination = harness.Create();
        var producer = await BuildProducerAsync(destination, harness);
        var queueFull = new DeliveryResult.DestinationFault("Local_QueueFull: Local: Queue full");

        producer.RefuseNext(new Error(ErrorCode.Local_QueueFull, "Local: Queue full"), refusals);
        for (var i = 0; i < refusals; i++)
        {
            Assert.Equal(queueFull, await destination.SendAsync(KafkaHarness.Message([1], [1]), CancellationToken.None));
        }

        Assert.Equal(1, destination.SlotsCreated);

        var sends = new ValueTask<DeliveryResult>[inFlight];
        for (var i = 0; i < inFlight; i++)
        {
            sends[i] = destination.SendAsync(KafkaHarness.Message([1], [1]), CancellationToken.None);
        }

        var pending = producer.Sends;
        Assert.Equal(inFlight + 1, pending.Count);
        for (var i = 1; i < pending.Count; i++)
        {
            pending[i].Persist();
        }

        foreach (var send in sends)
        {
            Assert.IsType<DeliveryResult.Delivered>(await send);
        }

        Assert.InRange(destination.SlotsCreated, 1, inFlight + 1);
        Assert.Equal(destination.SlotsCreated, destination.FreeSlots);
    }

    [Fact]
    public async Task Stale_report_after_cancel_does_not_complete_reused_slot()
    {
        var harness = new KafkaHarness();
        await using var destination = harness.Create();
        var producer = await BuildProducerAsync(destination, harness);
        using var cts = new CancellationTokenSource();

        var cancelled = destination.SendAsync(KafkaHarness.Message([2], [2]), cts.Token);
        var stale = producer.Sends[^1];
        await cts.CancelAsync();
        await Assert.ThrowsAsync<TaskCanceledException>(async () => await cancelled);

        var next = destination.SendAsync(KafkaHarness.Message([3], [3]), CancellationToken.None);
        Assert.Equal(1, destination.SlotsCreated);
        stale.Persist();
        Assert.False(next.IsCompleted);

        producer.Sends[^1].Persist();
        Assert.IsType<DeliveryResult.Delivered>(await next);
        Assert.Equal(1, destination.FreeSlots);
    }

    [Fact]
    public async Task Stale_report_after_retire_does_not_complete_reused_slot()
    {
        var harness = new KafkaHarness();
        await using var destination = harness.Create();
        var old = await BuildProducerAsync(destination, harness);

        var retired = destination.SendAsync(KafkaHarness.Message([2], [2]), CancellationToken.None);
        var stale = old.Sends[^1];
        Assert.Single(harness.FatalCallbacks)(new Error(ErrorCode.Local_Fatal, "fatal idempotence error", isFatal: true));
        Assert.Equal(Retired, await retired);

        var next = destination.SendAsync(KafkaHarness.Message([3], [3]), CancellationToken.None);
        await KafkaHarness.WaitUntilAsync(() => harness.Producers.Count == 2 && harness.Producers[1].Sends.Count == 1);
        Assert.Equal(1, destination.SlotsCreated);
        stale.Persist();
        Assert.False(next.IsCompleted);

        harness.Producers[1].Sends[0].Persist();
        Assert.IsType<DeliveryResult.Delivered>(await next);
        Assert.Equal(1, destination.FreeSlots);
    }

    [Fact]
    public async Task Stale_report_after_dispose_does_not_complete_reused_slot()
    {
        var harness = new KafkaHarness();
        await using var destination = harness.Create();
        var old = await BuildProducerAsync(destination, harness);

        // A fatal report marks the producer broken. The next send disposes it and retires its outstanding send.
        var outstanding = destination.SendAsync(KafkaHarness.Message([2], [2]), CancellationToken.None);
        var stale = old.Sends[^1];
        var fatal = destination.SendAsync(KafkaHarness.Message([3], [3]), CancellationToken.None);
        old.Sends[^1].Fail(new Error(ErrorCode.Local_Fatal, "fatal idempotence error", isFatal: true));
        Assert.IsType<DeliveryResult.DestinationFault>(await fatal);

        var rebuilt = destination.SendAsync(KafkaHarness.Message([4], [4]), CancellationToken.None);
        await KafkaHarness.WaitUntilAsync(() => harness.Producers.Count == 2 && harness.Producers[1].Sends.Count == 1);
        Assert.Equal(1, old.DisposeCalls);
        Assert.Equal(Retired, await outstanding);

        var next = destination.SendAsync(KafkaHarness.Message([5], [5]), CancellationToken.None);
        Assert.Equal(2, destination.SlotsCreated);
        stale.Persist();
        Assert.False(next.IsCompleted);

        harness.Producers[1].Sends[0].Persist();
        harness.Producers[1].Sends[1].Persist();
        Assert.IsType<DeliveryResult.Delivered>(await rebuilt);
        Assert.IsType<DeliveryResult.Delivered>(await next);
        Assert.Equal(2, destination.FreeSlots);
    }

    private static void Cycle(KafkaSendSlotPool<Probe> pool, Probe[] slots)
    {
        for (var i = 0; i < slots.Length; i++)
        {
            slots[i] = pool.Rent();
        }

        for (var i = 0; i < slots.Length; i++)
        {
            pool.Return(slots[i]);
        }
    }

    private static async Task<FakeProducer> BuildProducerAsync(KafkaDestination destination, KafkaHarness harness)
    {
        var send = destination.SendAsync(KafkaHarness.Message([1], [1]), CancellationToken.None);
        var producer = Assert.Single(harness.Producers);
        Assert.Single(producer.Sends).Persist();
        Assert.IsType<DeliveryResult.Delivered>(await send);
        return producer;
    }

    private static async Task SendAndPersistAsync(KafkaDestination destination, FakeProducer producer, OutgoingMessage message)
    {
        var send = destination.SendAsync(message, CancellationToken.None);
        producer.Sends[^1].Persist();
        Assert.IsType<DeliveryResult.Delivered>(await send);
    }

    private sealed class Probe(int index)
    {
        public int Index { get; } = index;
    }
}
