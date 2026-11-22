using System.Globalization;
using System.Threading.Tasks.Sources;
using Pernix.OutboxRelay.Core.Abstractions;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;

namespace Pernix.OutboxRelay.Destination.Kafka;

internal sealed class KafkaDestination : IDestination
{
    internal static readonly TimeSpan DescribeTimeout = TimeSpan.FromSeconds(15);
    internal static readonly TimeSpan DisposeTimeout = TimeSpan.FromSeconds(5);

    private const int DefaultTopicMaxMessageBytes = 1_048_588;
    private static readonly string RetiredError = $"{ErrorCode.Local_Fatal}: producer failed fatally before delivery report";
    private static readonly DeliveryResult NotDescribed =
        new DeliveryResult.DestinationFault($"{ErrorCode.Local_State}: topic not described, ValidateAsync has not run");
    private static readonly DeliveryResult CompactedNullKey =
        new DeliveryResult.MessageFault("InvalidRecord: null key on compacted topic", Permanent: true);

    private readonly string name;
    private readonly KafkaAddressMap addresses;
    private readonly string clusterName;
    private readonly KafkaClientSettings settings;
    private readonly bool probe;
    private readonly IReadOnlyList<string> deadLetterOf;
    private readonly Func<ProducerConfig, ILogger, Action<Error>, KafkaReportHandler, IKafkaRawProducer> producerFactory;
    private readonly ILogger logger;
    private readonly SemaphoreSlim producerLock = new(1, 1);

    private IKafkaRawProducer? producer;
    private IKafkaRawProducer? brokenProducer;
    private ProducerSlot? activeSlot;
    private int disposed;

    private readonly KafkaSendSlotPool<SendSlot> pool;
    private readonly KafkaReportHandler onReport;

    public KafkaDestination(
        string name,
        KafkaAddressMap addresses,
        string clusterName,
        KafkaClientSettings settings,
        bool probe,
        IReadOnlyList<string> deadLetterOf,
        Func<ProducerConfig, ILogger, Action<Error>, KafkaReportHandler, IKafkaRawProducer> producerFactory,
        ILogger logger)
    {
        this.name = name;
        this.addresses = addresses;
        this.clusterName = clusterName;
        this.settings = settings;
        this.probe = probe;
        this.deadLetterOf = deadLetterOf;
        this.producerFactory = producerFactory;
        this.logger = logger;
        pool = new KafkaSendSlotPool<SendSlot>(index => new SendSlot(this, index));
        onReport = OnReport;

        // The forced message.timeout.ms of the mode: 30 s for None, 0 (unbounded) for PerKey and Strict.
        var messageTimeoutMs = int.Parse(settings.Producer.Get("message.timeout.ms"), NumberStyles.None, CultureInfo.InvariantCulture);
        SendBound = messageTimeoutMs > 0 ? TimeSpan.FromMilliseconds(messageTimeoutMs) : null;
    }

    public TimeSpan? SendBound { get; }

    public IReadOnlySet<string>? Routes => addresses.Routes;

    // Pool proofs for tests only. Never logged or exported.
    internal int SlotsCreated => pool.Created;

    internal int FreeSlots => pool.IdleCount;

    public async Task ValidateAsync(CancellationToken ct)
    {
        var undescribed = Array.FindAll(addresses.Distinct, static a => Volatile.Read(ref a.Config) is null);
        if (undescribed.Length > 0)
        {
            await DescribeAsync(undescribed, ct).ConfigureAwait(false);
        }

        // Dead letters keep the original key, so compaction would discard all but the newest per key and refuse null keys.
        // A Routes destination cannot be a dead-letter destination (Core startup check), so only Topic destinations apply.
        if (deadLetterOf.Count > 0 && addresses.Single is { } single && Volatile.Read(ref single.Config) is { Compacted: true })
        {
            throw TopicFailure(single.Topic, $"is compacted and cannot hold dead letters of outbox '{string.Join("', '", deadLetterOf)}'");
        }

        // librdkafka rejects a bad property at construction and quotes its value, so only the sanitized text leaves.
        if (probe)
        {
            IKafkaRawProducer built;
            try
            {
                built = producerFactory(settings.Producer, logger, static _ => { }, static (_, _, _) => { });
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                throw InvalidConfiguration();
            }

            await DisposeClientAsync(built).ConfigureAwait(false);
        }
    }

    public ValueTask<DeliveryResult> SendAsync(OutgoingMessage message, CancellationToken ct)
    {
        if (message is null)
        {
            return ValueTask.FromException<DeliveryResult>(new ArgumentNullException(nameof(message)));
        }

        if (Volatile.Read(ref disposed) != 0)
        {
            return ValueTask.FromException<DeliveryResult>(new ObjectDisposedException(GetType().FullName));
        }

        if (addresses.Resolve(message) is not { } address)
        {
            return ValueTask.FromException<DeliveryResult>(new ArgumentException(ContractViolation(message), nameof(message)));
        }

        // Size and key checks need the described topic, so a send before ValidateAsync is a destination fault, not a throw.
        if (Volatile.Read(ref address.Config) is not { } topicConfig)
        {
            return new ValueTask<DeliveryResult>(NotDescribed);
        }

        var headers = message.DeadLetter is null ? message.Headers : KafkaDeadLetterHeaders.Merge(message);
        var size = KafkaRecordSize.UpperBound(message.Key, message.Payload, headers);
        var limit = KafkaRecordSize.Limit(topicConfig, settings.MessageMaxBytes);
        if (size > limit)
        {
            return new ValueTask<DeliveryResult>(
                new DeliveryResult.MessageFault($"MsgSizeTooLarge: record up to {size} bytes exceeds limit {limit}", Permanent: true));
        }

        if (topicConfig.Compacted && message.Key is null)
        {
            return new ValueTask<DeliveryResult>(CompactedNullKey);
        }

        return ActiveSlot() is { } active
            ? Produce(active, message, address, headers, ct)
            : AcquireSlotAndProduceAsync(message, address, headers, ct);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
        {
            return;
        }

        Volatile.Write(ref brokenProducer, null);
        if (Interlocked.Exchange(ref producer, null) is { } current)
        {
            await DisposeClientAsync(current).ConfigureAwait(false);
        }

        pool.Dispose();
    }

    private async ValueTask<DeliveryResult> AcquireSlotAndProduceAsync(OutgoingMessage message, KafkaAddress address, MessageHeaders headers, CancellationToken ct)
    {
        ProducerSlot slot;
        try
        {
            slot = await GetProducerAsync().ConfigureAwait(false);
        }
        catch (KafkaException ex)
        {
            return new DeliveryResult.DestinationFault($"{ex.Error.Code}: producer cannot be built");
        }
        catch (ArgumentException)
        {
            return new DeliveryResult.DestinationFault($"{ErrorCode.Local_InvalidArg}: producer cannot be built");
        }

        return await Produce(slot, message, address, headers, ct).ConfigureAwait(false);
    }

    private ValueTask<DeliveryResult> Produce(ProducerSlot slot, OutgoingMessage message, KafkaAddress address, MessageHeaders headers, CancellationToken ct)
    {
        var current = slot.Producer!;

        // Topic destinations keep the unprefixed fault text. The route is a slot field per send.
        var route = addresses.ByRoute is null ? null : message.Route;
        var send = pool.Rent();
        var token = send.Begin(slot, current, route, out var opaque);
        Error? refused;
        try
        {
            refused = current.Produce(address, message.Key, message.Payload, headers, opaque);
        }
        // A sync refusal or throw yields no delivery report and no cancel registration exists yet, so the slot returns now.
        catch (Exception ex)
        {
            send.Abandon();
            pool.Return(send);
            return ValueTask.FromException<DeliveryResult>(ex);
        }

        if (refused is not null)
        {
            send.Abandon();
            pool.Return(send);
            return new ValueTask<DeliveryResult>(ClassifyAndMarkFatal(current, refused, route));
        }

        // ct cancels the await only. The client still owns the message.
        slot.Track(send);
        send.CancelOn(ct);
        return new ValueTask<DeliveryResult>(send, token);
    }

    private DeliveryResult ClassifyAndMarkFatal(IKafkaRawProducer current, Error error, string? route)
    {
        if (error.IsFatal || error.Code == ErrorCode.Local_Fatal)
        {
            MarkBroken(current, error.Code.ToString());
        }

        var result = KafkaErrorClassifier.Classify(error);
        return route is not null && result is DeliveryResult.DestinationFault fault ? RouteFault(route, fault.Error) : result;
    }

    // Fault path only. The topic is looked up at fault time: the route resolved in the same frozen map at send start.
    private DeliveryResult.DestinationFault RouteFault(string route, string error) =>
        new($"route '{route}' topic '{addresses.ByRoute![route].Topic}': {error}");

    private void OnClientFatal(ProducerSlot slot, Error error)
    {
        var code = error.Code.ToString();
        Interlocked.CompareExchange(ref slot.FatalCode, code, null);

        // A fatal producer may never report its in-flight sends, so they end now rather than at the next rebuild.
        slot.Retire();
        if (Volatile.Read(ref slot.Producer) is { } built)
        {
            MarkBroken(built, code);
        }
    }

    private void MarkBroken(IKafkaRawProducer current, string code)
    {
        if (ReferenceEquals(Volatile.Read(ref producer), current)
            && Interlocked.CompareExchange(ref brokenProducer, current, null) is null)
        {
            logger.KafkaProducerFatal(name, code);
        }
    }

    private void OnReport(nint opaque, Error? error, PersistenceStatus status) =>
        pool.Find(opaque, out var generation)?.OnReport(generation, error, status);

    private ProducerSlot? ActiveSlot()
    {
        var active = Volatile.Read(ref activeSlot);
        var current = Volatile.Read(ref producer);
        return active is not null && current is not null && ReferenceEquals(active.Producer, current)
            && !ReferenceEquals(current, Volatile.Read(ref brokenProducer))
                ? active
                : null;
    }

    private async Task<ProducerSlot> GetProducerAsync()
    {
        if (ActiveSlot() is { } active)
        {
            return active;
        }

        await producerLock.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed) != 0, this);

            var current = Volatile.Read(ref producer);
            var rebuild = current is not null && ReferenceEquals(current, Volatile.Read(ref brokenProducer));
            if (current is not null && !rebuild)
            {
                return activeSlot!;
            }

            if (rebuild && Interlocked.CompareExchange(ref producer, null, current) == current)
            {
                await DisposeClientAsync(current!).ConfigureAwait(false);

                // Delivery reports of a disposed producer may never be served, so its outstanding sends end here.
                activeSlot!.Retire();
            }

            // The error handler can fire during Build, before the reference exists, so the slot carries the fatal over.
            var slot = new ProducerSlot();
            var built = producerFactory(settings.Producer, logger, error => OnClientFatal(slot, error), onReport);
            Volatile.Write(ref slot.Producer, built);
            Volatile.Write(ref activeSlot, slot);
            Volatile.Write(ref brokenProducer, null);
            Volatile.Write(ref producer, built);
            if (Volatile.Read(ref slot.FatalCode) is { } fatalCode)
            {
                MarkBroken(built, fatalCode);
            }

            // DisposeAsync ran while building: it never saw this producer.
            if (Volatile.Read(ref disposed) != 0)
            {
                if (Interlocked.Exchange(ref producer, null) is { } orphan)
                {
                    await DisposeClientAsync(orphan).ConfigureAwait(false);
                }

                throw new ObjectDisposedException(GetType().FullName);
            }

            if (rebuild)
            {
                logger.KafkaProducerRebuilt(name);
            }

            return slot;
        }
        finally
        {
            producerLock.Release();
        }
    }

    private async Task DescribeAsync(KafkaAddress[] undescribed, CancellationToken ct)
    {
        IAdminClient admin;
        try
        {
            admin = new AdminClientBuilder(settings.Admin)
                .SetLogHandler((_, message) => KafkaDestinationFactory.OnClientLog(logger, message))
                .SetErrorHandler((client, error) => KafkaDestinationFactory.OnClientError(logger, client.Name, error))
                .Build();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw InvalidConfiguration();
        }

        var reasons = new string?[undescribed.Length];
        try
        {
            var resources = Array.ConvertAll(undescribed, static a => new ConfigResource { Type = ResourceType.Topic, Name = a.Topic });
            var results = await admin
                .DescribeConfigsAsync(resources, new DescribeConfigsOptions { RequestTimeout = DescribeTimeout })
                .WaitAsync(DescribeTimeout, ct)
                .ConfigureAwait(false);
            foreach (var result in results)
            {
                Store(undescribed, result);
            }

            for (var i = 0; i < undescribed.Length; i++)
            {
                if (Volatile.Read(ref undescribed[i].Config) is null)
                {
                    reasons[i] = "cannot be described (no result)";
                }
            }
        }
        catch (DescribeConfigsException ex)
        {
            foreach (var report in ex.Results)
            {
                if (report.Error is not { IsError: true } error)
                {
                    Store(undescribed, report);
                }
                else if (IndexOf(undescribed, report.ConfigResource.Name) is var i and >= 0)
                {
                    reasons[i] = error.Code == ErrorCode.UnknownTopicOrPart ? "does not exist" : $"cannot be described ({error.Code})";
                }
            }

            for (var i = 0; i < undescribed.Length; i++)
            {
                if (reasons[i] is null && Volatile.Read(ref undescribed[i].Config) is null)
                {
                    reasons[i] = $"cannot be described ({ex.Error.Code})";
                }
            }
        }
        catch (KafkaException ex)
        {
            Array.Fill(reasons, $"cannot be described ({ex.Error.Code})");
        }
        catch (TimeoutException)
        {
            Array.Fill(reasons, "describe timed out");
        }
        finally
        {
            await DisposeClientAsync(admin).ConfigureAwait(false);
        }

        var texts = new List<string>();
        for (var i = 0; i < undescribed.Length; i++)
        {
            if (reasons[i] is { } reason)
            {
                texts.Add(TopicFailureText(undescribed[i].Topic, reason));
            }
        }

        if (texts.Count > 0)
        {
            throw new DestinationValidationException(string.Join("; ", texts));
        }
    }

    private static void Store(KafkaAddress[] undescribed, DescribeConfigsReport report)
    {
        if (IndexOf(undescribed, report.ConfigResource.Name) is var i and >= 0)
        {
            Volatile.Write(ref undescribed[i].Config, ConfigOf(report.Entries));
        }
    }

    private static void Store(KafkaAddress[] undescribed, DescribeConfigsResult result)
    {
        if (IndexOf(undescribed, result.ConfigResource.Name) is var i and >= 0)
        {
            Volatile.Write(ref undescribed[i].Config, ConfigOf(result.Entries));
        }
    }

    private static int IndexOf(KafkaAddress[] undescribed, string topic) =>
        Array.FindIndex(undescribed, a => string.Equals(a.Topic, topic, StringComparison.Ordinal));

    private static TopicConfig ConfigOf(Dictionary<string, ConfigEntryResult> entries)
    {
        var maxMessageBytes = entries.TryGetValue("max.message.bytes", out var max)
            && int.TryParse(max.Value, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed)
                ? parsed
                : DefaultTopicMaxMessageBytes;
        var compacted = entries.TryGetValue("cleanup.policy", out var policy)
            && (policy.Value ?? string.Empty).Split(',', StringSplitOptions.TrimEntries).Contains("compact", StringComparer.OrdinalIgnoreCase);
        return new TopicConfig(maxMessageBytes, compacted);
    }

    private static async Task DisposeClientAsync(IDisposable client)
    {
        var dispose = Task.Run(client.Dispose);
        try
        {
            await dispose.WaitAsync(DisposeTimeout).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            _ = dispose.ContinueWith(static t => _ = t.Exception, CancellationToken.None, TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        }
    }

    private DestinationValidationException TopicFailure(string topic, string reason) => new(TopicFailureText(topic, reason));

    private string TopicFailureText(string topic, string reason) => $"Destinations[{name}]: topic '{topic}' {reason}";

    // Failure branch only. Unreachable under the IDestination contract: Core sends only accepted rows.
    private string ContractViolation(OutgoingMessage message)
    {
        if (addresses.ByRoute is not null && message.DeadLetter is not null)
        {
            return $"Destinations[{name}]: dead letter on a Routes destination";
        }

        var shown = message.Route is { } route ? $"'{route}'" : "(none)";
        return $"Destinations[{name}]: route {shown} is not accepted by this destination";
    }

    private DestinationValidationException InvalidConfiguration() =>
        new($"Destinations[{name}]: invalid producer configuration for cluster '{clusterName}'");

    private sealed class ProducerSlot
    {
        private readonly HashSet<SendSlot> inFlight = [];
        private bool retired;

        public IKafkaRawProducer? Producer;
        public string? FatalCode;

        public void Track(SendSlot send)
        {
            lock (inFlight)
            {
                if (retired)
                {
                    send.Retire();
                }
                else if (!send.Ended)
                {
                    inFlight.Add(send);
                }
            }
        }

        public void Untrack(SendSlot send)
        {
            lock (inFlight)
            {
                inFlight.Remove(send);
            }
        }

        public void Retire()
        {
            lock (inFlight)
            {
                retired = true;
                var ending = new SendSlot[inFlight.Count];
                inFlight.CopyTo(ending);
                inFlight.Clear();
                foreach (var send in ending)
                {
                    send.Retire();
                }
            }
        }
    }

    // A pooled send of this destination. Continuations run synchronously on the completing thread: for a delivery report,
    // the librdkafka delivery-report thread in the raw report handler. Rule: a continuation on a Kafka send never blocks. Core's continuation
    // (CompleteMain) does metrics, a fault log on a non-cancelled fault and TryWrite. The log is unreachable here: a pending
    // send faults only on cancel (a sync Produce fault is returned completed and consumed inline on the loop thread), and a
    // cancelled term returns before the log.
    private sealed class SendSlot : IValueTaskSource<DeliveryResult>
    {
        private readonly KafkaDestination owner;
        private ManualResetValueTaskSourceCore<DeliveryResult> core;

        private long active;
        private long generation;
        private CancelTarget cancelTarget;
        private ProducerSlot? producerSlot;
        private IKafkaRawProducer? producer;
        private string? route;
        private CancellationTokenRegistration cancellation;

        public SendSlot(KafkaDestination owner, int index)
        {
            this.owner = owner;
            Index = index;
            core.RunContinuationsAsynchronously = false;
            cancelTarget = new CancelTarget(this);
        }

        public int Index { get; }

        public bool Ended => Volatile.Read(ref active) == 0;

        public short Begin(ProducerSlot slot, IKafkaRawProducer current, string? sentRoute, out nint opaque)
        {
            producerSlot = slot;
            producer = current;
            route = sentRoute;
            generation = KafkaSendSlotPool<SendSlot>.NextGeneration(generation);
            opaque = KafkaSendSlotPool<SendSlot>.Handle(Index, generation);
            Volatile.Write(ref active, generation);
            return core.Version;
        }

        public void CancelOn(CancellationToken ct)
        {
            if (ct.CanBeCanceled)
            {
                cancelTarget.Generation = generation;
                cancellation = ct.UnsafeRegister(static (state, token) => ((CancelTarget)state!).Cancel(token), cancelTarget);
            }
        }

        public void Abandon()
        {
            Volatile.Write(ref active, 0);
            producerSlot = null;
            producer = null;
            route = null;
        }

        public void OnReport(long reported, Error? error, PersistenceStatus status)
        {
            if (!TryEnd(reported))
            {
                return;
            }

            producerSlot!.Untrack(this);

            core.SetResult(error is not null
                ? owner.ClassifyAndMarkFatal(producer!, error, route)
                : status == PersistenceStatus.Persisted
                    ? DeliveryResult.Delivered.Instance
                    : NotConfirmed(status));
        }

        public void Cancel(long cancelled, CancellationToken token)
        {
            if (!TryEnd(cancelled))
            {
                return;
            }

            producerSlot!.Untrack(this);
            core.SetException(new TaskCanceledException(null, null, token));
        }

        public void Retire()
        {
            if (Volatile.Read(ref active) is var live and not 0 && TryEnd(live))
            {
                core.SetResult(new DeliveryResult.DestinationFault(RetiredError));
            }
        }

        public DeliveryResult GetResult(short token)
        {
            if (core.GetStatus(token) == ValueTaskSourceStatus.Pending)
            {
                throw new InvalidOperationException("The send has not completed.");
            }

            try
            {
                return core.GetResult(token);
            }
            finally
            {
                Release();
            }
        }

        public ValueTaskSourceStatus GetStatus(short token) => core.GetStatus(token);

        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
            core.OnCompleted(continuation, state, token, flags);

        private void Release()
        {
            if (cancellation.Token.CanBeCanceled && !cancellation.Unregister())
            {
                cancelTarget = new CancelTarget(this);
            }

            cancellation = default;
            producerSlot = null;
            producer = null;
            route = null;
            core.Reset();
            owner.pool.Return(this);
        }

        private bool TryEnd(long ending) => ending != 0 && Interlocked.CompareExchange(ref active, 0, ending) == ending;

        private DeliveryResult NotConfirmed(PersistenceStatus status)
        {
            var error = $"{status}: not confirmed";
            return route is { } sentRoute ? owner.RouteFault(sentRoute, error) : new DeliveryResult.DestinationFault(error);
        }
    }

    private sealed class CancelTarget(SendSlot slot)
    {
        public long Generation;

        public void Cancel(CancellationToken token) => slot.Cancel(Generation, token);
    }
}
