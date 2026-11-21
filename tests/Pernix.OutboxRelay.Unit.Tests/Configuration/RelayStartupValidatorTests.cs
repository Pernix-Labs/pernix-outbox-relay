using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Core.Leadership;
using Pernix.OutboxRelay.Unit.Tests.Leadership;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Configuration;

public sealed class RelayStartupValidatorTests
{
    private const string SentinelConnectionString = "Host=sentinel-db;Database=orders;Username=relay;Password=sentinel-secret-7f3a";

    private readonly Dictionary<string, string?> settings = new()
    {
        ["ConnectionStrings:OrdersDb"] = SentinelConnectionString,
        ["KafkaClusters:Main:BootstrapServers"] = "kafka-1:9092",
        ["Destinations:0:Name"] = "OrdersTopic",
        ["Destinations:0:Transport"] = "Kafka",
        ["Destinations:0:Kafka:Cluster"] = "Main",
        ["Destinations:0:Kafka:Topic"] = "orders.events",
        ["Destinations:1:Name"] = "PaymentsTopic",
        ["Destinations:1:Transport"] = "Kafka",
        ["Destinations:1:Kafka:Cluster"] = "Main",
        ["Destinations:1:Kafka:Topic"] = "payments.events",
        ["Outboxes:0:Name"] = "Orders",
        ["Outboxes:0:Destination"] = "OrdersTopic",
        ["Outboxes:0:Database:Provider"] = "PostgreSql",
        ["Outboxes:0:Database:Connection"] = "OrdersDb",
        ["Outboxes:0:Database:Table"] = "public.outbox",
        ["Outboxes:0:Ordering:Mode"] = "None",
        ["Outboxes:1:Name"] = "Payments",
        ["Outboxes:1:Destination"] = "paymentstopic",
        ["Outboxes:1:Database:Provider"] = "PostgreSql",
        ["Outboxes:1:Database:Connection"] = "OrdersDb",
        ["Outboxes:1:Database:Table"] = "payments.outbox",
        ["Outboxes:1:Ordering:Mode"] = "None",
    };

    private readonly FakeOutboxStoreProvider provider = new();
    private readonly FakeDestinationFactory factory = new();
    private readonly ValidatedOutboxes validatedOutboxes = new();
    private readonly RecordingLogger<RelayStartupValidator> logger = new();
    private readonly HostOptions hostOptions = new() { ShutdownTimeout = TimeSpan.FromSeconds(28) };

    [Fact]
    public async Task Shutdown_timeout_below_the_stop_budget_fails_start_before_store_validation()
    {
        hostOptions.ShutdownTimeout = TimeSpan.FromSeconds(28) - TimeSpan.FromTicks(1);

        var ex = await Assert.ThrowsAsync<RelayStartupValidationException>(StartingAsync);

        Assert.Equal(["HostOptions.ShutdownTimeout: must be at least 00:00:28 to cover the 27 s stop budget"], ex.Failures);
        Assert.Empty(provider.Stores);
        Assert.Empty(factory.Created);
    }

    [Fact]
    public async Task Config_failures_carry_the_section_prefix_and_the_host_options_line_does_not()
    {
        settings.Remove("Outboxes:0:Destination");
        hostOptions.ShutdownTimeout = TimeSpan.FromSeconds(1);

        var ex = await Assert.ThrowsAsync<RelayStartupValidationException>(StartingAsync);

        Assert.Equal(
            [
                "OutboxRelay.Outboxes[Orders].Destination: required",
                "HostOptions.ShutdownTimeout: must be at least 00:00:28 to cover the 27 s stop budget",
            ],
            ex.Failures);
    }

    [Fact]
    public async Task Root_connection_string_outside_the_section_does_not_satisfy_lookup()
    {
        var root = new ConfigurationBuilder()
            .AddInMemoryCollection(settings.Select(kv => new KeyValuePair<string, string?>(
                kv.Key.StartsWith("ConnectionStrings:", StringComparison.Ordinal) ? kv.Key : RelayConfiguration.SectionName + ":" + kv.Key,
                kv.Value)))
            .Build();
        var validator = new RelayStartupValidator(
            new RelayConfiguration(root.GetSection(RelayConfiguration.SectionName)), [provider], [factory], validatedOutboxes, Options.Create(hostOptions), logger);

        var ex = await Assert.ThrowsAsync<RelayStartupValidationException>(() => validator.StartingAsync(TestContext.Current.CancellationToken));

        Assert.Equal(
            [
                "OutboxRelay.Outboxes[Orders].Database.Connection: connection string 'OrdersDb' is not defined or empty in OutboxRelay.ConnectionStrings",
                "OutboxRelay.Outboxes[Payments].Database.Connection: connection string 'OrdersDb' is not defined or empty in OutboxRelay.ConnectionStrings",
            ],
            ex.Failures);
        Assert.Empty(provider.Stores);
    }

    [Fact]
    public async Task Shutdown_timeout_of_28_seconds_passes()
    {
        await StartingAsync();

        Assert.Equal(2, validatedOutboxes.Entries.Count);
    }

    [Fact]
    public async Task Config_faults_throw_with_every_failure_and_skip_store_validation()
    {
        settings.Remove("Outboxes:0:Destination");
        settings["Outboxes:1:Ordering:Mode"] = null;

        var ex = await Assert.ThrowsAsync<RelayStartupValidationException>(StartingAsync);

        Assert.Contains("OutboxRelay.Outboxes[Orders].Destination: required", ex.Failures);
        Assert.Contains(ex.Failures, f => f.StartsWith("OutboxRelay.Outboxes[Payments].Ordering.Mode: required", StringComparison.Ordinal));
        Assert.Empty(provider.Stores);
        Assert.Empty(factory.Created);
        Assert.Empty(validatedOutboxes.Entries);
    }

    [Fact]
    public async Task Refusals_and_transport_failures_are_one_config_failure()
    {
        settings["Outboxes:0:Ordering:Mode"] = "Strict";
        settings["Destinations:1:Transport"] = "Database";
        factory.ValidateFailures["OrdersTopic"] = ["Destinations[OrdersTopic].Kafka.Topic: required when Transport is Kafka"];

        var ex = await Assert.ThrowsAsync<RelayStartupValidationException>(StartingAsync);

        Assert.Equal(
            [
                "OutboxRelay.Destinations[OrdersTopic].Kafka.Topic: required when Transport is Kafka",
                "OutboxRelay.Destinations[PaymentsTopic].Transport: transport 'Database' is not implemented in this build",
            ],
            ex.Failures);
        Assert.Empty(provider.Stores);
        Assert.Empty(factory.Created);
    }

    [Fact]
    public async Task Store_failures_across_outboxes_throw_together()
    {
        provider.Configure = (name, store) => store.ValidateFailure = name == "Orders"
            ? new OutboxValidationException("Outboxes[Orders]: table 'public.outbox' not found")
            : new OutboxDatabaseUnreachableException("Outboxes[Payments]: database unreachable (timeout)");

        var ex = await Assert.ThrowsAsync<RelayStartupValidationException>(StartingAsync);

        Assert.Equal(
            ["OutboxRelay.Outboxes[Orders]: table 'public.outbox' not found", "OutboxRelay.Outboxes[Payments]: database unreachable (timeout)"],
            ex.Failures);
        Assert.Empty(factory.Created);
        Assert.Empty(validatedOutboxes.Entries);
    }

    [Fact]
    public async Task Create_store_failure_is_collected_with_other_store_failures()
    {
        provider.CreateFailures["Orders"] = new OutboxValidationException("Outboxes[Orders].Database.Connection: connection string is not valid");
        provider.Configure = (_, store) => store.ValidateFailure = new OutboxDatabaseUnreachableException("Outboxes[Payments]: database unreachable (timeout)");

        var ex = await Assert.ThrowsAsync<RelayStartupValidationException>(StartingAsync);

        Assert.Equal(
            ["OutboxRelay.Outboxes[Orders].Database.Connection: connection string is not valid", "OutboxRelay.Outboxes[Payments]: database unreachable (timeout)"],
            ex.Failures);
        Assert.Empty(validatedOutboxes.Entries);
    }

    [Fact]
    public async Task Unexpected_store_exception_escapes()
    {
        provider.Configure = (_, store) => store.ValidateFailure = new InvalidOperationException("bug");

        await Assert.ThrowsAsync<InvalidOperationException>(StartingAsync);
    }

    [Fact]
    public async Task No_outboxes_logs_warning_and_leaves_catalog_empty()
    {
        foreach (var key in settings.Keys.Where(k => k.StartsWith("Outboxes:", StringComparison.Ordinal)).ToList())
        {
            settings.Remove(key);
        }

        await StartingAsync();

        Assert.Empty(validatedOutboxes.Entries);
        var entry = Assert.Single(logger.WithEvent(100));
        Assert.Equal(Microsoft.Extensions.Logging.LogLevel.Warning, entry.Level);
    }

    [Fact]
    public async Task No_outboxes_still_probes_and_disposes_every_destination()
    {
        RemoveOutboxesAndSecondDestination();

        await StartingAsync();

        var created = Assert.Single(factory.Created);
        Assert.Equal("OrdersTopic", created.Destination);
        Assert.Same(DestinationContext.Probe, created.Context);
        Assert.Equal(["OrdersTopic"], factory.Disposed);
        Assert.Empty(validatedOutboxes.Entries);
    }

    [Fact]
    public async Task No_outboxes_with_failing_probe_throws()
    {
        RemoveOutboxesAndSecondDestination();
        factory.Probes["OrdersTopic"] = _ => throw new DestinationValidationException("Destinations[OrdersTopic]: topic 'orders.events' does not exist");

        var ex = await Assert.ThrowsAsync<RelayStartupValidationException>(StartingAsync);

        Assert.Equal(["OutboxRelay.Destinations[OrdersTopic]: topic 'orders.events' does not exist"], ex.Failures);
        Assert.Equal(["OrdersTopic"], factory.Disposed);
    }

    [Fact]
    public async Task Valid_config_validates_every_store_and_fills_catalog_in_config_order()
    {
        await StartingAsync();

        Assert.Equal(["Orders", "Payments"], validatedOutboxes.Entries.Select(e => e.Options.Name));
        Assert.Equal(["OrdersTopic", "PaymentsTopic"], validatedOutboxes.Entries.Select(e => e.Destination.Name));
        Assert.All(validatedOutboxes.Entries, e => Assert.Same(factory, e.DestinationFactory));
        Assert.Same(provider.Stores["Orders"], validatedOutboxes.Entries[0].Store);
        Assert.Same(provider.Stores["Payments"], validatedOutboxes.Entries[1].Store);
        Assert.All(provider.Stores.Values, s => Assert.Equal(1, s.ValidateCalls));
        Assert.Empty(logger.Entries);
    }

    [Fact]
    public async Task Configured_dead_letter_destination_is_bound_in_the_catalog()
    {
        settings["Outboxes:1:DeadLetter:Destination"] = "orderstopic";

        await StartingAsync();

        Assert.Null(validatedOutboxes.Entries[0].DeadLetterDestination);
        var deadLetter = validatedOutboxes.Entries[1].DeadLetterDestination;
        Assert.NotNull(deadLetter);
        Assert.Equal("OrdersTopic", deadLetter.Options.Name);
        Assert.Same(factory, deadLetter.Factory);
    }

    [Theory]
    [InlineData("")]
    [InlineData(null)]
    public async Task Empty_or_absent_dead_letter_destination_leaves_binding_null(string? value)
    {
        settings["Outboxes:0:DeadLetter:Destination"] = value;

        await StartingAsync();

        Assert.All(validatedOutboxes.Entries, e => Assert.Null(e.DeadLetterDestination));
    }

    [Fact]
    public async Task Catalog_holds_the_handle_returned_by_validation()
    {
        var handles = new Dictionary<string, FakeOutboxStore>();
        provider.Configure = (name, store) => store.ValidatedHandle = handles[name] = new FakeOutboxStore();

        await StartingAsync();

        Assert.Same(handles["Orders"], validatedOutboxes.Entries[0].Store);
        Assert.Same(handles["Payments"], validatedOutboxes.Entries[1].Store);
    }

    [Fact]
    public async Task Provider_name_resolves_case_insensitively()
    {
        settings["Outboxes:0:Database:Provider"] = "postgresql";

        await StartingAsync();

        Assert.Equal(2, validatedOutboxes.Entries.Count);
    }

    [Fact]
    public async Task Duplicate_provider_name_is_a_programming_error()
    {
        var validator = new RelayStartupValidator(Configuration(), [provider, new FakeOutboxStoreProvider("postgresql")], [factory], validatedOutboxes, Options.Create(hostOptions), logger);

        await Assert.ThrowsAsync<InvalidOperationException>(() => validator.StartingAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Duplicate_transport_is_a_programming_error()
    {
        var validator = new RelayStartupValidator(Configuration(), [provider], [factory, new FakeDestinationFactory("kafka")], validatedOutboxes, Options.Create(hostOptions), logger);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => validator.StartingAsync(TestContext.Current.CancellationToken));

        Assert.Equal("Destination transport 'kafka' is registered more than once.", ex.Message);
    }

    [Fact]
    public async Task Every_destination_is_probed_once_with_the_probe_context_and_disposed()
    {
        await StartingAsync();

        var probes = factory.Created.Where(c => c.Context.IsProbe).ToList();
        Assert.Equal(["OrdersTopic", "PaymentsTopic"], probes.Select(c => c.Destination).Order());
        Assert.All(probes, c => Assert.Same(DestinationContext.Probe, c.Context));
        Assert.Equal(["OrdersTopic", "PaymentsTopic"], factory.ProbeStarted.Order());
        Assert.Equal(factory.Created.Count, factory.Disposed.Count);
    }

    [Fact]
    public async Task Bounds_are_read_from_term_shaped_instances_built_like_the_worker_and_disposed()
    {
        settings["Outboxes:1:Ordering:Mode"] = "PerKey";

        await StartingAsync();

        var terms = factory.Created.Where(c => !c.Context.IsProbe).ToList();
        Assert.Equal(
            [
                ("OrdersTopic", new DestinationContext(OrderingMode.None)),
                ("PaymentsTopic", new DestinationContext(OrderingMode.PerKey)),
            ],
            terms);
        Assert.Equal(4, factory.Disposed.Count);
        Assert.Equal(2, factory.ProbeStarted.Count);
    }

    [Fact]
    public async Task Null_bounds_are_accepted()
    {
        factory.Bounds["OrdersTopic"] = _ => null;
        factory.Bounds["PaymentsTopic"] = _ => null;

        await StartingAsync();

        Assert.Equal(2, validatedOutboxes.Entries.Count);
    }

    [Theory]
    [InlineData(0, "00:00:00")]
    [InlineData(-1, "-00:00:01")]
    public async Task Send_bound_not_greater_than_zero_is_refused(int seconds, string text)
    {
        factory.Bounds["PaymentsTopic"] = _ => TimeSpan.FromSeconds(seconds);

        var ex = await Assert.ThrowsAsync<RelayStartupValidationException>(StartingAsync);

        Assert.Equal([$"OutboxRelay.Outboxes[Payments]: destination 'PaymentsTopic' declares SendBound {text}; must be greater than zero"], ex.Failures);
        Assert.Equal(factory.Created.Count, factory.Disposed.Count);
    }

    [Fact]
    public async Task Every_bound_failure_is_collected_into_one_exception()
    {
        factory.Bounds["OrdersTopic"] = _ => TimeSpan.Zero;

        var ex = await Assert.ThrowsAsync<RelayStartupValidationException>(StartingAsync);

        Assert.Equal(
            [
                "OutboxRelay.Outboxes[Orders]: destination 'OrdersTopic' declares SendBound 00:00:00; must be greater than zero",
            ],
            ex.Failures);
        Assert.Equal(factory.Created.Count, factory.Disposed.Count);
    }

    [Fact]
    public async Task Dead_letter_destination_is_checked_with_the_outbox_context()
    {
        settings["Outboxes:1:Ordering:Mode"] = "Strict";
        settings["Outboxes:1:DeadLetter:Destination"] = "orderstopic";
        factory.Bounds["OrdersTopic"] = context => context.Mode == OrderingMode.Strict ? TimeSpan.Zero : null;

        var ex = await Assert.ThrowsAsync<RelayStartupValidationException>(StartingAsync);

        Assert.Equal(["OutboxRelay.Outboxes[Payments]: destination 'OrdersTopic' declares SendBound 00:00:00; must be greater than zero"], ex.Failures);
        Assert.Contains(("OrdersTopic", new DestinationContext(OrderingMode.Strict)), factory.Created);
        Assert.Equal(factory.Created.Count, factory.Disposed.Count);
    }

    [Fact]
    public async Task Mode_dependent_bound_is_checked_per_outbox()
    {
        settings["Outboxes:1:Destination"] = "OrdersTopic";
        settings["Outboxes:1:Ordering:Mode"] = "PerKey";
        factory.Bounds["OrdersTopic"] = context => context.Mode == OrderingMode.None ? TimeSpan.FromSeconds(30) : TimeSpan.Zero;

        var ex = await Assert.ThrowsAsync<RelayStartupValidationException>(StartingAsync);

        Assert.Equal(["OutboxRelay.Outboxes[Payments]: destination 'OrdersTopic' declares SendBound 00:00:00; must be greater than zero"], ex.Failures);
    }

    [Fact]
    public async Task Routed_dead_letter_destination_is_refused_without_key_checks()
    {
        settings["Outboxes:1:DeadLetter:Destination"] = "orderstopic";
        factory.Routes["OrdersTopic"] = new HashSet<string>(StringComparer.Ordinal) { "Bad Key" };

        var ex = await Assert.ThrowsAsync<RelayStartupValidationException>(StartingAsync);

        Assert.Equal(
            [
                "OutboxRelay.Outboxes[Orders]: destination 'OrdersTopic' declares route key 'Bad Key'; must match ^[a-z0-9][a-z0-9._-]{0,62}$",
                "OutboxRelay.Outboxes[Payments].DeadLetter.Destination: destination 'OrdersTopic' declares Routes. A dead-letter destination has one address.",
            ],
            ex.Failures);
        Assert.Equal(factory.Created.Count, factory.Disposed.Count);
    }

    [Fact]
    public async Task Empty_routes_set_is_refused()
    {
        factory.Routes["OrdersTopic"] = new HashSet<string>(StringComparer.Ordinal);

        var ex = await Assert.ThrowsAsync<RelayStartupValidationException>(StartingAsync);

        Assert.Equal(
            ["OutboxRelay.Outboxes[Orders]: destination 'OrdersTopic' declares an empty Routes set; Routes is null (one address) or names at least one route"],
            ex.Failures);
    }

    [Theory]
    [InlineData("Alerts")]
    [InlineData("a\n")]
    [InlineData("")]
    public async Task Route_key_outside_the_pattern_is_refused(string key)
    {
        factory.Routes["PaymentsTopic"] = new HashSet<string>(StringComparer.Ordinal) { "alerts", key };

        var ex = await Assert.ThrowsAsync<RelayStartupValidationException>(StartingAsync);

        Assert.Equal(
            [$"OutboxRelay.Outboxes[Payments]: destination 'PaymentsTopic' declares route key '{key}'; must match ^[a-z0-9][a-z0-9._-]{{0,62}}$"],
            ex.Failures);
    }

    [Fact]
    public async Task Null_route_key_is_refused()
    {
        factory.Routes["PaymentsTopic"] = new HashSet<string>(StringComparer.Ordinal) { "alerts", null! };

        var ex = await Assert.ThrowsAsync<RelayStartupValidationException>(StartingAsync);

        Assert.Equal(
            ["OutboxRelay.Outboxes[Payments]: destination 'PaymentsTopic' declares route key (null); must match ^[a-z0-9][a-z0-9._-]{0,62}$"],
            ex.Failures);
    }

    [Fact]
    public async Task Routed_main_destination_with_single_address_dead_letter_passes()
    {
        settings["Outboxes:1:DeadLetter:Destination"] = "orderstopic";
        factory.Routes["PaymentsTopic"] = new HashSet<string>(StringComparer.Ordinal) { "alerts", "billing.v2", "0" };

        await StartingAsync();

        Assert.Equal(2, validatedOutboxes.Entries.Count);
        Assert.NotNull(validatedOutboxes.Entries[1].DeadLetterDestination);
        Assert.Equal(factory.Created.Count, factory.Disposed.Count);
    }

    [Fact]
    public async Task Route_and_bound_failures_are_collected_into_one_exception()
    {
        settings["Outboxes:1:DeadLetter:Destination"] = "orderstopic";
        factory.Routes["OrdersTopic"] = new HashSet<string>(StringComparer.Ordinal);
        factory.Routes["PaymentsTopic"] = new HashSet<string>(StringComparer.Ordinal) { "-bad" };

        var ex = await Assert.ThrowsAsync<RelayStartupValidationException>(StartingAsync);

        Assert.Equal(
            [
                "OutboxRelay.Outboxes[Orders]: destination 'OrdersTopic' declares an empty Routes set; Routes is null (one address) or names at least one route",
                "OutboxRelay.Outboxes[Payments]: destination 'PaymentsTopic' declares route key '-bad'; must match ^[a-z0-9][a-z0-9._-]{0,62}$",
                "OutboxRelay.Outboxes[Payments].DeadLetter.Destination: destination 'OrdersTopic' declares Routes. A dead-letter destination has one address.",
            ],
            ex.Failures);
        Assert.Equal(factory.Created.Count, factory.Disposed.Count);
    }

    [Fact]
    public async Task Probes_run_in_parallel_and_failures_keep_config_order()
    {
        var paymentsStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        factory.Probes["OrdersTopic"] = async ct =>
        {
            // Completes only if the second probe starts while this one is still running.
            await paymentsStarted.Task.WaitAsync(TimeSpan.FromSeconds(10), ct);
            throw new DestinationValidationException("Destinations[OrdersTopic]: topic 'orders.events' does not exist");
        };
        factory.Probes["PaymentsTopic"] = _ =>
        {
            paymentsStarted.SetResult();
            throw new DestinationValidationException("Destinations[PaymentsTopic]: topic 'payments.events' does not exist");
        };

        var ex = await Assert.ThrowsAsync<RelayStartupValidationException>(StartingAsync);

        Assert.Equal(
            [
                "OutboxRelay.Destinations[OrdersTopic]: topic 'orders.events' does not exist",
                "OutboxRelay.Destinations[PaymentsTopic]: topic 'payments.events' does not exist",
            ],
            ex.Failures);
        Assert.Equal(2, factory.Disposed.Count);
        Assert.Empty(validatedOutboxes.Entries);
    }

    [Fact]
    public async Task Probes_run_only_after_every_store_validated()
    {
        provider.Configure = (name, store) => store.ValidateFailure = name == "Payments"
            ? new OutboxValidationException("Outboxes[Payments]: table 'payments.outbox' not found")
            : null;

        await Assert.ThrowsAsync<RelayStartupValidationException>(StartingAsync);

        Assert.Empty(factory.Created);
    }

    [Fact]
    public async Task Unexpected_probe_exception_escapes()
    {
        factory.Probes["OrdersTopic"] = _ => throw new InvalidOperationException("bug");

        await Assert.ThrowsAsync<InvalidOperationException>(StartingAsync);
    }

    [Fact]
    public async Task Failure_text_never_contains_connection_string()
    {
        settings["Outboxes:0:Destination"] = "Missing";
        settings["Outboxes:1:Database:Connection"] = "Undefined";

        var ex = await Assert.ThrowsAsync<RelayStartupValidationException>(StartingAsync);

        Assert.Equal(2, ex.Failures.Count);
        Assert.DoesNotContain("sentinel", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Other_lifecycle_methods_do_nothing()
    {
        var validator = new RelayStartupValidator(Configuration(), [provider], [factory], validatedOutboxes, Options.Create(hostOptions), logger);
        var ct = TestContext.Current.CancellationToken;

        await validator.StartAsync(ct);
        await validator.StartedAsync(ct);
        await validator.StoppingAsync(ct);
        await validator.StopAsync(ct);
        await validator.StoppedAsync(ct);

        Assert.Empty(provider.Stores);
        Assert.Empty(validatedOutboxes.Entries);
    }

    private Task StartingAsync() =>
        new RelayStartupValidator(Configuration(), [provider], [factory], validatedOutboxes, Options.Create(hostOptions), logger).StartingAsync(TestContext.Current.CancellationToken);

    private void RemoveOutboxesAndSecondDestination()
    {
        foreach (var key in settings.Keys.Where(k => k.StartsWith("Outboxes:", StringComparison.Ordinal) || k.StartsWith("Destinations:1:", StringComparison.Ordinal)).ToList())
        {
            settings.Remove(key);
        }
    }

    private RelayConfiguration Configuration() => new(new ConfigurationBuilder().AddInMemoryCollection(settings).Build());
}
