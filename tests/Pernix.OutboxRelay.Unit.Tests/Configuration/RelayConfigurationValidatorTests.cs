using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Configuration;

public sealed class RelayConfigurationValidatorTests
{
    private static readonly OrderingMode[] AllModes = [OrderingMode.None, OrderingMode.PerKey, OrderingMode.Strict];

    private readonly Dictionary<string, string?> settings = new()
    {
        ["ConnectionStrings:OrdersDb"] = "Host=orders-db;Database=orders;Username=relay;Password=pw",
        ["KafkaClusters:Main:BootstrapServers"] = "kafka-1:9092,kafka-2:9092",
        ["KafkaClusters:Main:Producer:acks"] = "all",
        ["Destinations:0:Name"] = "OrdersTopic",
        ["Destinations:0:Transport"] = "Kafka",
        ["Destinations:0:Kafka:Cluster"] = "Main",
        ["Destinations:0:Kafka:Topic"] = "orders.events",
        ["Destinations:1:Name"] = "OrdersDeadLetter",
        ["Destinations:1:Transport"] = "Kafka",
        ["Destinations:1:Kafka:Cluster"] = "Main",
        ["Destinations:1:Kafka:Topic"] = "orders.dead-letter",
        ["Outboxes:0:Name"] = "Orders",
        ["Outboxes:0:Destination"] = "OrdersTopic",
        ["Outboxes:0:Database:Provider"] = "PostgreSql",
        ["Outboxes:0:Database:Connection"] = "OrdersDb",
        ["Outboxes:0:Database:Table"] = "public.outbox",
        ["Outboxes:0:Polling:Interval"] = "00:00:01",
        ["Outboxes:0:Polling:BatchSize"] = "500",
        ["Outboxes:0:Retry:MaxRetryAttempts"] = "3",
        ["Outboxes:0:Retry:InitialDelay"] = "00:00:10",
        ["Outboxes:0:Retry:BackoffMultiplier"] = "2.0",
        ["Outboxes:0:Retry:MaxDelay"] = "00:05:00",
        ["Outboxes:0:Ordering:Mode"] = "None",
        ["Outboxes:0:Ordering:OnPermanentFailure"] = "Continue",
    };

    private readonly FakeDestinationFactory factory = new();

    private IReadOnlyList<string> Validate(params OrderingMode[] supportedModes)
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(settings).Build();
        var provider = new FakeProvider("PostgreSql", supportedModes.Length == 0 ? AllModes : supportedModes);
        return RelayConfigurationValidator.Validate(
            RelayOptionsBinder.Bind(configuration),
            configuration.GetConnectionString,
            new Dictionary<string, IOutboxStoreProvider>(StringComparer.OrdinalIgnoreCase) { [provider.Name] = provider },
            Factories(factory));
    }

    private static DestinationFactoryResolver Factories(params IDestinationFactory[] factories) => new(factories);

    private void Set(string key, string? value)
    {
        if (value is null)
        {
            settings.Remove(key);
        }
        else
        {
            settings[key] = value;
        }
    }

    private void RemoveSection(string prefix)
    {
        foreach (var key in settings.Keys.Where(k => k.StartsWith(prefix + ":", StringComparison.Ordinal)).ToList())
        {
            settings.Remove(key);
        }
    }

    [Fact]
    public void Design_example_config_is_valid()
    {
        Assert.Empty(Validate());
    }

    [Fact]
    public void No_outboxes_fails()
    {
        RemoveSection("Outboxes");
        RemoveSection("Destinations");

        Assert.Equal(["Outboxes: at least one outbox is required"], Validate());
    }

    [Fact]
    public void No_outboxes_is_reported_with_other_configuration_failures()
    {
        RemoveSection("Outboxes");
        Set("Destinations:0:Transport", null);

        Assert.Equal(["Outboxes: at least one outbox is required", "Destinations[OrdersTopic].Transport: required"], Validate());
    }

    [Fact]
    public void Empty_configuration_fails_with_the_required_outbox_rule()
    {
        settings.Clear();

        Assert.Equal(["Outboxes: at least one outbox is required"], Validate());
    }

    [Theory]
    [InlineData("Outboxes:0:Name", null, "Outboxes[0].Name: required")]
    [InlineData("Outboxes:0:Name", "", "Outboxes[0].Name: required")]
    [InlineData("Outboxes:0:Destination", null, "Outboxes[Orders].Destination: required")]
    [InlineData("Outboxes:0:Destination", " ", "Outboxes[Orders].Destination: required")]
    [InlineData("Outboxes:0:Database:Provider", null, "Outboxes[Orders].Database.Provider: required")]
    [InlineData("Outboxes:0:Database:Connection", "", "Outboxes[Orders].Database.Connection: required")]
    [InlineData("Outboxes:0:Database:Table", null, "Outboxes[Orders].Database.Table: required")]
    [InlineData("Outboxes:0:Ordering:Mode", null, "Outboxes[Orders].Ordering.Mode: required, one of None, PerKey, Strict")]
    [InlineData("Destinations:0:Name", null, "Destinations[0].Name: required")]
    [InlineData("Destinations:0:Name", "", "Destinations[0].Name: required")]
    [InlineData("Destinations:0:Transport", null, "Destinations[OrdersTopic].Transport: required")]
    [InlineData("Destinations:0:Transport", " ", "Destinations[OrdersTopic].Transport: required")]
    public void Missing_required_setting_fails(string key, string? value, string expected)
    {
        Set(key, value);

        Assert.Contains(expected, Validate());
    }

    [Fact]
    public void Missing_ordering_section_fails_on_mode()
    {
        RemoveSection("Outboxes:0:Ordering");

        Assert.Equal(["Outboxes[Orders].Ordering.Mode: required, one of None, PerKey, Strict"], Validate());
    }

    [Fact]
    public void Missing_database_section_fails_on_each_field()
    {
        RemoveSection("Outboxes:0:Database");

        Assert.Equal(
            [
                "Outboxes[Orders].Database.Provider: required",
                "Outboxes[Orders].Database.Connection: required",
                "Outboxes[Orders].Database.Table: required",
            ],
            Validate());
    }

    [Fact]
    public void Outbox_database_routes_is_an_unknown_setting()
    {
        Set("Outboxes:0:Database:Routes:orders", "public.orders");

        Assert.Equal(["Outboxes[Orders].Database: unknown setting 'Routes'"], Validate());
    }

    [Theory]
    [InlineData("Outboxes:0:Ordering:Mode", "7", "Outboxes[Orders].Ordering.Mode: must be one of None, PerKey, Strict")]
    [InlineData("Outboxes:0:Ordering:OnPermanentFailure", "5", "Outboxes[Orders].Ordering.OnPermanentFailure: must be one of Continue, Block")]
    public void Undefined_numeric_enum_value_fails_once(string key, string value, string expected)
    {
        Set(key, value);

        Assert.Equal([expected], Validate());
    }

    [Fact]
    public void Undefined_enum_on_constructed_options_fails_without_value()
    {
        var options = new RelayOptions
        {
            Outboxes = { new OutboxOptions { Name = "Orders", Ordering = new OrderingOptions { Mode = (OrderingMode)7, OnPermanentFailure = (OnPermanentFailure)5 } } },
        };

        var failures = RelayConfigurationValidator.Validate(options, _ => null, new Dictionary<string, IOutboxStoreProvider>(), Factories());

        Assert.Contains("Outboxes[Orders].Ordering.Mode: must be one of None, PerKey, Strict", failures);
        Assert.Contains("Outboxes[Orders].Ordering.OnPermanentFailure: must be one of Continue, Block", failures);
    }

    [Fact]
    public void Binding_failures_of_every_outbox_are_reported_with_the_other_checks()
    {
        foreach (var (key, value) in settings.Where(kv => kv.Key.StartsWith("Outboxes:0:", StringComparison.Ordinal)).ToList())
        {
            settings["Outboxes:1:" + key["Outboxes:0:".Length..]] = value;
        }

        Set("Outboxes:1:Name", "Payments");
        Set("Outboxes:0:Ordering:Mode", "sentinel-mode");
        Set("Outboxes:1:Ordering:OnPermanentFailure", "sentinel-failure");
        Set("Outboxes:1:Destination", "Missing");

        var ex = new RelayStartupValidationException(Validate());

        Assert.Equal(
            [
                "Outboxes[Orders].Ordering.Mode: must be one of None, PerKey, Strict",
                "Outboxes[Payments].Ordering.OnPermanentFailure: must be one of Continue, Block",
                "Outboxes[Payments].Destination: destination 'Missing' is not defined in Destinations",
            ],
            ex.Failures);
        Assert.DoesNotContain("sentinel", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Validate_uses_the_provider_map_as_given()
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(settings).Build();
        var provider = new FakeProvider("PostgreSql", AllModes);

        var failures = RelayConfigurationValidator.Validate(
            RelayOptionsBinder.Bind(configuration),
            configuration.GetConnectionString,
            new Dictionary<string, IOutboxStoreProvider>(StringComparer.Ordinal) { ["Other"] = provider },
            Factories(factory));

        Assert.Equal(["Outboxes[Orders].Database.Provider: provider 'PostgreSql' is not registered"], failures);
    }

    [Theory]
    [InlineData("Outboxes:0:Polling:BatchSize", "1", "0", "Outboxes[Orders].Polling.BatchSize: must be at least 1")]
    [InlineData("Outboxes:0:Polling:BatchSize", "1000", "1001", "Outboxes[Orders].Polling.BatchSize: must be at most 1000")]
    [InlineData("Outboxes:0:Polling:Interval", "00:00:00.001", "00:00:00", "Outboxes[Orders].Polling.Interval: must be greater than 00:00:00")]
    [InlineData("Outboxes:0:Retry:MaxRetryAttempts", "0", "-1", "Outboxes[Orders].Retry.MaxRetryAttempts: must be at least 0")]
    [InlineData("Outboxes:0:Retry:InitialDelay", "00:00:00.001", "00:00:00", "Outboxes[Orders].Retry.InitialDelay: must be greater than 00:00:00")]
    [InlineData("Outboxes:0:Retry:BackoffMultiplier", "1.0", "0.99", "Outboxes[Orders].Retry.BackoffMultiplier: must be at least 1.0")]
    [InlineData("Outboxes:0:Retry:BackoffMultiplier", "1.0", "NaN", "Outboxes[Orders].Retry.BackoffMultiplier: must be at least 1.0")]
    [InlineData("Outboxes:0:Retry:MaxDelay", "00:00:10", "00:00:09", "Outboxes[Orders].Retry.MaxDelay: must be at least Retry.InitialDelay")]
    public void Range_passes_at_limit_and_fails_past_it(string key, string atLimit, string pastLimit, string expected)
    {
        Set(key, atLimit);
        Assert.Empty(Validate());

        Set(key, pastLimit);
        Assert.Equal([expected], Validate());
    }

    [Fact]
    public void Block_with_mode_none_fails()
    {
        Set("Outboxes:0:Ordering:Mode", "None");
        Set("Outboxes:0:Ordering:OnPermanentFailure", "Block");

        Assert.Equal(["Outboxes[Orders].Ordering.OnPermanentFailure: Block is not allowed when Ordering.Mode is None"], Validate());
    }

    [Theory]
    [InlineData("PerKey", "Continue")]
    [InlineData("PerKey", "Block")]
    [InlineData("strict", "Continue")]
    [InlineData("Strict", "Block")]
    public void Ordered_mode_is_accepted_with_either_failure_policy(string mode, string onPermanentFailure)
    {
        Set("Outboxes:0:Ordering:Mode", mode);
        Set("Outboxes:0:Ordering:OnPermanentFailure", onPermanentFailure);

        Assert.Empty(Validate());
    }

    [Theory]
    [InlineData("PerKey", OrderingMode.PerKey)]
    [InlineData("Strict", OrderingMode.Strict)]
    public void Ordered_mode_not_supported_by_provider_fails(string value, OrderingMode mode)
    {
        Set("Outboxes:0:Ordering:Mode", value);

        Assert.Equal([$"Outboxes[Orders].Ordering.Mode: provider 'PostgreSql' does not support '{mode}'"], Validate(OrderingMode.None));
    }

    [Fact]
    public void Mode_not_supported_by_provider_fails()
    {
        Assert.Equal(["Outboxes[Orders].Ordering.Mode: provider 'PostgreSql' does not support 'None'"], Validate(OrderingMode.Strict));
    }

    [Fact]
    public void Mode_supported_by_provider_passes()
    {
        Assert.Empty(Validate(OrderingMode.None));
    }

    [Fact]
    public void Mode_check_is_skipped_for_unknown_provider()
    {
        Set("Outboxes:0:Database:Provider", "SqlServer");

        Assert.Equal(["Outboxes[Orders].Database.Provider: provider 'SqlServer' is not registered"], Validate(OrderingMode.Strict));
    }

    [Fact]
    public void Dead_letter_to_own_destination_fails()
    {
        Set("Outboxes:0:DeadLetter:Destination", "orderstopic");

        Assert.Equal(["Outboxes[Orders].DeadLetter.Destination: names the outbox's own destination 'OrdersTopic'"], Validate());
    }

    [Fact]
    public void Kafka_dead_letter_destination_is_accepted()
    {
        Set("Outboxes:0:DeadLetter:Destination", "OrdersDeadLetter");

        Assert.Empty(Validate());
    }

    [Fact]
    public void Database_dead_letter_destination_is_refused_as_unregistered_transport()
    {
        Set("Outboxes:0:DeadLetter:Destination", "OrdersDeadLetter");
        Set("Destinations:1:Transport", "Database");
        Set("Destinations:1:Database:Provider", "PostgreSql");
        Set("Destinations:1:Database:Connection", "OrdersDb");
        Set("Destinations:1:Database:Table", "public.dead_letter");

        Assert.Equal(["Destinations[OrdersDeadLetter].Transport: transport 'Database' is not registered"], Validate());
    }

    [Theory]
    [InlineData("")]
    [InlineData(null)]
    public void Empty_or_absent_dead_letter_is_off(string? value)
    {
        Set("Outboxes:0:DeadLetter:Destination", value);

        Assert.Empty(Validate());
    }

    [Fact]
    public void Undefined_outbox_destination_fails()
    {
        Set("Outboxes:0:Destination", "Missing");

        Assert.Equal(["Outboxes[Orders].Destination: destination 'Missing' is not defined in Destinations"], Validate());
    }

    [Fact]
    public void Undefined_dead_letter_destination_fails()
    {
        Set("Outboxes:0:DeadLetter:Destination", "Missing");

        Assert.Equal(["Outboxes[Orders].DeadLetter.Destination: destination 'Missing' is not defined in Destinations"], Validate());
    }

    [Fact]
    public void Undefined_connection_fails()
    {
        Set("Outboxes:0:Database:Connection", "Other");

        Assert.Equal(["Outboxes[Orders].Database.Connection: connection string 'Other' is not defined or empty in OutboxRelay.ConnectionStrings"], Validate());
    }

    [Fact]
    public void Empty_connection_string_value_fails()
    {
        Set("ConnectionStrings:OrdersDb", "");

        Assert.Equal(["Outboxes[Orders].Database.Connection: connection string 'OrdersDb' is not defined or empty in OutboxRelay.ConnectionStrings"], Validate());
    }

    [Fact]
    public void SqlServer_provider_is_not_registered()
    {
        Set("Outboxes:0:Database:Provider", "SqlServer");

        Assert.Equal(["Outboxes[Orders].Database.Provider: provider 'SqlServer' is not registered"], Validate());
    }

    [Theory]
    [InlineData("Database")]
    [InlineData("database")]
    [InlineData("Sqs")]
    public void Unregistered_transport_is_refused_naming_the_destination(string transport)
    {
        Set("Destinations:1:Transport", transport);
        Set("Destinations:1:Database:Provider", "PostgreSql");
        Set("Destinations:1:Database:Connection", "OrdersDb");
        Set("Destinations:1:Database:Table", "public.dead_letter");

        Assert.Equal([$"Destinations[OrdersDeadLetter].Transport: transport '{transport}' is not registered"], Validate());
        Assert.DoesNotContain(factory.Validated, v => v.Destination == "OrdersDeadLetter");
    }

    [Fact]
    public void Transport_resolves_case_insensitively_and_validates_each_destination_with_its_path()
    {
        Set("Destinations:0:Transport", "kAFKA");
        Set("Destinations:1:Name", null);

        Validate();

        Assert.Equal([("OrdersTopic", "Destinations[OrdersTopic]"), ("", "Destinations[1]")], factory.Validated);
    }

    [Fact]
    public void Transport_failures_are_merged_with_core_failures()
    {
        factory.ValidateFailures["OrdersTopic"] = ["Destinations[OrdersTopic].Kafka.Topic: required when Transport is Kafka"];
        Set("Outboxes:0:Polling:BatchSize", "0");

        Assert.Equal(
            [
                "Outboxes[Orders].Polling.BatchSize: must be at least 1",
                "Destinations[OrdersTopic].Kafka.Topic: required when Transport is Kafka",
            ],
            Validate());
    }

    [Fact]
    public void Refusals_are_collected_with_every_other_failure()
    {
        factory.ValidateFailures["OrdersTopic"] = ["Destinations[OrdersTopic]: unknown setting 'Database'"];
        Set("Outboxes:0:Polling:BatchSize", "0");
        Set("Outboxes:0:Ordering:Mode", "PerKey");
        Set("Outboxes:0:DeadLetter:Destination", "OrdersDeadLetter");
        Set("Destinations:1:Transport", "Database");

        Assert.Equal(
            [
                "Outboxes[Orders].Polling.BatchSize: must be at least 1",
                "Destinations[OrdersTopic]: unknown setting 'Database'",
                "Destinations[OrdersDeadLetter].Transport: transport 'Database' is not registered",
            ],
            Validate());
    }

    [Fact]
    public void Duplicate_destination_names_differing_by_case_fail()
    {
        Set("Destinations:1:Name", "ORDERSTOPIC");
        Set("Outboxes:0:DeadLetter:Destination", null);

        Assert.Equal(["Destinations[1].Name: 'ORDERSTOPIC' duplicates Destinations[0] (names are case-insensitive)"], Validate());
    }

    [Fact]
    public void Duplicate_outbox_names_differing_by_case_fail()
    {
        foreach (var (key, value) in settings.Where(kv => kv.Key.StartsWith("Outboxes:0:", StringComparison.Ordinal)).ToList())
        {
            settings["Outboxes:1:" + key["Outboxes:0:".Length..]] = value;
        }

        Set("Outboxes:1:Name", "ORDERS");

        Assert.Equal(["Outboxes[1].Name: 'ORDERS' duplicates Outboxes[0] (names are case-insensitive)"], Validate());
    }

    [Fact]
    public void Mixed_case_references_resolve()
    {
        Set("Outboxes:0:Destination", "ORDERSTOPIC");
        Set("Outboxes:0:Database:Provider", "postgresql");
        Set("Outboxes:0:Database:Connection", "ORDERSDB");
        Set("Destinations:1:Transport", "kafka");

        Assert.Empty(Validate());
    }

    [Fact]
    public void Multiple_faults_are_all_reported()
    {
        Set("Outboxes:0:Polling:BatchSize", "0");
        Set("Outboxes:0:Destination", "Missing");
        Set("Outboxes:0:Ordering:Mode", "None");
        Set("Outboxes:0:Ordering:OnPermanentFailure", "Block");
        Set("Destinations:1:Name", null);

        Assert.Equal(
            [
                "Destinations[1].Name: required",
                "Outboxes[Orders].Polling.BatchSize: must be at least 1",
                "Outboxes[Orders].Destination: destination 'Missing' is not defined in Destinations",
                "Outboxes[Orders].Ordering.OnPermanentFailure: Block is not allowed when Ordering.Mode is None",
            ],
            Validate());
    }

    [Fact]
    public void Failures_never_contain_secret_values()
    {
        Set("ConnectionStrings:OrdersDb", "Host=secret-host;Password=secret-pw");
        Set("KafkaClusters:Main:Producer:sasl.password", "sentinel-producer");
        Set("Outboxes:0:Database:Provider", "SqlServer");
        Set("Outboxes:0:Retry:MaxDelay", "00:00:01");
        Set("Outboxes:0:DeadLetter:Destination", "OrdersTopic");
        Set("Destinations:1:Transport", "Database");
        Set("Destinations:1:Database:Connection", "OrdersDb");

        var failures = Validate();

        Assert.True(failures.Count >= 4);
        Assert.All(failures, f =>
        {
            Assert.DoesNotContain("secret-host", f);
            Assert.DoesNotContain("secret-pw", f);
            Assert.DoesNotContain("sentinel-producer", f);
        });
    }

    private sealed class FakeProvider(string name, IEnumerable<OrderingMode> modes) : IOutboxStoreProvider
    {
        public string Name => name;

        public IReadOnlySet<OrderingMode> SupportedModes { get; } = modes.ToHashSet();

        public IOutboxStore CreateStore(OutboxOptions options) => throw new NotSupportedException();
    }
}
