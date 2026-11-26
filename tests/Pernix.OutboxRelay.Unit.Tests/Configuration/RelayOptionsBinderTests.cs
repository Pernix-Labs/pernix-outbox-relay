using System.Text.Json;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Configuration;

public sealed class RelayOptionsBinderTests
{
    private static RelayOptions Bind(Dictionary<string, string?> settings) =>
        RelayOptionsBinder.Bind(new ConfigurationBuilder().AddInMemoryCollection(settings).Build());

    private static IReadOnlyList<string> Failures(Dictionary<string, string?> settings) =>
        RelayConfigurationValidator.Validate(Bind(settings), _ => null, new Dictionary<string, IOutboxStoreProvider>(), new DestinationFactoryResolver([]));

    [Fact]
    public void Omitted_settings_get_design_defaults()
    {
        var options = Bind(new() { ["Outboxes:0:Name"] = "Orders" });

        var outbox = Assert.Single(options.Outboxes);
        Assert.Equal(TimeSpan.FromSeconds(1), outbox.Polling.Interval);
        Assert.Equal(500, outbox.Polling.BatchSize);
        Assert.Equal(3, outbox.Retry.MaxRetryAttempts);
        Assert.Equal(TimeSpan.FromSeconds(10), outbox.Retry.InitialDelay);
        Assert.Equal(2.0, outbox.Retry.BackoffMultiplier);
        Assert.Equal(TimeSpan.FromMinutes(5), outbox.Retry.MaxDelay);
        Assert.Null(outbox.DeadLetter);
        Assert.Null(outbox.Ordering);
        Assert.Equal(OnPermanentFailure.Continue, new OrderingOptions().OnPermanentFailure);
    }

    [Fact]
    public void Omitted_on_permanent_failure_defaults_to_continue_when_ordering_present()
    {
        var options = Bind(new() { ["Outboxes:0:Ordering:Mode"] = "Strict" });

        Assert.Equal(OnPermanentFailure.Continue, options.Outboxes[0].Ordering!.OnPermanentFailure);
    }

    [Fact]
    public void Required_settings_have_no_default()
    {
        var options = Bind(new()
        {
            ["Outboxes:0:Polling:BatchSize"] = "10",
            ["Destinations:0:Kafka:Topic"] = "t",
        });

        var outbox = Assert.Single(options.Outboxes);
        Assert.Null(outbox.Name);
        Assert.Null(outbox.Destination);
        Assert.Null(outbox.Database);
        Assert.Null(outbox.Ordering);
        var destination = Assert.Single(options.Destinations);
        Assert.Null(destination.Name);
        Assert.Null(destination.Transport);
    }

    [Fact]
    public void Absent_sections_bind_empty()
    {
        var options = Bind([]);

        Assert.Empty(options.Destinations);
        Assert.Empty(options.Outboxes);
    }

    [Fact]
    public void Enums_bind_case_insensitively()
    {
        var options = Bind(new()
        {
            ["Outboxes:0:Ordering:Mode"] = "perkey",
            ["Outboxes:0:Ordering:OnPermanentFailure"] = "BLOCK",
        });

        Assert.Equal(OrderingMode.PerKey, options.Outboxes[0].Ordering!.Mode);
        Assert.Equal(OnPermanentFailure.Block, options.Outboxes[0].Ordering!.OnPermanentFailure);
    }

    [Fact]
    public void Durations_bind_from_hh_mm_ss()
    {
        var options = Bind(new()
        {
            ["Outboxes:0:Polling:Interval"] = "00:00:02",
            ["Outboxes:0:Retry:InitialDelay"] = "00:00:30",
            ["Outboxes:0:Retry:MaxDelay"] = "00:10:00",
        });

        Assert.Equal(TimeSpan.FromSeconds(2), options.Outboxes[0].Polling.Interval);
        Assert.Equal(TimeSpan.FromSeconds(30), options.Outboxes[0].Retry.InitialDelay);
        Assert.Equal(TimeSpan.FromMinutes(10), options.Outboxes[0].Retry.MaxDelay);
    }

    [Fact]
    public void Kafka_clusters_key_is_ignored()
    {
        var failures = Failures(new()
        {
            ["KafkaClusters:Main:BootstrapServers"] = "b:9092",
            ["KafkaClusters:Main:BootstrapServer"] = "sentinel-unknown",
        });

        // The only failure is the required-outbox rule: the Kafka destination package owns this key.
        Assert.Equal(["Outboxes: at least one outbox is required"], failures);
    }

    [Fact]
    public void Bad_value_fails_with_path_and_without_raw_value()
    {
        var failures = Failures(new()
        {
            ["Outboxes:0:Name"] = "Orders",
            ["Outboxes:0:Polling:BatchSize"] = "sentinel-xyz",
        });

        Assert.Contains(failures, f => f.StartsWith("Outboxes[Orders].Polling.BatchSize: cannot convert value to ", StringComparison.Ordinal));
        Assert.All(failures, f => Assert.DoesNotContain("sentinel-xyz", f));
        Assert.DoesNotContain("sentinel-xyz", new RelayStartupValidationException(failures).Message);
    }

    [Theory]
    [InlineData("Outboxes:0:Ordering:OnPermanentFaliure", "Outboxes[Orders].Ordering: unknown setting 'OnPermanentFaliure'")]
    [InlineData("Outboxes:0:DeadLeter:Destination", "Outboxes[Orders]: unknown setting 'DeadLeter'")]
    [InlineData("Outboxes:0:Database:Tabel", "Outboxes[Orders].Database: unknown setting 'Tabel'")]
    [InlineData("Outboxes:0:Database:SendTimeout", "Outboxes[Orders].Database: unknown setting 'SendTimeout'")]
    [InlineData("Outboxes:0:Database:MaxInFlight", "Outboxes[Orders].Database: unknown setting 'MaxInFlight'")]
    [InlineData("Outboxes:0:Polling:Intreval", "Outboxes[Orders].Polling: unknown setting 'Intreval'")]
    [InlineData("Outboxes:0:Retry:MaxDelays", "Outboxes[Orders].Retry: unknown setting 'MaxDelays'")]
    [InlineData("Outboxes:0:Name:Extra", "Outboxes[Orders].Name: unknown setting 'Extra'")]
    public void Unknown_key_fails_naming_the_key_and_not_the_value(string key, string expected)
    {
        var failures = Failures(new()
        {
            ["Outboxes:0:Name"] = "Orders",
            ["Destinations:0:Name"] = "OrdersTopic",
            [key] = "sentinel-unknown",
        });

        Assert.Contains(expected, failures);
        Assert.All(failures, f => Assert.DoesNotContain("sentinel-unknown", f));
    }

    [Theory]
    [InlineData("Polling:Interval", "5")]
    [InlineData("Retry:MaxDelay", "300")]
    [InlineData("Retry:InitialDelay", "1.5")]
    [InlineData("Retry:InitialDelay", "00:10")]
    [InlineData("Polling:Interval", "sentinel-duration")]
    public void Duration_not_in_hh_mm_ss_form_fails(string key, string value)
    {
        var failures = Failures(new()
        {
            ["Outboxes:0:Name"] = "Orders",
            ["Outboxes:0:" + key] = value,
        });

        Assert.Contains($"Outboxes[Orders].{key.Replace(':', '.')}: must be a duration in hh:mm:ss form", failures);
        Assert.All(failures, f => Assert.DoesNotContain("sentinel-duration", f));
    }

    [Fact]
    public void Rejected_value_skips_cross_field_checks_of_its_group()
    {
        var failures = Failures(new()
        {
            ["Outboxes:0:Name"] = "Orders",
            ["Outboxes:0:Retry:InitialDelay"] = "5",
            ["Outboxes:0:Retry:MaxDelay"] = "00:00:08",
        });

        Assert.Contains("Outboxes[Orders].Retry.InitialDelay: must be a duration in hh:mm:ss form", failures);
        Assert.DoesNotContain(failures, f => f.StartsWith("Outboxes[Orders].Retry.MaxDelay", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("1.00:00:00", 1, 0)]
    [InlineData("00:00:00.5", 0, 500)]
    public void Duration_accepts_day_prefix_and_fraction(string value, int days, int milliseconds)
    {
        var options = Bind(new() { ["Outboxes:0:Retry:MaxDelay"] = value });

        Assert.Equal(new TimeSpan(days, 0, 0, 0, milliseconds), options.Outboxes[0].Retry.MaxDelay);
        Assert.DoesNotContain(Failures(new() { ["Outboxes:0:Retry:MaxDelay"] = value }), f => f.Contains("hh:mm:ss", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("Outboxes:0:Ordering:Mode", "0", "Outboxes[Orders].Ordering.Mode: must be one of None, PerKey, Strict")]
    [InlineData("Outboxes:0:Ordering:Mode", "None, Strict", "Outboxes[Orders].Ordering.Mode: must be one of None, PerKey, Strict")]
    [InlineData("Outboxes:0:Ordering:Mode", "sentinel-mode", "Outboxes[Orders].Ordering.Mode: must be one of None, PerKey, Strict")]
    [InlineData("Outboxes:0:Ordering:OnPermanentFailure", "1", "Outboxes[Orders].Ordering.OnPermanentFailure: must be one of Continue, Block")]
    public void Enum_text_that_is_not_a_defined_name_fails(string key, string value, string expected)
    {
        var failures = Failures(new()
        {
            ["Outboxes:0:Name"] = "Orders",
            ["Destinations:0:Name"] = "OrdersTopic",
            [key] = value,
        });

        Assert.Contains(expected, failures);
        Assert.All(failures, f => Assert.DoesNotContain("sentinel-mode", f));
    }

    [Fact]
    public void Binding_continues_past_the_first_conversion_error()
    {
        var failures = Failures(new()
        {
            ["Outboxes:0:Name"] = "Orders",
            ["Outboxes:0:Polling:BatchSize"] = "sentinel-a",
            ["Outboxes:0:Retry:MaxRetryAttempts"] = "sentinel-b",
            ["Outboxes:1:Retry:BackoffMultiplier"] = "sentinel-c",
        });

        Assert.Contains(failures, f => f.StartsWith("Outboxes[Orders].Polling.BatchSize: cannot convert value to ", StringComparison.Ordinal));
        Assert.Contains(failures, f => f.StartsWith("Outboxes[Orders].Retry.MaxRetryAttempts: cannot convert value to ", StringComparison.Ordinal));
        Assert.Contains(failures, f => f.StartsWith("Outboxes[1].Retry.BackoffMultiplier: cannot convert value to ", StringComparison.Ordinal));
        Assert.Contains("Outboxes[1].Name: required", failures);
        Assert.All(failures, f => Assert.DoesNotContain("sentinel", f, StringComparison.Ordinal));
    }

    [Fact]
    public void Bind_section_rejects_unknown_key_and_bad_value_with_binder_messages()
    {
        var section = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Block:Interval"] = "sentinel-duration",
            ["Block:BatchSize"] = "sentinel-number",
            ["Block:Intreval"] = "sentinel-unknown",
        }).Build().GetSection("Block");
        var failures = new List<string>();

        var bound = RelayOptionsBinder.BindSection<PollingOptions>(section, "Custom[x]", failures);

        Assert.Contains("Custom[x]: unknown setting 'Intreval'", failures);
        Assert.Contains("Custom[x].Interval: must be a duration in hh:mm:ss form", failures);
        Assert.Contains(failures, f => f.StartsWith("Custom[x].BatchSize: cannot convert value to ", StringComparison.Ordinal));
        Assert.All(failures, f => Assert.DoesNotContain("sentinel", f, StringComparison.Ordinal));
        Assert.Equal(new PollingOptions().BatchSize, bound.BatchSize);
    }

    [Fact]
    public void Bind_section_accepts_scalar_lists_by_index()
    {
        var (bound, failures) = BindLists(new()
        {
            ["Block:Names:0"] = "a",
            ["Block:Names:1"] = "b",
            ["Block:Codes:0"] = "7",
            ["Block:Modes:0"] = "strict",
            ["Block:Delays:0"] = "00:00:05",
            ["Block:Nested:Names:0"] = "c",
        });

        Assert.Empty(failures);
        Assert.Equal(["a", "b"], bound.Names!);
        Assert.Equal([7], bound.Codes);
        Assert.Equal([OrderingMode.Strict], bound.Modes);
        Assert.Equal([TimeSpan.FromSeconds(5)], bound.Delays!);
        Assert.Equal(["c"], bound.Nested!.Names!);
    }

    [Fact]
    public void Bind_section_rejects_an_index_on_a_non_list_property()
    {
        var (bound, failures) = BindLists(new()
        {
            ["Block:Name:0"] = "sentinel-scalar",
            ["Block:Nested:0"] = "sentinel-section",
            ["Block:Names:first"] = "sentinel-key",
            ["Block:Names:0:Deep"] = "sentinel-deep",
        });

        Assert.Equal(
            [
                "Custom[x].Name: unknown setting '0'",
                "Custom[x].Names: unknown setting '0'",
                "Custom[x].Names: unknown setting 'first'",
                "Custom[x].Nested: unknown setting '0'",
            ],
            failures.Order(StringComparer.Ordinal));
        Assert.All(failures, f => Assert.DoesNotContain("sentinel", f, StringComparison.Ordinal));
        Assert.Null(bound.Name);
        Assert.Null(bound.Names!);
    }

    [Fact]
    public void Bind_section_rejects_bad_list_element_values_without_the_value()
    {
        var (bound, failures) = BindLists(new()
        {
            ["Block:Names:0"] = "ok",
            ["Block:Modes:0"] = "sentinel-mode",
            ["Block:Delays:0"] = "5",
            ["Block:Codes:0"] = "sentinel-number",
        });

        Assert.Contains($"Custom[x].Modes[0]: must be one of {string.Join(", ", Enum.GetNames<OrderingMode>())}", failures);
        Assert.Contains("Custom[x].Delays[0]: must be a duration in hh:mm:ss form", failures);
        Assert.Contains("Custom[x].Codes[0]: must be an integer", failures);
        Assert.All(failures, f => Assert.DoesNotContain("sentinel", f, StringComparison.Ordinal));
        Assert.Equal(["ok"], bound.Names!);
        Assert.Null(bound.Codes);
    }

    private static (ListOptions Bound, List<string> Failures) BindLists(Dictionary<string, string?> settings)
    {
        var section = new ConfigurationBuilder().AddInMemoryCollection(settings).Build().GetSection("Block");
        var failures = new List<string>();
        return (RelayOptionsBinder.BindSection<ListOptions>(section, "Custom[x]", failures), failures);
    }

    private sealed class ListOptions
    {
        public string? Name { get; set; }
        public string[]? Names { get; set; }
        public List<int>? Codes { get; set; }
        public IReadOnlyList<OrderingMode>? Modes { get; set; }
        public TimeSpan[]? Delays { get; set; }
        public ListOptions? Nested { get; set; }
    }

    [Fact]
    public void Each_destination_keeps_its_raw_element_as_settings()
    {
        var options = Bind(new()
        {
            ["Destinations:0:Name"] = "A",
            ["Destinations:0:Kafka:Topic"] = "a-topic",
            ["Destinations:1:Name"] = "B",
            ["Destinations:1:Kafka:Topic"] = "b-topic",
        });

        Assert.Collection(
            options.Destinations,
            d => Assert.Equal("a-topic", d.Settings!["Kafka:Topic"]),
            d => Assert.Equal("b-topic", d.Settings!["Kafka:Topic"]));
    }

    [Fact]
    public void Destination_binds_name_and_transport_as_given()
    {
        var options = Bind(new()
        {
            ["Destinations:0:Name"] = "OrdersTopic",
            ["Destinations:0:Transport"] = "kAFKA",
            ["Destinations:1:Transport"] = "Sqs",
        });

        Assert.Collection(
            options.Destinations,
            d => Assert.Equal(("OrdersTopic", "kAFKA"), (d.Name, d.Transport)),
            d => Assert.Equal((null, "Sqs"), (d.Name, d.Transport)));
    }

    [Theory]
    [InlineData("Destinations:0:Kafak:Topic")]
    [InlineData("Destinations:0:Database:Table")]
    [InlineData("Destinations:0:Settings")]
    public void Destination_keys_are_not_checked_by_the_binder(string key)
    {
        var failures = Failures(new()
        {
            ["Destinations:0:Name"] = "OrdersTopic",
            [key] = "x",
        });

        Assert.DoesNotContain(failures, f => f.Contains("unknown setting", StringComparison.Ordinal));
    }

    [Fact]
    public void Exception_message_lists_every_failure()
    {
        var ex = new RelayStartupValidationException(["a: required", "b: required"]);

        Assert.Equal("Relay startup validation failed (2):\n- a: required\n- b: required", ex.Message);
        Assert.Equal(["a: required", "b: required"], ex.Failures);
    }

    [Fact]
    public void Core_does_not_depend_on_npgsql_or_dapper()
    {
        var depsPath = Path.Combine(AppContext.BaseDirectory, $"{typeof(RelayOptionsBinderTests).Assembly.GetName().Name}.deps.json");
        using var deps = JsonDocument.Parse(File.ReadAllText(depsPath));

        var core = deps.RootElement.GetProperty("targets").EnumerateObject().First().Value.EnumerateObject()
            .Single(lib => lib.Name.StartsWith("Pernix.OutboxRelay.Core/", StringComparison.Ordinal));
        var dependencies = core.Value.TryGetProperty("dependencies", out var d)
            ? d.EnumerateObject().Select(p => p.Name).ToArray()
            : [];

        Assert.DoesNotContain(dependencies, n => n.StartsWith("Npgsql", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(dependencies, n => n.StartsWith("Dapper", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(typeof(RelayOptions).Assembly.GetReferencedAssemblies(),
            a => a.Name is "Npgsql" or "Dapper");
    }
}
