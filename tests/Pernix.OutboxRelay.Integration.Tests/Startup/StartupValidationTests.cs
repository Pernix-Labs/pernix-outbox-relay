using System.Collections.Concurrent;
using System.Net;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Integration.Tests.Kafka;
using Pernix.OutboxRelay.Integration.Tests.PostgreSql;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests.Startup;

public sealed class StartupValidationTests(PostgreSqlFixture postgres, KafkaFixture kafka)
{
    private const string UnreachableConnection = "Host=127.0.0.1;Port=1;Username=relay;Password=unused;Database=relay";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    // Overrides are "key=value", or "key" alone for a null value. {outbox} is replaced by the test's outbox name.
    [Theory]
    [InlineData("OutboxRelay.Outboxes[{outbox}].Ordering.Mode: required, one of None, PerKey, Strict", "Outboxes:0:Ordering:Mode")]
    [InlineData("OutboxRelay.Destinations[Events].Transport: required", "Destinations:0:Transport")]
    [InlineData("OutboxRelay.Destinations[Events].Transport: transport 'Sqs' is not registered", "Destinations:0:Transport=Sqs")]
    [InlineData("OutboxRelay.Outboxes[{outbox}].Ordering.Mode: must be one of None, PerKey, Strict", "Outboxes:0:Ordering:Mode=Sideways")]
    [InlineData("OutboxRelay.Outboxes[{outbox}].Polling.BatchSize: must be at least 1", "Outboxes:0:Polling:BatchSize=0")]
    [InlineData("OutboxRelay.Outboxes[{outbox}].Retry.MaxDelay: must be at least Retry.InitialDelay", "Outboxes:0:Retry:MaxDelay=00:00:01")]
    [InlineData(
        "OutboxRelay.Outboxes[{outbox}].Ordering.OnPermanentFailure: Block is not allowed when Ordering.Mode is None",
        "Outboxes:0:Ordering:Mode=None",
        "Outboxes:0:Ordering:OnPermanentFailure=Block")]
    [InlineData(
        "OutboxRelay.Outboxes[{outbox}].DeadLetter.Destination: names the outbox's own destination 'Events'",
        "Outboxes:0:DeadLetter:Destination=events")]
    [InlineData(
        "OutboxRelay.Outboxes[{outbox}].Destination: destination 'Nowhere' is not defined in Destinations",
        "Outboxes:0:Destination=Nowhere")]
    [InlineData(
        "OutboxRelay.Outboxes[{outbox}].DeadLetter.Destination: destination 'Nowhere' is not defined in Destinations",
        "Outboxes:0:DeadLetter:Destination=Nowhere")]
    [InlineData(
        "OutboxRelay.Destinations[Events].Kafka.Cluster: Kafka cluster 'Other' is not defined in KafkaClusters",
        "Destinations:0:Kafka:Cluster=Other")]
    [InlineData(
        "OutboxRelay.Outboxes[{outbox}].Database.Connection: connection string 'Other' is not defined or empty in OutboxRelay.ConnectionStrings",
        "Outboxes:0:Database:Connection=Other")]
    [InlineData(
        "OutboxRelay.Outboxes[{outbox}].Database.Provider: provider 'SqlServer' is not registered",
        "Outboxes:0:Database:Provider=SqlServer")]
    [InlineData("OutboxRelay.Destinations[Events].Kafka: Topic or Routes is required when Transport is Kafka", "Destinations:0:Kafka:Topic")]
    [InlineData("OutboxRelay.Destinations[Events].Kafka: Topic and Routes are both set; set exactly one", "Destinations:0:Kafka:Routes:alerts=risk.alerts")]
    [InlineData(
        "OutboxRelay.Destinations[Events].Kafka.Routes.alerts: topic is required",
        "Destinations:0:Kafka:Topic",
        "Destinations:0:Kafka:Routes:alerts=")]
    [InlineData("OutboxRelay.Destinations[Events]: unknown setting 'Database'", "Destinations:0:Database:Table=public.dead_letter")]
    public async Task Invalid_configuration_refuses_to_start(string expected, params string[] overrides)
    {
        var outbox = RelayFactory.NewOutboxName();
        var settings = RelayFactory.OutboxSettings(postgres.ConnectionString, kafka.BootstrapServers, (outbox, PostgreSqlFixture.NewSchema()));
        foreach (var entry in overrides)
        {
            var parts = entry.Split('=', 2);
            settings[parts[0]] = parts.Length == 2 ? parts[1] : null;
        }

        var failures = await StartFailuresAsync(settings);

        Assert.Contains(failures, f => f.StartsWith(expected.Replace("{outbox}", outbox, StringComparison.Ordinal), StringComparison.Ordinal));
    }

    [Fact]
    public async Task Duplicate_outbox_name_refuses_to_start()
    {
        var outbox = RelayFactory.NewOutboxName();
        var duplicate = outbox.ToUpperInvariant();
        var settings = RelayFactory.OutboxSettings(
            postgres.ConnectionString,
            kafka.BootstrapServers,
            (outbox, await postgres.CreateOutboxAsync()),
            (duplicate, await postgres.CreateOutboxAsync()));

        var failures = await StartFailuresAsync(settings);

        Assert.Contains($"OutboxRelay.Outboxes[1].Name: '{duplicate}' duplicates Outboxes[0] (names are case-insensitive)", failures);
    }

    [Fact]
    public async Task Duplicate_destination_name_refuses_to_start()
    {
        var settings = RelayFactory.OutboxSettings(postgres.ConnectionString, kafka.BootstrapServers, (RelayFactory.NewOutboxName(), await postgres.CreateOutboxAsync()));
        settings["Destinations:1:Name"] = "EVENTS";
        settings["Destinations:1:Transport"] = "Kafka";
        settings["Destinations:1:Kafka:Cluster"] = "Main";
        settings["Destinations:1:Kafka:Topic"] = "other";

        var failures = await StartFailuresAsync(settings);

        Assert.Contains("OutboxRelay.Destinations[1].Name: 'EVENTS' duplicates Destinations[0] (names are case-insensitive)", failures);
    }

    [Fact]
    public async Task Missing_table_refuses_to_start()
    {
        var outbox = RelayFactory.NewOutboxName();
        var schema = PostgreSqlFixture.NewSchema();
        await postgres.ExecuteAsync($"CREATE SCHEMA {schema}");

        var failures = await StartFailuresAsync(RelayFactory.OutboxSettings(postgres.ConnectionString, kafka.BootstrapServers, (outbox, schema)));

        Assert.Contains($"OutboxRelay.Outboxes[{outbox}]: table '{schema}.outbox' not found", failures);
    }

    [Fact]
    public async Task Missing_column_refuses_to_start()
    {
        var outbox = RelayFactory.NewOutboxName();
        var schema = await postgres.CreateOutboxAsync();
        await postgres.ExecuteAsync($"ALTER TABLE {schema}.outbox DROP COLUMN last_error");

        var failures = await StartFailuresAsync(RelayFactory.OutboxSettings(postgres.ConnectionString, kafka.BootstrapServers, (outbox, schema)));

        Assert.Contains($"OutboxRelay.Outboxes[{outbox}]: table '{schema}.outbox' missing columns: last_error", failures);
    }

    [Fact]
    public async Task Missing_route_column_refuses_to_start()
    {
        var outbox = RelayFactory.NewOutboxName();
        var schema = await postgres.CreateOutboxAsync();
        await postgres.ExecuteAsync($"ALTER TABLE {schema}.outbox DROP COLUMN route");

        var failures = await StartFailuresAsync(RelayFactory.OutboxSettings(postgres.ConnectionString, kafka.BootstrapServers, (outbox, schema)));

        Assert.Contains($"OutboxRelay.Outboxes[{outbox}]: table '{schema}.outbox' missing columns: route", failures);
    }

    [Fact]
    public async Task Column_type_mismatch_refuses_to_start()
    {
        var outbox = RelayFactory.NewOutboxName();
        var schema = await postgres.CreateOutboxAsync();
        await postgres.ExecuteAsync($"ALTER TABLE {schema}.outbox ALTER COLUMN created_at TYPE timestamp");

        var failures = await StartFailuresAsync(RelayFactory.OutboxSettings(postgres.ConnectionString, kafka.BootstrapServers, (outbox, schema)));

        Assert.Contains(
            $"OutboxRelay.Outboxes[{outbox}]: table '{schema}.outbox' columns differ from V001: created_at timestamp without time zone NOT NULL (expected timestamp with time zone NOT NULL)",
            failures);
    }

    [Fact]
    public async Task Same_table_twice_refuses_to_start()
    {
        var first = RelayFactory.NewOutboxName();
        var second = RelayFactory.NewOutboxName();
        var schema = await postgres.CreateOutboxAsync();

        var failures = await StartFailuresAsync(RelayFactory.OutboxSettings(postgres.ConnectionString, kafka.BootstrapServers, (first, schema), (second, schema)));

        Assert.Contains($"OutboxRelay.Outboxes[{second}]: table '{schema}.outbox' same table as outbox '{first}'", failures);
    }

    [Fact]
    public async Task Unreachable_database_refuses_to_start()
    {
        var outbox = RelayFactory.NewOutboxName();

        var failures = await StartFailuresAsync(RelayFactory.OutboxSettings(UnreachableConnection, kafka.BootstrapServers, (outbox, "public")));

        var failure = Assert.Single(failures);
        Assert.StartsWith($"OutboxRelay.Outboxes[{outbox}]: database unreachable (", failure, StringComparison.Ordinal);
        Assert.DoesNotContain("unused", failure, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Refusals_are_reported_together_with_other_failures()
    {
        var outbox = RelayFactory.NewOutboxName();
        var settings = RelayFactory.OutboxSettings(postgres.ConnectionString, kafka.BootstrapServers, (outbox, PostgreSqlFixture.NewSchema()));
        settings["Outboxes:0:Polling:BatchSize"] = "0";
        settings["Outboxes:0:Ordering:Mode"] = "Strict";
        settings["Outboxes:0:DeadLetter:Destination"] = "Events";
        settings["Destinations:1:Name"] = "Archive";
        settings["Destinations:1:Transport"] = "Database";

        var failures = await StartFailuresAsync(settings);

        Assert.Equal(
            [
                $"OutboxRelay.Outboxes[{outbox}].Polling.BatchSize: must be at least 1",
                "OutboxRelay.Destinations[Archive].Transport: transport 'Database' is not registered",
                $"OutboxRelay.Outboxes[{outbox}].DeadLetter.Destination: names the outbox's own destination 'Events'",
            ],
            failures);
    }

    [Fact]
    public async Task Missing_topic_refuses_to_start_naming_destination_and_topic()
    {
        var settings = RelayFactory.OutboxSettings(postgres.ConnectionString, kafka.BootstrapServers, (RelayFactory.NewOutboxName(), await postgres.CreateOutboxAsync()));
        var topic = KafkaFixture.NewTopic();
        settings["Destinations:0:Kafka:Topic"] = topic;

        var failures = await StartFailuresAsync(settings);

        Assert.Equal([$"OutboxRelay.Destinations[Events]: topic '{topic}' does not exist"], failures);
    }

    [Fact]
    public async Task Missing_topic_exits_with_code_1()
    {
        var settings = RelayFactory.OutboxSettings(postgres.ConnectionString, kafka.BootstrapServers, (RelayFactory.NewOutboxName(), await postgres.CreateOutboxAsync()));
        settings["Destinations:0:Kafka:Topic"] = KafkaFixture.NewTopic();

        Assert.Equal(1, await RunProgramAsync(settings));
    }

    [Fact]
    public async Task Column_type_mismatch_exits_with_code_1()
    {
        var schema = await postgres.CreateOutboxAsync();
        await postgres.ExecuteAsync($"ALTER TABLE {schema}.outbox ALTER COLUMN created_at TYPE timestamp");
        var settings = RelayFactory.OutboxSettings(postgres.ConnectionString, kafka.BootstrapServers, (RelayFactory.NewOutboxName(), schema));

        Assert.Equal(1, await RunProgramAsync(settings));
    }

    [Fact]
    public async Task Refused_configuration_exits_with_code_1()
    {
        var settings = RelayFactory.OutboxSettings(postgres.ConnectionString, kafka.BootstrapServers, (RelayFactory.NewOutboxName(), PostgreSqlFixture.NewSchema()));
        settings["Outboxes:0:Polling:BatchSize"] = "0";

        Assert.Equal(1, await RunProgramAsync(settings));
    }

    [Fact]
    public async Task Block_with_None_ordering_exits_with_code_1()
    {
        var settings = RelayFactory.OutboxSettings(postgres.ConnectionString, kafka.BootstrapServers, (RelayFactory.NewOutboxName(), PostgreSqlFixture.NewSchema()));
        settings["Outboxes:0:Ordering:OnPermanentFailure"] = "Block";

        Assert.Equal(1, await RunProgramAsync(settings));
    }

    [Fact]
    public async Task Kafka_dead_letter_topic_starts_and_is_ready()
    {
        var topic = KafkaFixture.NewTopic();
        await kafka.CreateTopicAsync(topic, 1);
        var settings = RelayFactory.OutboxSettings(postgres.ConnectionString, kafka.BootstrapServers, (RelayFactory.NewOutboxName(), await postgres.CreateOutboxAsync()));
        AddDeadLetterDestination(settings, topic);
        await using var factory = new RelayFactory().WithRelaySettings(settings);
        using var client = factory.CreateClient();

        using var response = await client.GetAsync("/readyz", Ct);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task Missing_dead_letter_topic_refuses_to_start_naming_destination_and_topic()
    {
        var topic = KafkaFixture.NewTopic();
        var settings = RelayFactory.OutboxSettings(postgres.ConnectionString, kafka.BootstrapServers, (RelayFactory.NewOutboxName(), await postgres.CreateOutboxAsync()));
        AddDeadLetterDestination(settings, topic);

        var failures = await StartFailuresAsync(settings);

        Assert.Equal([$"OutboxRelay.Destinations[DeadLetters]: topic '{topic}' does not exist"], failures);
    }

    [Fact]
    public async Task Missing_dead_letter_topic_exits_with_code_1()
    {
        var settings = RelayFactory.OutboxSettings(postgres.ConnectionString, kafka.BootstrapServers, (RelayFactory.NewOutboxName(), await postgres.CreateOutboxAsync()));
        AddDeadLetterDestination(settings, KafkaFixture.NewTopic());

        Assert.Equal(1, await RunProgramAsync(settings));
    }

    [Fact]
    public async Task Empty_relay_section_refuses_to_start()
    {
        var failures = await StartFailuresAsync(new Dictionary<string, string?>());

        Assert.Equal(["OutboxRelay.Outboxes: at least one outbox is required"], failures);
    }

    // The bootstrap logger writes StartupFailed as JSON to stdout, without the event name. Other tests' console lines may interleave, so match on content.
    [Fact]
    public async Task Empty_relay_section_exits_with_code_1_and_logs_startup_failed()
    {
        var original = Console.Out;
        using var output = new StringWriter();
        Console.SetOut(TextWriter.Synchronized(output));
        int exitCode;
        try
        {
            exitCode = await RunProgramAsync(new Dictionary<string, string?>());
        }
        finally
        {
            Console.SetOut(original);
        }

        Assert.Equal(1, exitCode);
        Assert.Contains(
            output.ToString().Split('\n'),
            line => line.Contains("\"Message\":\"Relay startup failed\"", StringComparison.Ordinal)
                && line.Contains("OutboxRelay.Outboxes: at least one outbox is required", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Valid_configuration_starts_and_is_ready()
    {
        var settings = RelayFactory.OutboxSettings(postgres.ConnectionString, kafka.BootstrapServers, (RelayFactory.NewOutboxName(), await postgres.CreateOutboxAsync()));
        await using var factory = new RelayFactory().WithRelaySettings(settings);
        using var client = factory.CreateClient();

        using var response = await client.GetAsync("/readyz", Ct);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Theory]
    [InlineData("PerKey", "Continue")]
    [InlineData("PerKey", "Block")]
    [InlineData("Strict", "Continue")]
    [InlineData("Strict", "Block")]
    public async Task Ordered_configuration_starts_and_is_ready(string mode, string onPermanentFailure)
    {
        var settings = RelayFactory.OutboxSettings(postgres.ConnectionString, kafka.BootstrapServers, (RelayFactory.NewOutboxName(), await postgres.CreateOutboxAsync()));
        settings["Outboxes:0:Ordering:Mode"] = mode;
        settings["Outboxes:0:Ordering:OnPermanentFailure"] = onPermanentFailure;
        await using var factory = new RelayFactory().WithRelaySettings(settings);
        using var client = factory.CreateClient();

        using var response = await client.GetAsync("/readyz", Ct);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    private static void AddDeadLetterDestination(IDictionary<string, string?> settings, string topic)
    {
        settings["Destinations:1:Name"] = "DeadLetters";
        settings["Destinations:1:Transport"] = "Kafka";
        settings["Destinations:1:Kafka:Cluster"] = "Main";
        settings["Destinations:1:Kafka:Topic"] = topic;
        settings["Outboxes:0:DeadLetter:Destination"] = "DeadLetters";
    }

    // Runs the real entry point, so the result is the process exit code.
    private static async Task<int> RunProgramAsync(IDictionary<string, string?> settings)
    {
        string[] args =
        [
            .. settings.Where(kv => kv.Value is not null).Select(kv => $"--{RelayFactory.RelayKey(kv.Key)}={kv.Value}"),
        ];
        return await Task.Run(() => Assert.IsType<int>(typeof(Program).Assembly.EntryPoint!.Invoke(null, [args])), Ct);
    }

    // Program's catch turns the start exception into StartupFailed + exit 1, so it never reaches the factory.
    // The generic host logs the exception it failed to start with; capture it from there.
    private static async Task<IReadOnlyList<string>> StartFailuresAsync(IDictionary<string, string?> settings, Action<IServiceCollection>? services = null) =>
        Assert.Single((await StartExceptionsAsync(settings, services)).OfType<RelayStartupValidationException>()).Failures;

    private static async Task<IReadOnlyCollection<Exception>> StartExceptionsAsync(IDictionary<string, string?> settings, Action<IServiceCollection>? services = null)
    {
        var exceptions = new ExceptionCapture();
        var factory = new RelayFactory()
            .WithRelaySettings(settings)
            .WithServices(s => s.AddSingleton<ILoggerProvider>(exceptions));
        if (services is not null)
        {
            factory = factory.WithServices(services);
        }

        Assert.NotNull(Record.Exception(() => factory.CreateClient()));
        await factory.DisposeAsync();

        return exceptions.Exceptions;
    }

    private sealed class ExceptionCapture : ILoggerProvider, ILogger
    {
        public ConcurrentQueue<Exception> Exceptions { get; } = new();

        public ILogger CreateLogger(string categoryName) => this;

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (exception is not null)
            {
                Exceptions.Enqueue(exception);
            }
        }

        public void Dispose()
        {
        }
    }
}
