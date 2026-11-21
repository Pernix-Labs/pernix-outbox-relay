using Pernix.OutboxRelay.Integration.Tests.Kafka;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Pernix.OutboxRelay.Integration.Tests;

public class RelayFactory : WebApplicationFactory<Program>
{
    public const string HostEnvironmentName = "Testing";
    public const string SentinelKey = "RelayTests:Sentinel";
    public const string SentinelValue = "sentinel-from-appsettings-testing";
    public const string LayeringKey = "RelayTests:LayeringSentinel";
    public const string LayeringFileValue = "layering-from-appsettings-testing";
    public const string LayeringEnvValue = "layering-from-env";

    private const string TestSettingsFile = "appsettings.Testing.json";

    private readonly List<IDictionary<string, string?>> relaySettings = [];
    private readonly List<Action<IServiceCollection>> serviceOverrides = [];

    // Env layer is the real process environment, read by the host's default env source.
    // The key is unique to these tests, so setting it process-wide is harmless.
    static RelayFactory() =>
        Environment.SetEnvironmentVariable(LayeringKey.Replace(":", "__", StringComparison.Ordinal), LayeringEnvValue);

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment(HostEnvironmentName);
        builder.ConfigureAppConfiguration((_, config) =>
        {
            InsertTestSettings(config);
            foreach (var settings in relaySettings)
            {
                config.AddInMemoryCollection(settings);
            }
        });
        builder.ConfigureTestServices(services =>
        {
            services.AddSingleton<ILoggerProvider>(startErrors);
            foreach (var configure in serviceOverrides)
            {
                configure(services);
            }
        });
    }

    private readonly HostErrorCapture startErrors = new();

    // A failed start disposes the host inside Program, so the factory only reports ObjectDisposedException. Surface what the host logged.
    protected override IHost CreateHost(IHostBuilder builder)
    {
        try
        {
            var host = base.CreateHost(builder);
            _ = host.Services.GetService<IHostApplicationLifetime>();
            return host;
        }
        catch (Exception ex) when (!startErrors.Exceptions.IsEmpty)
        {
            throw new AggregateException("Relay host failed to start: " + string.Join(" | ", startErrors.Exceptions.Select(e => e.Message)), [.. startErrors.Exceptions, ex]);
        }
    }

    // Appended after every other source, so these settings win over files, env and command line.
    public RelayFactory WithRelaySettings(IDictionary<string, string?> settings)
    {
        relaySettings.Add(settings);
        return this;
    }

    public RelayFactory WithServices(Action<IServiceCollection> configure)
    {
        serviceOverrides.Add(configure);
        return this;
    }

    public const string DestinationName = "Events";

    // One Kafka destination on topic 'events' and one None PostgreSql outbox per entry, each on '<schema>.outbox' of connection 'Main'.
    public static Dictionary<string, string?> OutboxSettings(string connectionString, string bootstrapServers, params (string Name, string Schema)[] outboxes)
    {
        var settings = new Dictionary<string, string?>
        {
            ["ConnectionStrings:Main"] = connectionString,
            ["KafkaClusters:Main:BootstrapServers"] = bootstrapServers,
            ["Destinations:0:Name"] = DestinationName,
            ["Destinations:0:Transport"] = "Kafka",
            ["Destinations:0:Kafka:Cluster"] = "Main",
            ["Destinations:0:Kafka:Topic"] = KafkaFixture.EventsTopic,
        };

        for (var i = 0; i < outboxes.Length; i++)
        {
            var prefix = $"Outboxes:{i}:";
            settings[prefix + "Name"] = outboxes[i].Name;
            settings[prefix + "Destination"] = DestinationName;
            settings[prefix + "Database:Provider"] = "PostgreSql";
            settings[prefix + "Database:Connection"] = "Main";
            settings[prefix + "Database:Table"] = $"{outboxes[i].Schema}.outbox";
            settings[prefix + "Ordering:Mode"] = "None";
        }

        return settings;
    }

    public static string NewOutboxName() => $"o_{Guid.NewGuid():N}";

    // Test settings live in the test output, not the host content root.
    // Insert them at the appsettings.{env}.json position so env and command line still win.
    private static void InsertTestSettings(IConfigurationBuilder config)
    {
        var sources = config.Sources;
        var index = sources.Select((s, i) => (s, i))
            .Where(x => x.s is JsonConfigurationSource { Path: TestSettingsFile })
            .Select(x => x.i)
            .DefaultIfEmpty(-1)
            .First();

        if (index < 0)
        {
            throw new InvalidOperationException($"Host configuration has no '{TestSettingsFile}' source to anchor test settings.");
        }

        var testSettings = new JsonConfigurationSource
        {
            FileProvider = new PhysicalFileProvider(AppContext.BaseDirectory),
            Path = TestSettingsFile,
            Optional = false,
        };

        sources.Insert(index + 1, testSettings);
    }

    private sealed class HostErrorCapture : ILoggerProvider, ILogger
    {
        public System.Collections.Concurrent.ConcurrentQueue<Exception> Exceptions { get; } = new();

        public ILogger CreateLogger(string categoryName) => this;

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Error;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (exception is not null && logLevel >= LogLevel.Error)
            {
                Exceptions.Enqueue(exception);
            }
        }

        public void Dispose()
        {
        }
    }
}
