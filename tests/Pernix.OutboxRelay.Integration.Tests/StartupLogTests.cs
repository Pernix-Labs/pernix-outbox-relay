using System.Collections.Concurrent;
using Pernix.OutboxRelay.Integration.Tests.Kafka;
using Pernix.OutboxRelay.Integration.Tests.PostgreSql;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests;

public sealed class StartupLogTests(PostgreSqlFixture postgres, KafkaFixture kafka)
{
    private const string RelayStartingEventName = "RelayStarting";

    [Fact]
    public async Task Relay_starting_is_logged_with_identity()
    {
        var provider = new CapturingLoggerProvider();
        await using var relay = await new RelayFactory().WithOutboxAsync(postgres, kafka);
        await using var factory = relay.WithWebHostBuilder(b => b.ConfigureTestServices(s =>
            s.AddSingleton<ILoggerProvider>(provider)));

        using var client = factory.CreateClient();

        var entry = Assert.Single(provider.Entries, e => e.EventId.Name == RelayStartingEventName);
        Assert.Equal("outbox-relay", entry.State["ComponentName"]);
    }

    private sealed record Entry(EventId EventId, IReadOnlyDictionary<string, object?> State);

    private sealed class CapturingLoggerProvider : ILoggerProvider
    {
        public ConcurrentQueue<Entry> Entries { get; } = new();

        public ILogger CreateLogger(string categoryName) => new CapturingLogger(Entries);

        public void Dispose()
        {
        }
    }

    private sealed class CapturingLogger(ConcurrentQueue<Entry> entries) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            var values = state as IEnumerable<KeyValuePair<string, object?>> ?? [];
            entries.Enqueue(new Entry(eventId, values.ToDictionary(kv => kv.Key, kv => kv.Value)));
        }
    }
}
