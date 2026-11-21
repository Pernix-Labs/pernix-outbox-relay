using System.Collections.Concurrent;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests;

public sealed class LogSecrecyTests
{
    private const string SecretKey = "Secret:Sentinel";

    [Fact]
    public async Task Configuration_value_never_appears_in_logs()
    {
        var sentinel = Guid.NewGuid().ToString("N");
        var provider = new CapturingLoggerProvider();
        await using var factory = new RelayFactory().WithWebHostBuilder(b => b
            .UseSetting(SecretKey, sentinel)
            .ConfigureTestServices(s => s.AddSingleton<ILoggerProvider>(provider)));

        using var client = factory.CreateClient();
        using var live = await client.GetAsync("/livez", TestContext.Current.CancellationToken);
        using var ready = await client.GetAsync("/readyz", TestContext.Current.CancellationToken);

        Assert.Equal(sentinel, factory.Services.GetRequiredService<IConfiguration>()[SecretKey]);
        Assert.NotEmpty(provider.Entries);
        Assert.All(provider.Entries, e =>
        {
            Assert.DoesNotContain(sentinel, e.Message, StringComparison.Ordinal);
            Assert.All(e.StateValues, v => Assert.DoesNotContain(sentinel, v, StringComparison.Ordinal));
        });
    }

    private sealed record Entry(string Message, IReadOnlyList<string> StateValues);

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
            var stateValues = values.Select(kv => kv.Value?.ToString() ?? string.Empty).ToList();
            entries.Enqueue(new Entry(formatter(state, exception) + exception, stateValues));
        }
    }
}
