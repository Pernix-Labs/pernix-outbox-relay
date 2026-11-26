using System.Collections.Concurrent;
using Pernix.OutboxRelay.Integration.Tests.Http;
using Pernix.OutboxRelay.Integration.Tests.Kafka;
using Pernix.OutboxRelay.Integration.Tests.PostgreSql;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests;

public sealed class LogSecrecyTests(PostgreSqlFixture postgres, KafkaFixture kafka)
{
    private const string SecretKey = "Secret:Sentinel";

    [Fact]
    public async Task Configuration_value_never_appears_in_logs()
    {
        var sentinel = Guid.NewGuid().ToString("N");
        var provider = new CapturingLoggerProvider();
        await using var relay = await new RelayFactory().WithOutboxAsync(postgres, kafka);
        await using var factory = relay.WithWebHostBuilder(b => b
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

    [Fact]
    public async Task Http_credentials_templated_urls_and_row_data_never_appear_in_logs()
    {
        var token = Secret();
        var signingSecret = Secret();
        var apiKey = Secret();
        var pathLiteral = Secret();
        var queryLiteral = Secret();
        var headerLiteral = Secret();
        var rowKey = Secret();
        var rowHeader = Secret();
        var payload = Secret();
        string[] sentinels = [token, signingSecret, apiKey, pathLiteral, queryLiteral, headerLiteral, rowKey, rowHeader, payload];

        await using var endpoint = await HttpTestEndpoint.StartAsync();
        var mainCalls = 0;
        var deadLetterCalls = 0;
        endpoint.Respond = r => r.Path.StartsWith("/main/", StringComparison.Ordinal)
            ? new EndpointResponse(Interlocked.Increment(ref mainCalls) == 1 ? 503 : 422)
            : new EndpointResponse(Interlocked.Increment(ref deadLetterCalls) == 1 ? 503 : 200);

        var setup = await HttpOutbox.CreateAsync(postgres, url: endpoint.Url($"/main/{pathLiteral}/{{key}}?sig={queryLiteral}&id={{id}}"));
        setup.Http("Headers:X-Tenant", $"{headerLiteral}-{{header:tenant}}");
        setup.Http("Authentication:Scheme", "Bearer");
        setup.Http("Authentication:Credential", "Main");
        setup.Http("Signing:Scheme", "HmacSha256");
        setup.Http("Signing:Credential", "Signing");
        setup.Http("Signing:HeaderName", "X-Signature");
        setup.DeadLetterTo("Http", new Dictionary<string, string?>
        {
            ["Url"] = endpoint.Url($"/dead/{pathLiteral}?sig={queryLiteral}"),
            ["Authentication:Scheme"] = "ApiKey",
            ["Authentication:HeaderName"] = "X-Api-Key",
            ["Authentication:Credential"] = "DeadLetter",
        });
        setup.RootSettings["OutboxRelay:HttpCredentials:Main:Token"] = token;
        setup.RootSettings["OutboxRelay:HttpCredentials:Signing:SigningSecret"] = signingSecret;
        setup.RootSettings["OutboxRelay:HttpCredentials:DeadLetter:ApiKey"] = apiKey;
        setup.RootSettings["Logging:LogLevel:Default"] = "Trace";

        await setup.InsertAsync(new HttpRow(rowKey, payload, new Dictionary<string, string> { ["tenant"] = rowHeader }));
        await using var relay = setup.Relay();
        using var client = relay.CreateClient();
        Assert.True(await setup.DrainedAsync(TimeSpan.FromSeconds(60)), "row not dead-lettered");
        using var ready = await client.GetAsync("/readyz", TestContext.Current.CancellationToken);

        var main = endpoint.RequestsTo($"/main/{pathLiteral}/{rowKey}");
        Assert.Equal(2, main.Count);
        Assert.All(main, r => Assert.Equal($"Bearer {token}", r.Header("Authorization")));
        Assert.All(main, r => Assert.Equal($"{headerLiteral}-{rowHeader}", r.Header("X-Tenant")));
        Assert.All(main, r => Assert.NotNull(r.Header("X-Signature")));
        var dead = endpoint.RequestsTo($"/dead/{pathLiteral}");
        Assert.Equal(2, dead.Count);
        Assert.All(dead, r => Assert.Equal(apiKey, r.Header("X-Api-Key")));

        var entries = setup.Logs.Entries;
        Assert.Contains(entries, e => e.Message.Contains("HTTP 503", StringComparison.Ordinal));
        Assert.Contains(entries, e => e.Message.Contains("HTTP 422", StringComparison.Ordinal));
        Assert.All(entries, e => Assert.All(sentinels, sentinel =>
        {
            Assert.DoesNotContain(sentinel, e.Message, StringComparison.Ordinal);
            Assert.All(e.State.Values, v => Assert.DoesNotContain(sentinel, v, StringComparison.Ordinal));
        }));
    }

    private static string Secret() => Guid.NewGuid().ToString("N");

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
