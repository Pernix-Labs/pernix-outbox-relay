using System.Net;
using Pernix.OutboxRelay.Integration.Tests.Kafka;
using Pernix.OutboxRelay.Integration.Tests.PostgreSql;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests;

public sealed class HealthEndpointTests(OutboxRelayFactory factory, PostgreSqlFixture postgres, KafkaFixture kafka) : IClassFixture<OutboxRelayFactory>
{
    [Theory]
    [InlineData("/livez")]
    [InlineData("/readyz")]
    public async Task Health_endpoint_returns_200_healthy(string path)
    {
        using var client = factory.CreateClient();

        using var response = await client.GetAsync(path, TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("Healthy", await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData("/livez")]
    [InlineData("/readyz")]
    public async Task Health_body_contains_no_configuration_value(string path)
    {
        using var client = factory.CreateClient();

        var body = await client.GetStringAsync(path, TestContext.Current.CancellationToken);

        Assert.DoesNotContain(RelayFactory.SentinelValue, body, StringComparison.Ordinal);
        Assert.DoesNotContain(RelayFactory.LayeringFileValue, body, StringComparison.Ordinal);
        Assert.DoesNotContain(RelayFactory.LayeringEnvValue, body, StringComparison.Ordinal);
    }

    // Own factory: stopping the shared fixture host would break the other tests.
    // The request runs inside the ApplicationStopping callback because the
    // in-memory host disposes its services as soon as StopApplication returns.
    [Fact]
    public async Task Readyz_returns_503_once_application_is_stopping()
    {
        await using var stoppingFactory = await new RelayFactory().WithOutboxAsync(postgres, kafka);
        using var client = stoppingFactory.CreateClient();
        var lifetime = stoppingFactory.Services.GetRequiredService<IHostApplicationLifetime>();

        HttpStatusCode? status = null;
        string? body = null;
        using var registration = lifetime.ApplicationStopping.Register(() =>
        {
            using var response = Task.Run(() => client.GetAsync("/readyz")).GetAwaiter().GetResult();
            status = response.StatusCode;
            body = Task.Run(() => response.Content.ReadAsStringAsync()).GetAwaiter().GetResult();
        });

        lifetime.StopApplication();

        Assert.Equal(HttpStatusCode.ServiceUnavailable, status);
        Assert.Equal("Unhealthy", body);
    }
}
