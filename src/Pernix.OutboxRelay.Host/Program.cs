using Pernix.OutboxRelay.Core;
using Pernix.OutboxRelay.Host.Configuration;
using Pernix.OutboxRelay.Host.Health;
using Pernix.OutboxRelay.Host.Logging;
using Pernix.OutboxRelay.Host.Telemetry;
using Pernix.OutboxRelay.Kafka;
using Pernix.OutboxRelay.PostgreSql;

using var bootstrapLoggerFactory = LoggingRegistration.CreateBootstrapLoggerFactory();
var bootstrapLogger = bootstrapLoggerFactory.CreateLogger("Pernix.OutboxRelay.Host");

try
{
    var builder = WebApplication.CreateBuilder(args);

    var identity = RelayIdentity.From(builder.Configuration);

    builder.Services.Configure<HostOptions>(builder.Configuration.GetSection("HostOptions"));
    builder.AddRelayLogging();
    builder.Services.AddOutboxRelayCore().AddPostgreSqlOutboxProvider().AddKafkaTransport();
    builder.AddRelayHealth();
    builder.AddRelayTelemetry(identity);

    var app = builder.Build();
    app.MapRelayHealth();

    app.Logger.RelayStarting(
        identity.ServiceName,
        identity.Version,
        identity.Commit,
        identity.EnvironmentName,
        identity.ComponentName);

    await app.RunAsync();
    return 0;
}
// WebApplicationFactory stops the host with HostAbortedException. It must pass through.
catch (Exception ex) when (ex is not HostAbortedException)
{
    bootstrapLogger.StartupFailed(ex);
    return 1;
}

public partial class Program;
