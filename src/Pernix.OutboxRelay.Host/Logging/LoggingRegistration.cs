using Microsoft.Extensions.Logging.Console;

namespace Pernix.OutboxRelay.Host.Logging;

public static class LoggingRegistration
{
    public static void ConfigureJsonConsole(JsonConsoleFormatterOptions options)
    {
        options.UseUtcTimestamp = true;
        options.TimestampFormat = "yyyy-MM-ddTHH:mm:ss.fffZ";
        options.IncludeScopes = true;
        options.JsonWriterOptions = options.JsonWriterOptions with { Indented = false };
    }

    public static WebApplicationBuilder AddRelayLogging(this WebApplicationBuilder builder)
    {
        builder.Logging.ClearProviders().AddJsonConsole(ConfigureJsonConsole);
        return builder;
    }

    public static ILoggerFactory CreateBootstrapLoggerFactory() =>
        LoggerFactory.Create(b => b.AddJsonConsole(ConfigureJsonConsole));
}
