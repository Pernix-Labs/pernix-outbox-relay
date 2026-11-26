using System.Globalization;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Destination.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;

namespace Pernix.OutboxRelay.Benchmarks.Infrastructure;

// Builds the real HttpDestination through HttpDestinationFactory (Validate, then Create) with an injected handler.
internal static class HttpMock
{
    public const string DeadLetterName = "bench-http-dlq";
    public const int SaturatedInFlight = 8;
    private const string StaticPath = "/orders";
    private const string TemplatedPath = "/orders/{id}?key={key}";

    public static readonly string[] UrlKinds = ["static", "tpl"];

    public static string Url(string host, string kind) => kind switch
    {
        "static" => $"http://{host}{StaticPath}",
        "tpl" => $"http://{host}{TemplatedPath}",
        _ => throw new InvalidOperationException($"http: url kind '{kind}' is not one of {string.Join(", ", UrlKinds)}"),
    };

    public static Func<HandlerSettings, HttpMessageHandler> Loopback(HttpLoopback server) => settings =>
    {
        var handler = HttpHandlerFactory.Create(settings);
        handler.ConnectCallback = server.ConnectAsync;
        return handler;
    };

    public static IDestination Create(OrderingMode mode, Func<HandlerSettings, HttpMessageHandler> handlers, string url, int? maxInFlight = null) =>
        Build(Workload.Destination, mode, handlers, url, maxInFlight, deadLetter: false);

    public static IDestination CreateDeadLetter(OrderingMode mode, Func<HandlerSettings, HttpMessageHandler> handlers, string url) =>
        Build(DeadLetterName, mode, handlers, url, maxInFlight: null, deadLetter: true);

    private static IDestination Build(
        string name, OrderingMode mode, Func<HandlerSettings, HttpMessageHandler> handlers, string url, int? maxInFlight, bool deadLetter)
    {
        var settings = new Dictionary<string, string?>
        {
            ["Destinations:0:Name"] = name,
            ["Destinations:0:Transport"] = "Http",
            ["Destinations:0:Http:Url"] = url,
            ["Outboxes:0:Name"] = Workload.Outbox,
            ["Outboxes:0:Destination"] = deadLetter ? Workload.Destination : name,
        };
        if (deadLetter)
        {
            settings["Outboxes:0:DeadLetter:Destination"] = name;
        }

        if (maxInFlight is { } limit)
        {
            settings["Destinations:0:Http:MaxRequestsInFlight"] = limit.ToString(CultureInfo.InvariantCulture);
        }

        var config = new ConfigurationBuilder().AddInMemoryCollection(settings).Build();
        var factory = new HttpDestinationFactory(
            NullLoggerFactory.Instance, new HttpCredentialSection(new RelayConfiguration(config)), HttpHooks.None, handlers, time: null);
        var relay = RelayOptionsBinder.Bind(config);
        var destination = relay.Destinations.Single();
        var failures = factory.Validate(destination, "Destinations[0]", relay);
        if (failures.Count != 0)
        {
            throw new InvalidOperationException($"http mock config rejected: {string.Join("; ", failures)}");
        }

        return factory.Create(destination, new DestinationContext(mode));
    }
}
