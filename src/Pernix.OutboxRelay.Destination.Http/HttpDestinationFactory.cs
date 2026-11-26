using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Destination.Http.Options;
using Microsoft.Extensions.Logging;

namespace Pernix.OutboxRelay.Destination.Http;

public sealed class HttpDestinationFactory : IDestinationFactory
{
    private const string OwnSetting = "Http";

    private readonly ILogger logger;
    private readonly HttpCredentialSection credentials;

    internal HttpDestinationFactory(ILoggerFactory loggerFactory, HttpCredentialSection credentials)
    {
        ArgumentNullException.ThrowIfNull(loggerFactory);
        ArgumentNullException.ThrowIfNull(credentials);

        this.credentials = credentials;
        logger = loggerFactory.CreateLogger<HttpDestinationFactory>();
    }

    public string Transport => "Http";

    public IReadOnlyList<string> Validate(DestinationOptions destination, string path, RelayOptions relay)
    {
        ArgumentNullException.ThrowIfNull(destination);
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(relay);

        var failures = new List<string>();
        if (destination.Settings is { } settings)
        {
            foreach (var child in settings.GetChildren())
            {
                if (!string.Equals(child.Key, OwnSetting, StringComparison.OrdinalIgnoreCase)
                    && !DestinationOptions.CoreKeys.Contains(child.Key, StringComparer.OrdinalIgnoreCase))
                {
                    failures.Add($"{path}: unknown setting '{child.Key}'");
                }
            }

            _ = RelayOptionsBinder.BindSection<HttpDestinationOptions>(settings.GetSection(OwnSetting), $"{path}.{OwnSetting}", failures);
        }

        return failures;
    }

    public IDestination Create(DestinationOptions destination, DestinationContext context) =>
        throw new NotImplementedException("Http destination creation is not implemented yet.");
}
