using System.Collections.Concurrent;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Destination.Http.Options;
using Microsoft.Extensions.Logging;

namespace Pernix.OutboxRelay.Destination.Http;

public sealed class HttpDestinationFactory : IDestinationFactory
{
    private const string OwnSetting = "Http";

    private readonly ConcurrentDictionary<string, HttpDestinationPlan> resolved = new(StringComparer.OrdinalIgnoreCase);
    private readonly ILogger logger;
    private readonly HttpCredentialSection credentials;
    private readonly HttpHookLookup hooks;

    internal HttpDestinationFactory(ILoggerFactory loggerFactory, HttpCredentialSection credentials)
        : this(loggerFactory, credentials, HttpHookLookup.None)
    {
    }

    internal HttpDestinationFactory(ILoggerFactory loggerFactory, HttpCredentialSection credentials, HttpHookLookup hooks)
    {
        ArgumentNullException.ThrowIfNull(loggerFactory);
        ArgumentNullException.ThrowIfNull(credentials);
        ArgumentNullException.ThrowIfNull(hooks);

        this.credentials = credentials;
        this.hooks = hooks;
        logger = loggerFactory.CreateLogger<HttpDestinationFactory>();
    }

    public string Transport => "Http";

    public IReadOnlyList<string> Validate(DestinationOptions destination, string path, RelayOptions relay)
    {
        ArgumentNullException.ThrowIfNull(destination);
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(relay);

        var failures = new List<string>();
        var http = new HttpDestinationOptions();
        var ownPath = $"{path}.{OwnSetting}";
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

            http = RelayOptionsBinder.BindSection<HttpDestinationOptions>(settings.GetSection(OwnSetting), ownPath, failures);
        }

        var name = destination.Name ?? string.Empty;
        var outboxes = relay.Outboxes
            .Where(o => string.Equals(o.Destination, name, StringComparison.OrdinalIgnoreCase)
                        || string.Equals(o.DeadLetterDestinationName, name, StringComparison.OrdinalIgnoreCase))
            .Select(o => o.Name)
            .OfType<string>()
            .ToArray();

        var compileFailures = new List<string>();
        var plan = HttpPlanCompiler.Compile(http, name, ownPath, outboxes, credentials, hooks, CreateTokenCache, compileFailures);

        // A rejected value falls back to its default, so rules on the same key group would report a bogus failure.
        var bindGroups = failures.Select(f => Group(f, ownPath)).OfType<string>().ToHashSet(StringComparer.Ordinal);
        failures.AddRange(compileFailures.Where(f => Group(f, ownPath) is not { } group || !bindGroups.Contains(group)));

        if (failures.Count == 0 && plan is not null && !string.IsNullOrWhiteSpace(destination.Name))
        {
            resolved.AddOrUpdate(destination.Name, plan, (_, previous) =>
            {
                previous.Auth.TokenCache?.Dispose();
                return plan;
            });
        }
        else
        {
            plan?.Auth.TokenCache?.Dispose();
        }

        return failures;

        OAuth2TokenCache CreateTokenCache(AuthenticationOptions options, HttpCredential credential, HandlerSettings handler) =>
            new(name, options, credential, handler, logger);
    }

    public IDestination Create(DestinationOptions destination, DestinationContext context) =>
        throw new NotImplementedException("Http destination creation is not implemented yet.");

    internal HttpDestinationPlan PlanOf(string destination) => resolved[destination];

    private static string? Group(string failure, string ownPath)
    {
        if (!failure.StartsWith(ownPath + ".", StringComparison.Ordinal))
        {
            return null;
        }

        var rest = failure.AsSpan(ownPath.Length + 1);
        var end = rest.IndexOfAny('.', ':', '[');
        return end < 0 ? rest.ToString() : rest[..end].ToString();
    }
}
