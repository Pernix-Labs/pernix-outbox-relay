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
    private readonly ILoggerFactory loggerFactory;
    private readonly ILogger logger;
    private readonly HttpCredentialSection credentials;
    private readonly HttpHooks hooks;
    private readonly Func<HandlerSettings, HttpMessageHandler>? handlers;
    private readonly TimeProvider? time;

    internal HttpDestinationFactory(ILoggerFactory loggerFactory, HttpCredentialSection credentials)
        : this(loggerFactory, credentials, HttpHooks.None)
    {
    }

    internal HttpDestinationFactory(ILoggerFactory loggerFactory, HttpCredentialSection credentials, HttpHooks hooks)
        : this(loggerFactory, credentials, hooks, null, null)
    {
    }

    // handlers and time are test seams: one handler per destination instance and per token cache.
    internal HttpDestinationFactory(
        ILoggerFactory loggerFactory,
        HttpCredentialSection credentials,
        HttpHookLookup hooks,
        Func<HandlerSettings, HttpMessageHandler>? handlers = null,
        TimeProvider? time = null)
        : this(loggerFactory, credentials, HttpHooks.From(hooks ?? throw new ArgumentNullException(nameof(hooks))), handlers, time)
    {
    }

    internal HttpDestinationFactory(
        ILoggerFactory loggerFactory,
        HttpCredentialSection credentials,
        HttpHooks hooks,
        Func<HandlerSettings, HttpMessageHandler>? handlers,
        TimeProvider? time)
    {
        ArgumentNullException.ThrowIfNull(loggerFactory);
        ArgumentNullException.ThrowIfNull(credentials);
        ArgumentNullException.ThrowIfNull(hooks);

        this.credentials = credentials;
        this.hooks = hooks;
        this.loggerFactory = loggerFactory;
        this.handlers = handlers;
        this.time = time;
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
        var plan = HttpPlanCompiler.Compile(http, name, ownPath, outboxes, credentials, hooks.Lookup, CreateTokenCache, compileFailures);

        // A rejected value falls back to its default: rules on that key, and scheme-dependent rules under a
        // rejected Scheme, would report a failure the operator did not cause.
        var bindKeys = failures.Select(f => KeyOf(f, ownPath)).OfType<string>().ToHashSet(StringComparer.OrdinalIgnoreCase);
        failures.AddRange(compileFailures.Where(f => !Cascades(f)));

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

        bool Cascades(string failure)
        {
            if (KeyOf(failure, ownPath) is not { } key)
            {
                return false;
            }

            if (bindKeys.Contains(key))
            {
                return true;
            }

            var parent = key.LastIndexOf('.');
            return parent > 0
                   && bindKeys.Contains($"{key[..parent]}.Scheme")
                   && failure.Contains(": does not apply to Scheme ", StringComparison.Ordinal);
        }

        OAuth2TokenCache CreateTokenCache(AuthenticationOptions options, HttpCredential credential, HandlerSettings handler) =>
            handlers is null
                ? new(name, options, credential, handler, logger, time)
                : new(name, options, credential, handlers(handler), logger, time);
    }

    public IDestination Create(DestinationOptions destination, DestinationContext context)
    {
        ArgumentNullException.ThrowIfNull(destination);
        ArgumentNullException.ThrowIfNull(context);

        if (destination.Name is not { } name || !resolved.TryGetValue(name, out var plan))
        {
            throw new InvalidOperationException($"Destination '{destination.Name}' was not validated by the Http transport.");
        }

        var mapper = plan.RequestMapper is { } mapperName
            ? hooks.Mapper(mapperName) ?? throw new InvalidOperationException($"Destinations[{name}]: request mapper '{mapperName}' is not registered.")
            : null;
        var classifier = plan.ResponseClassifier is { } classifierName
            ? hooks.Classifier(classifierName) ?? throw new InvalidOperationException($"Destinations[{name}]: response classifier '{classifierName}' is not registered.")
            : null;
        return new HttpDestination(plan, context, loggerFactory.CreateLogger<HttpDestination>(), handlers, time, mapper, classifier);
    }

    internal HttpDestinationPlan PlanOf(string destination) => resolved[destination];

    private static string? KeyOf(string failure, string ownPath)
    {
        if (!failure.StartsWith(ownPath + ".", StringComparison.Ordinal))
        {
            return null;
        }

        var rest = failure.AsSpan(ownPath.Length + 1);
        var end = rest.IndexOf(": ", StringComparison.Ordinal);
        return end < 0 ? null : rest[..end].ToString();
    }
}
