using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Leadership;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Pernix.OutboxRelay.Core.Configuration;

// Runs in StartingAsync, so a failure aborts host start before Kestrel listens and before any worker runs.
public sealed class RelayStartupValidator(
    RelayConfiguration configuration,
    IEnumerable<IOutboxStoreProvider> providers,
    IEnumerable<IDestinationFactory> factories,
    ValidatedOutboxes validatedOutboxes,
    IOptions<HostOptions> hostOptions,
    ILogger<RelayStartupValidator> logger) : IHostedLifecycleService
{
    // The leader closes its session by T0 + 27 s, so the host must wait at least 1 s longer.
    internal static readonly TimeSpan MinShutdownTimeout = OutboxWorker.StopCloseDeadline + TimeSpan.FromSeconds(1);

    public async Task StartingAsync(CancellationToken cancellationToken)
    {
        var providersByName = new Dictionary<string, IOutboxStoreProvider>(StringComparer.OrdinalIgnoreCase);
        foreach (var provider in providers)
        {
            if (!providersByName.TryAdd(provider.Name, provider))
            {
                throw new InvalidOperationException($"Outbox store provider '{provider.Name}' is registered more than once.");
            }
        }

        var resolver = new DestinationFactoryResolver(factories);

        var options = RelayOptionsBinder.Bind(configuration.Section);
        var failures = RelayConfigurationValidator.Validate(options, configuration.Section.GetConnectionString, providersByName, resolver)
            .Select(Prefixed)
            .ToList();
        if (hostOptions.Value.ShutdownTimeout < MinShutdownTimeout)
        {
            failures.Add(
                $"HostOptions.ShutdownTimeout: must be at least {MinShutdownTimeout:c} to cover the {OutboxWorker.StopCloseDeadline.TotalSeconds:0} s stop budget");
        }

        if (failures.Count > 0)
        {
            throw new RelayStartupValidationException(failures);
        }

        // No early return: configured destinations are still probed with zero outboxes.
        if (options.Outboxes.Count == 0)
        {
            logger.NoOutboxesConfigured();
        }

        var storeFailures = new List<string>();
        var stores = new List<(OutboxOptions Options, IValidatedOutboxStore Store)>();
        foreach (var outbox in options.Outboxes)
        {
            try
            {
                var store = providersByName[outbox.Database!.Provider!].CreateStore(outbox);
                var validated = await store.ValidateAsync(cancellationToken).ConfigureAwait(false);
                stores.Add((outbox, validated));
            }
            catch (Exception ex) when (ex is OutboxValidationException or OutboxDatabaseUnreachableException)
            {
                storeFailures.Add(Prefixed(ex.Message));
            }
        }

        if (storeFailures.Count > 0)
        {
            throw new RelayStartupValidationException(storeFailures);
        }

        var destinationFailures = await Task.WhenAll(options.Destinations.Select(d => ProbeAsync(d, resolver.Get(d), cancellationToken)))
            .ConfigureAwait(false);
        if (destinationFailures.OfType<string>().Select(Prefixed).ToList() is { Count: > 0 } probeFailures)
        {
            throw new RelayStartupValidationException(probeFailures);
        }

        var destinationsByName = options.Destinations.ToDictionary(d => d.Name!, StringComparer.OrdinalIgnoreCase);
        foreach (var (outbox, store) in stores)
        {
            var destination = destinationsByName[outbox.Destination!];
            DestinationBinding? deadLetterDestination = null;
            if (outbox.DeadLetterDestinationName is { } deadLetterDestinationName)
            {
                var deadLetterOptions = destinationsByName[deadLetterDestinationName];
                deadLetterDestination = new DestinationBinding(deadLetterOptions, resolver.Get(deadLetterOptions));
            }

            validatedOutboxes.Add(
                new ValidatedOutbox(outbox, store, destination, resolver.Get(destination)) { DeadLetterDestination = deadLetterDestination });
        }

        // Bounds and routes the delivery term will use: built exactly as OutboxWorker.NewTerm builds them.
        var boundFailures = new List<string>();
        foreach (var outbox in validatedOutboxes.Entries)
        {
            var context = new DestinationContext(outbox.Options.Ordering?.Mode ?? default);
            await CheckTermDestinationAsync(
                    outbox.Options.Name!, outbox.Destination, outbox.DestinationFactory, context, deadLetter: false, boundFailures)
                .ConfigureAwait(false);
            if (outbox.DeadLetterDestination is { } deadLetter)
            {
                await CheckTermDestinationAsync(outbox.Options.Name!, deadLetter.Options, deadLetter.Factory, context, deadLetter: true, boundFailures)
                    .ConfigureAwait(false);
            }
        }

        if (boundFailures.Count > 0)
        {
            throw new RelayStartupValidationException(boundFailures.Select(Prefixed).ToList());
        }
    }

    // Failure paths are section-relative; the prefix names the section they came from. HostOptions lines are not relay keys.
    private static string Prefixed(string failure) => RelayConfiguration.SectionName + "." + failure;

    private static async Task CheckTermDestinationAsync(
        string outbox, DestinationOptions destination, IDestinationFactory factory, DestinationContext context, bool deadLetter, List<string> failures)
    {
        var instance = factory.Create(destination, context);
        try
        {
            if (instance.SendBound is { } bound && bound <= TimeSpan.Zero)
            {
                failures.Add($"Outboxes[{outbox}]: destination '{destination.Name}' declares SendBound {bound:c}; must be greater than zero");
            }

            CheckRoutes(outbox, destination.Name, instance.Routes, deadLetter, failures);
        }
        finally
        {
            await instance.DisposeAsync().ConfigureAwait(false);
        }
    }

    // Keys come from config, never row data.
    private static void CheckRoutes(string outbox, string? destination, IReadOnlySet<string>? routes, bool deadLetter, List<string> failures)
    {
        if (routes is null)
        {
            return;
        }

        if (deadLetter)
        {
            failures.Add(
                $"Outboxes[{outbox}].DeadLetter.Destination: destination '{destination}' declares Routes. A dead-letter destination has one address.");
            return;
        }

        if (routes.Count == 0)
        {
            failures.Add(
                $"Outboxes[{outbox}]: destination '{destination}' declares an empty Routes set; Routes is null (one address) or names at least one route");
            return;
        }

        foreach (var key in routes)
        {
            if (!RouteKeyRule.IsValid(key))
            {
                var shown = key is null ? "route key (null)" : $"route key '{key}'";
                failures.Add($"Outboxes[{outbox}]: destination '{destination}' declares {shown}; must match {RouteKeyRule.Pattern}");
            }
        }
    }

    private static async Task<string?> ProbeAsync(DestinationOptions destination, IDestinationFactory factory, CancellationToken cancellationToken)
    {
        // Yield first so a factory that blocks in Create does not serialize the other probes.
        await Task.Yield();
        var probe = factory.Create(destination, DestinationContext.Probe);
        try
        {
            await probe.ValidateAsync(cancellationToken).ConfigureAwait(false);
            return null;
        }
        catch (DestinationValidationException ex)
        {
            return ex.Message;
        }
        finally
        {
            await probe.DisposeAsync().ConfigureAwait(false);
        }
    }

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StoppingAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StoppedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
