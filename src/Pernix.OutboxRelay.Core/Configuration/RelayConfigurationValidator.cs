using Pernix.OutboxRelay.Core.Abstractions;

namespace Pernix.OutboxRelay.Core.Configuration;

public static class RelayConfigurationValidator
{
    internal const int MaxBatchSize = 1000;

    private static readonly StringComparer NameComparer = StringComparer.OrdinalIgnoreCase;

    public static IReadOnlyList<string> Validate(
        RelayOptions options,
        Func<string, string?> getConnectionString,
        IReadOnlyDictionary<string, IOutboxStoreProvider> providers,
        DestinationFactoryResolver factories)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(getConnectionString);
        ArgumentNullException.ThrowIfNull(providers);
        ArgumentNullException.ThrowIfNull(factories);

        var failures = new List<string>();
        var destinations = options.Destinations.Select((d, i) => (Options: d, Path: PathOf("Destinations", d.Name, i))).ToList();
        var outboxes = options.Outboxes.Select((o, i) => (Options: o, Path: PathOf("Outboxes", o.Name, i))).ToList();

        foreach (var (d, path) in destinations)
        {
            Required(failures, $"{path}.Name", d.Name);
            Required(failures, $"{path}.Transport", d.Transport);
        }

        foreach (var (o, path) in outboxes)
        {
            Required(failures, $"{path}.Name", o.Name);
            Required(failures, $"{path}.Destination", o.Destination);
            RequiredDatabase(failures, $"{path}.Database", o.Database, "required");
            RequiredEnum(failures, $"{path}.Ordering.Mode", o.Ordering?.Mode);
            if (o.Ordering is { } ordering && !Enum.IsDefined(ordering.OnPermanentFailure))
            {
                failures.Add($"{path}.Ordering.OnPermanentFailure: {OneOf<OnPermanentFailure>()}");
            }
        }

        foreach (var (o, path) in outboxes)
        {
            if (o.Polling.BatchSize < 1)
            {
                failures.Add($"{path}.Polling.BatchSize: must be at least 1");
            }

            // Every held id goes into the read's exclude list, which PostgreSQL 13 and 14 scan linearly per row.
            if (o.Polling.BatchSize > MaxBatchSize)
            {
                failures.Add($"{path}.Polling.BatchSize: must be at most {MaxBatchSize}");
            }

            if (o.Polling.Interval <= TimeSpan.Zero)
            {
                failures.Add($"{path}.Polling.Interval: must be greater than 00:00:00");
            }

            if (o.Retry.MaxRetryAttempts < 0)
            {
                failures.Add($"{path}.Retry.MaxRetryAttempts: must be at least 0");
            }

            if (o.Retry.InitialDelay <= TimeSpan.Zero)
            {
                failures.Add($"{path}.Retry.InitialDelay: must be greater than 00:00:00");
            }

            if (!(o.Retry.BackoffMultiplier >= 1.0))
            {
                failures.Add($"{path}.Retry.BackoffMultiplier: must be at least 1.0");
            }

            if (!HasBindingFailure(options, $"{path}.Retry") && o.Retry.MaxDelay < o.Retry.InitialDelay)
            {
                failures.Add($"{path}.Retry.MaxDelay: must be at least Retry.InitialDelay");
            }
        }

        Duplicates(failures, "Destinations", options.Destinations.Select(d => d.Name));
        Duplicates(failures, "Outboxes", options.Outboxes.Select(o => o.Name));

        var destinationNames = new HashSet<string>(options.Destinations.Select(d => d.Name).Where(HasValue).Select(n => n!), NameComparer);
        foreach (var (o, path) in outboxes)
        {
            if (HasValue(o.Destination) && !destinationNames.Contains(o.Destination!))
            {
                failures.Add($"{path}.Destination: destination '{o.Destination}' is not defined in Destinations");
            }

            if (o.DeadLetterDestinationName is { } deadLetter && !destinationNames.Contains(deadLetter))
            {
                failures.Add($"{path}.DeadLetter.Destination: destination '{deadLetter}' is not defined in Destinations");
            }

            DatabaseReferences(failures, $"{path}.Database", o.Database, getConnectionString, providers);
        }

        foreach (var (d, path) in destinations)
        {
            if (factories.Resolve(d, path, failures) is not { } factory)
            {
                continue;
            }

            failures.AddRange(factory.Validate(d, path, options));
        }

        foreach (var (o, path) in outboxes)
        {
            if (!HasBindingFailure(options, $"{path}.Ordering") && o.Ordering is { OnPermanentFailure: OnPermanentFailure.Block, Mode: OrderingMode.None })
            {
                failures.Add($"{path}.Ordering.OnPermanentFailure: Block is not allowed when Ordering.Mode is None");
            }
        }

        foreach (var (o, path) in outboxes)
        {
            if (o.Ordering?.Mode is { } mode
                && !HasBindingFailure(options, $"{path}.Ordering")
                && Enum.IsDefined(mode)
                && HasValue(o.Database?.Provider)
                && providers.TryGetValue(o.Database!.Provider!, out var provider)
                && !provider.SupportedModes.Contains(mode))
            {
                failures.Add($"{path}.Ordering.Mode: provider '{provider.Name}' does not support '{mode}'");
            }
        }

        foreach (var (o, path) in outboxes)
        {
            if (o.DeadLetterDestinationName is { } deadLetter && NameComparer.Equals(deadLetter, o.Destination))
            {
                failures.Add($"{path}.DeadLetter.Destination: names the outbox's own destination '{o.Destination}'");
            }
        }

        // A setting the binder rejected is reported once, by the binder, not again as missing.
        var boundPaths = options.BindingFailures.Select(PathPart).ToHashSet(StringComparer.Ordinal);
        return [.. options.BindingFailures, .. failures.Where(f => !boundPaths.Contains(PathPart(f)))];
    }

    // A rejected value falls back to its default, so cross-field checks on its group would report a bogus failure.
    private static bool HasBindingFailure(RelayOptions options, string group) =>
        options.BindingFailures.Any(f =>
            f.StartsWith(group, StringComparison.Ordinal) && f.Length > group.Length && f[group.Length] is '.' or ':');

    private static string PathPart(string failure) => failure[..failure.IndexOf(": ", StringComparison.Ordinal)];

    internal static string PathOf(string section, string? name, int index) =>
        HasValue(name) ? $"{section}[{name}]" : $"{section}[{index}]";

    private static bool HasValue(string? value) => !string.IsNullOrWhiteSpace(value);

    private static string OneOf<TEnum>()
        where TEnum : struct, Enum =>
        $"must be one of {string.Join(", ", Enum.GetNames<TEnum>())}";

    private static void Required(List<string> failures, string path, string? value, string requirement = "required")
    {
        if (!HasValue(value))
        {
            failures.Add($"{path}: {requirement}");
        }
    }

    private static void RequiredDatabase(List<string> failures, string path, DatabaseOptions? database, string requirement)
    {
        Required(failures, $"{path}.Provider", database?.Provider, requirement);
        Required(failures, $"{path}.Connection", database?.Connection, requirement);
        Required(failures, $"{path}.Table", database?.Table, requirement);
    }

    private static void RequiredEnum<TEnum>(List<string> failures, string path, TEnum? value)
        where TEnum : struct, Enum
    {
        if (value is not { } v)
        {
            failures.Add($"{path}: required, one of {string.Join(", ", Enum.GetNames<TEnum>())}");
        }
        else if (!Enum.IsDefined(v))
        {
            failures.Add($"{path}: {OneOf<TEnum>()}");
        }
    }

    private static void Duplicates(List<string> failures, string section, IEnumerable<string?> names)
    {
        var seen = new Dictionary<string, int>(NameComparer);
        var index = 0;
        foreach (var name in names)
        {
            if (HasValue(name) && !seen.TryAdd(name!, index))
            {
                failures.Add($"{section}[{index}].Name: '{name}' duplicates {section}[{seen[name!]}] (names are case-insensitive)");
            }

            index++;
        }
    }

    private static void DatabaseReferences(
        List<string> failures,
        string path,
        DatabaseOptions? database,
        Func<string, string?> getConnectionString,
        IReadOnlyDictionary<string, IOutboxStoreProvider> providers)
    {
        if (HasValue(database?.Connection) && string.IsNullOrWhiteSpace(getConnectionString(database!.Connection!)))
        {
            failures.Add($"{path}.Connection: connection string '{database.Connection}' is not defined or empty in OutboxRelay.ConnectionStrings");
        }

        if (HasValue(database?.Provider) && !providers.ContainsKey(database!.Provider!))
        {
            failures.Add($"{path}.Provider: provider '{database.Provider}' is not registered");
        }
    }
}
