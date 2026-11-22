using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Runtime.CompilerServices;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Microsoft.Extensions.Configuration;

[assembly: InternalsVisibleTo("Pernix.OutboxRelay.Unit.Tests")]
[assembly: InternalsVisibleTo("Pernix.OutboxRelay.Integration.Tests")]
[assembly: InternalsVisibleTo("Pernix.OutboxRelay.Benchmarks")]

namespace Pernix.OutboxRelay.Outbox.PostgreSql;

public sealed class PostgreSqlOutboxStoreProvider(RelayConfiguration configuration) : IOutboxStoreProvider
{
    public const string ProviderName = "PostgreSql";

    private static readonly IReadOnlySet<OrderingMode> Modes =
        new[] { OrderingMode.None, OrderingMode.PerKey, OrderingMode.Strict }.ToFrozenSet();

    private readonly ConcurrentDictionary<TableIdentity, string> tableOwners = new();
    private readonly ConcurrentDictionary<LockKeyIdentity, string> lockKeyOwners = new();

    public string Name => ProviderName;

    public IReadOnlySet<OrderingMode> SupportedModes => Modes;

    public IOutboxStore CreateStore(OutboxOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var name = options.Name ?? throw new InvalidOperationException("Outbox Name is required.");
        var table = options.Database?.Table ?? throw new InvalidOperationException($"Outboxes[{name}].Database.Table is required.");
        var connectionName = options.Database.Connection
            ?? throw new InvalidOperationException($"Outboxes[{name}].Database.Connection is required.");

        var configured = configuration.Section.GetConnectionString(connectionName);
        if (string.IsNullOrEmpty(configured))
        {
            throw new InvalidOperationException($"Outboxes[{name}].Database.Connection: connection '{connectionName}' is not configured.");
        }

        string session;
        string validation;
        string stats;
        try
        {
            session = PostgreSqlConnectionStrings.ForSession(configured);
            validation = PostgreSqlConnectionStrings.ForValidation(configured);
            stats = PostgreSqlConnectionStrings.ForStats(configured, name);
        }
        catch (Exception)
        {
            // Npgsql's parse errors quote fragments of the string, which may hold secrets. No inner exception.
            throw new OutboxValidationException($"Outboxes[{name}].Database.Connection: connection string is not valid");
        }

        var mode = options.Ordering?.Mode ?? throw new InvalidOperationException($"Outboxes[{name}].Ordering.Mode is required.");
        return new PostgreSqlOutboxStore(name, table, mode, session, validation, stats, tableOwners, lockKeyOwners);
    }
}
