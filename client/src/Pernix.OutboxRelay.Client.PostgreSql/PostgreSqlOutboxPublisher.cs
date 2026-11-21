using System.Data;
using System.Data.Common;
using System.Transactions;
using Npgsql;
using NpgsqlTypes;

namespace Pernix.OutboxRelay.Client.PostgreSql;

/// <summary>
/// Writes outbox rows to one PostgreSQL outbox table with raw Npgsql commands, inside the caller's transaction.
/// Builds its SQL once at construction, keeps no state after that and is safe for concurrent use from many
/// transactions. Register one instance per outbox table.
/// </summary>
/// <remarks>
/// Write-order rule: call a publish method after the business writes of the same transaction. A message published
/// before the business writes can reach consumers ahead of the state it describes.
/// The insert names only <c>message_key</c>, <c>payload</c>, and, when present, <c>headers</c> and <c>route</c>. It
/// reads nothing back, so the connecting role needs only <c>INSERT</c> on the table. Database errors are not caught,
/// wrapped or logged.
/// A route-scoped publisher (registered with
/// <see cref="PostgreSqlOutboxServiceCollectionExtensions.AddKeyedPostgreSqlOutboxPublisher(Microsoft.Extensions.DependencyInjection.IServiceCollection, object, string, string)"/>)
/// writes its route on a message without a route, writes a message with the same route as is, and throws
/// <see cref="ArgumentException"/> for a message with another route before anything is sent.
/// </remarks>
public sealed class PostgreSqlOutboxPublisher : IOutboxPublisher
{
    private readonly string insertSql;
    private readonly string insertWithHeadersSql;
    private readonly string insertWithRouteSql;
    private readonly string insertWithHeadersAndRouteSql;
    private readonly string? scopedRoute;

    /// <summary>Creates a publisher for <paramref name="table"/> and builds its INSERT statements.</summary>
    /// <param name="table">The outbox table, created with <see cref="OutboxTable.Parse"/>.</param>
    /// <exception cref="ArgumentException"><paramref name="table"/> is <c>default(OutboxTable)</c>.</exception>
    public PostgreSqlOutboxPublisher(OutboxTable table)
    {
        var tableText = table.ToString();
        if (tableText.Length == 0)
        {
            throw new ArgumentException("Outbox table is default(OutboxTable). Create it with OutboxTable.Parse.", nameof(table));
        }

        Table = table;

        // Positional parameters: Npgsql sends the text as is, without rewriting named placeholders.
        insertSql = $"INSERT INTO {tableText} ({OutboxColumns.MessageKey}, {OutboxColumns.Payload}) VALUES ($1, $2)";
        insertWithHeadersSql = $"INSERT INTO {tableText} ({OutboxColumns.MessageKey}, {OutboxColumns.Payload}, {OutboxColumns.Headers}) VALUES ($1, $2, $3)";
        insertWithRouteSql = $"INSERT INTO {tableText} ({OutboxColumns.MessageKey}, {OutboxColumns.Payload}, {OutboxColumns.Route}) VALUES ($1, $2, $3)";
        insertWithHeadersAndRouteSql = $"INSERT INTO {tableText} ({OutboxColumns.MessageKey}, {OutboxColumns.Payload}, {OutboxColumns.Headers}, {OutboxColumns.Route}) VALUES ($1, $2, $3, $4)";
    }

    internal PostgreSqlOutboxPublisher(OutboxTable table, string route)
        : this(TableAfterRouteCheck(table, route))
    {
        scopedRoute = route;
    }

    /// <inheritdoc />
    public OutboxTable Table { get; }

    /// <summary>
    /// Inserts one outbox row in <paramref name="transaction"/>, on its connection. The row commits or rolls back with
    /// the transaction.
    /// </summary>
    /// <remarks>
    /// Write-order rule: call this after the business writes of the same transaction. The insert runs now. The method
    /// never commits, rolls back or disposes the transaction.
    /// </remarks>
    /// <param name="transaction">The open <see cref="NpgsqlTransaction"/> that holds the business writes.</param>
    /// <param name="message">The message to write.</param>
    /// <param name="ct">Cancels the insert. After cancellation the caller decides what happens to the transaction.</param>
    /// <returns>A task that completes when the row is inserted.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="transaction"/> is null.</exception>
    /// <exception cref="InvalidOperationException">
    /// <paramref name="transaction"/> is not an <see cref="NpgsqlTransaction"/>, or it is already completed.
    /// </exception>
    /// <exception cref="ArgumentException">The publisher is route-scoped and <paramref name="message"/> has a different route.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="ct"/> is cancelled.</exception>
    public async Task PublishAsync(DbTransaction transaction, OutboxMessage message, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        var npgsqlTransaction = AsNpgsqlTransaction(transaction);
        var connection = ConnectionOf(npgsqlTransaction);
        ct.ThrowIfCancellationRequested();
        var route = RouteFor(message, -1);

        await using var command = new NpgsqlCommand(SqlFor(route, message), connection, npgsqlTransaction);
        AddParameters(command.Parameters, message, route);
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Inserts one outbox row per message in <paramref name="transaction"/>, as one <see cref="NpgsqlBatch"/> in one
    /// round trip. Row ids ascend in list order. An empty list returns without contacting the database.
    /// </summary>
    /// <remarks>
    /// Write-order rule: call this after the business writes of the same transaction. The batch has no size cap: a
    /// large batch holds the caller's transaction open and all its commands and parameters in memory until the round
    /// trip completes. The batch size is the caller's choice.
    /// </remarks>
    /// <param name="transaction">The open <see cref="NpgsqlTransaction"/> that holds the business writes.</param>
    /// <param name="messages">The messages to write, in delivery order.</param>
    /// <param name="ct">Cancels the insert. After cancellation the caller decides what happens to the transaction.</param>
    /// <returns>A task that completes when the rows are inserted.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="transaction"/> or <paramref name="messages"/> is null.</exception>
    /// <exception cref="InvalidOperationException">
    /// <paramref name="transaction"/> is not an <see cref="NpgsqlTransaction"/>, or it is already completed.
    /// </exception>
    /// <exception cref="ArgumentException">The publisher is route-scoped and a message in <paramref name="messages"/> has a different route.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="ct"/> is cancelled.</exception>
    public async Task PublishAsync(DbTransaction transaction, IReadOnlyList<OutboxMessage> messages, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentNullException.ThrowIfNull(messages);
        var npgsqlTransaction = AsNpgsqlTransaction(transaction);
        var connection = ConnectionOf(npgsqlTransaction);
        if (messages.Count == 0)
        {
            return;
        }

        ct.ThrowIfCancellationRequested();

        await using var batch = new NpgsqlBatch(connection, npgsqlTransaction);
        AddCommands(batch, messages);
        await batch.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Inserts one outbox row on <paramref name="connection"/>, in whatever transaction the connection is in. Requires
    /// an ambient <see cref="TransactionScope"/> (<see cref="Transaction.Current"/>).
    /// </summary>
    /// <remarks>
    /// Open the connection inside the scope with enlistment on (<c>Enlist=true</c>, the Npgsql default), and create the
    /// scope with <see cref="TransactionScopeAsyncFlowOption.Enabled"/>. The publisher never enlists the connection
    /// itself: the insert runs in the same
    /// transaction as the business writes already sent on that connection. A connection opened before the scope is
    /// not enlisted, so its insert autocommits like its business writes. Without a scope, pass the
    /// <see cref="DbTransaction"/> to <see cref="PublishAsync(DbTransaction, OutboxMessage, CancellationToken)"/>.
    /// Write-order rule: call this after the business writes of the same scope.
    /// </remarks>
    /// <param name="connection">The open <see cref="NpgsqlConnection"/>, opened inside the scope, that holds the business writes.</param>
    /// <param name="message">The message to write.</param>
    /// <param name="ct">Cancels the insert. After cancellation the caller decides what happens to the scope.</param>
    /// <returns>A task that completes when the row is inserted.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="connection"/> is null.</exception>
    /// <exception cref="InvalidOperationException">
    /// <paramref name="connection"/> is not an <see cref="NpgsqlConnection"/>, no ambient transaction exists, or the
    /// connection is not open.
    /// </exception>
    /// <exception cref="ArgumentException">The publisher is route-scoped and <paramref name="message"/> has a different route.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="ct"/> is cancelled.</exception>
    public async Task PublishEnlistedAsync(DbConnection connection, OutboxMessage message, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        var npgsqlConnection = AsEnlistedConnection(connection);
        ct.ThrowIfCancellationRequested();
        var route = RouteFor(message, -1);

        await using var command = new NpgsqlCommand(SqlFor(route, message), npgsqlConnection);
        AddParameters(command.Parameters, message, route);
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Inserts one outbox row per message on <paramref name="connection"/>, as one <see cref="NpgsqlBatch"/> in one
    /// round trip, in whatever transaction the connection is in. Requires an ambient <see cref="TransactionScope"/>.
    /// Row ids ascend in list order. An empty list returns without contacting the database.
    /// </summary>
    /// <remarks>
    /// Open the connection inside the scope with enlistment on (<c>Enlist=true</c>, the Npgsql default), and create the
    /// scope with <see cref="TransactionScopeAsyncFlowOption.Enabled"/>. The publisher never enlists the connection
    /// itself. Without a scope, pass the
    /// <see cref="DbTransaction"/> to <see cref="PublishAsync(DbTransaction, IReadOnlyList{OutboxMessage}, CancellationToken)"/>.
    /// Write-order rule: call this after the business writes of the same scope. The batch has no size cap: a large
    /// batch holds the caller's transaction open and all its commands and parameters in memory until the round trip
    /// completes.
    /// </remarks>
    /// <param name="connection">The open <see cref="NpgsqlConnection"/>, opened inside the scope, that holds the business writes.</param>
    /// <param name="messages">The messages to write, in delivery order.</param>
    /// <param name="ct">Cancels the insert. After cancellation the caller decides what happens to the scope.</param>
    /// <returns>A task that completes when the rows are inserted.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="messages"/> is null.</exception>
    /// <exception cref="InvalidOperationException">
    /// <paramref name="connection"/> is not an <see cref="NpgsqlConnection"/>, no ambient transaction exists, or the
    /// connection is not open.
    /// </exception>
    /// <exception cref="ArgumentException">The publisher is route-scoped and a message in <paramref name="messages"/> has a different route.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="ct"/> is cancelled.</exception>
    public async Task PublishEnlistedAsync(DbConnection connection, IReadOnlyList<OutboxMessage> messages, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(messages);
        var npgsqlConnection = AsEnlistedConnection(connection);
        if (messages.Count == 0)
        {
            return;
        }

        ct.ThrowIfCancellationRequested();

        await using var batch = new NpgsqlBatch(npgsqlConnection);
        AddCommands(batch, messages);
        await batch.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private static NpgsqlTransaction AsNpgsqlTransaction(DbTransaction transaction) =>
        transaction as NpgsqlTransaction
        ?? throw new InvalidOperationException(
            $"Transaction type '{transaction.GetType().FullName}' is not supported. PostgreSqlOutboxPublisher requires an NpgsqlTransaction.");

    private static NpgsqlConnection ConnectionOf(NpgsqlTransaction transaction)
    {
        var connection = transaction.Connection ?? throw TransactionCompleted(null);

        // Npgsql (8.0.3 to 10.x) keeps Connection after Commit/Rollback, and 8.x would run the insert in autocommit.
        // Its IsolationLevel getter throws once the transaction is completed.
        try
        {
            _ = transaction.IsolationLevel;
        }
        catch (InvalidOperationException exception) when (exception is not ObjectDisposedException)
        {
            throw TransactionCompleted(exception);
        }

        return connection;
    }

    private static InvalidOperationException TransactionCompleted(Exception? inner) =>
        new("The transaction is completed. Publish inside an open transaction.", inner);

    private static NpgsqlConnection AsEnlistedConnection(DbConnection connection)
    {
        var npgsqlConnection = connection as NpgsqlConnection
            ?? throw new InvalidOperationException(
                $"Connection type '{connection.GetType().FullName}' is not supported. PostgreSqlOutboxPublisher requires an NpgsqlConnection.");

        if (Transaction.Current is null)
        {
            throw new InvalidOperationException("No ambient TransactionScope. Pass the DbTransaction to PublishAsync.");
        }

        if (npgsqlConnection.State != ConnectionState.Open)
        {
            throw new InvalidOperationException(
                $"The connection is {npgsqlConnection.State}. Open it inside the TransactionScope before publishing.");
        }

        return npgsqlConnection;
    }

    private static OutboxTable TableAfterRouteCheck(OutboxTable table, string route)
    {
        RouteRules.Validate(route, nameof(route));
        return table;
    }

    internal string? RouteFor(in OutboxMessage message, int index) =>
        scopedRoute is null ? message.Route : RouteRules.Resolve(message.Route, scopedRoute, index);

    internal string SqlFor(string? route, in OutboxMessage message) => route is null
        ? (message.Headers.Count > 0 ? insertWithHeadersSql : insertSql)
        : (message.Headers.Count > 0 ? insertWithHeadersAndRouteSql : insertWithRouteSql);

    private void AddCommands(NpgsqlBatch batch, IReadOnlyList<OutboxMessage> messages)
    {
        var commands = batch.BatchCommands;
        for (var i = 0; i < messages.Count; i++)
        {
            var message = messages[i];
            var route = RouteFor(message, i);
            var command = new NpgsqlBatchCommand(SqlFor(route, message));
            AddParameters(command.Parameters, message, route);
            commands.Add(command);
        }
    }

    private static void AddParameters(NpgsqlParameterCollection parameters, in OutboxMessage message, string? route)
    {
        // ReadOnlyMemory<byte> is written as bytea from the caller's memory, without a copy to a new array.
        var key = message.Key;
        parameters.Add(key.HasValue
            ? new NpgsqlParameter<ReadOnlyMemory<byte>> { NpgsqlDbType = NpgsqlDbType.Bytea, TypedValue = key.GetValueOrDefault() }
            : new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bytea, Value = DBNull.Value });
        parameters.Add(new NpgsqlParameter<ReadOnlyMemory<byte>> { NpgsqlDbType = NpgsqlDbType.Bytea, TypedValue = message.Payload });

        // Empty headers use the statement without the column, so the DDL default '{}' applies and no JSON is sent.
        var headers = message.Headers;
        if (headers.Count > 0)
        {
            parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Jsonb, TypedValue = headers.Json });
        }

        if (route is not null)
        {
            parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = route });
        }
    }
}
