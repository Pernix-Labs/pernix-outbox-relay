# Outbox Relay producer client packages

Producer guide for the `Pernix.OutboxRelay.Client*` NuGet packages. They write outbox rows inside your transaction, so the relay delivers a message only when your business change commits. Read the sections in order: create the table, grant, pick a path, then the contracts.

Every C# example below is compiled against the package source (`client/tests/Pernix.OutboxRelay.Client.Docs.Unit.Tests`). Types the examples assume: `AppDb` and `SalesDb` are your `DbContext` types, `Order` and `Account` your entities, `OrderPlaced` and `AccountDebited` your event records, and `AppJson` your source-generated `JsonSerializerContext`.

## Which path do I use

| Your code holds | API | Install |
|---|---|---|
| ADO.NET or Dapper with an open `DbTransaction` | `publisher.PublishAsync(transaction, ...)` ([ADO.NET and Dapper](#adonet-and-dapper)) | `Pernix.OutboxRelay.Client.PostgreSql` |
| A `TransactionScope` | `publisher.PublishEnlistedAsync(connection, ...)` ([TransactionScope](#transactionscope)) | `Pernix.OutboxRelay.Client.PostgreSql` |
| EF Core with your own transaction, `ExecuteUpdate` or raw SQL | `publisher.PublishAsync(dbContext, ...)` ([EF Core: explicit transaction](#ef-core-explicit-transaction)) | `Pernix.OutboxRelay.Client.EntityFrameworkCore` and `Pernix.OutboxRelay.Client.PostgreSql` |
| EF Core `SaveChanges` unit of work, for example `EfUnitOfWork` | `UseOutbox` plus `IOutboxQueue<TContext>.Enqueue` or `EnqueueOutbox` ([EF Core queue](#ef-core-queue)) | `Pernix.OutboxRelay.Client.EntityFrameworkCore` and `Pernix.OutboxRelay.Client.PostgreSql` |
| Autocommit, no transaction | None. Wrap the publish in a transaction ([ADO.NET and Dapper](#adonet-and-dapper)). | |

## Packages

| Package | Contains | Direct dependencies (floor) |
|---|---|---|
| `Pernix.OutboxRelay.Client` | `OutboxMessage`, `OutboxHeaders`, `OutboxTable`, `OutboxColumns`, `IOutboxPublisher`. Namespace `Pernix.OutboxRelay.Client`. | None |
| `Pernix.OutboxRelay.Client.PostgreSql` | `PostgreSqlOutboxPublisher`, `AddPostgreSqlOutboxPublisher`, `AddKeyedPostgreSqlOutboxPublisher`, `PostgreSqlOutboxSchema`, `OutboxSchemaScript`. Namespace `Pernix.OutboxRelay.Client.PostgreSql`. | `Pernix.OutboxRelay.Client` `>=` the package's own version, `Npgsql` `8.0.3`, `Microsoft.Extensions.DependencyInjection.Abstractions` `8.0.0` |
| `Pernix.OutboxRelay.Client.EntityFrameworkCore` | `UseOutbox`, `AddOutboxQueue<TContext>`, `AddKeyedOutboxQueue<TContext>`, `IOutboxQueue<TContext>`, `EnqueueOutbox`, `PublishAsync(DbContext, ...)`. Namespace `Pernix.OutboxRelay.Client.EntityFrameworkCore`. | `Pernix.OutboxRelay.Client` `>=` the package's own version, `Microsoft.EntityFrameworkCore.Relational` `8.0.10` |

All three versions move together. NuGet resolves the provider's `Pernix.OutboxRelay.Client` dependency as a floor, not an exact version, so install every client package you use at one version. Each package ships one `net8.0` asset, which runs on net8, net9 and net10 runtimes. The EF package writes through an `IOutboxPublisher`, so EF producers install the PostgreSql package too.

## Create the outbox table

`PostgreSqlOutboxSchema.Scripts(table, namePrefix)` returns the relay's outbox DDL, rendered for your table, as `OutboxSchemaScript(Version, Name, Sql)` records in `Version` order. `Name` is the script name without extension, for example `V001__create_outbox`. Neither the package nor the relay runs DDL. Your migration tool or DBA applies each script once, in `Version` order, under a role with `CREATE` on the schema. The schema must exist before the first script runs: the scripts create no schema.

Columns after V001 and V002: `id`, `tx_id`, `message_key`, `payload`, `headers`, `route`, `created_at`, `retry_count`, `next_attempt_at`, `last_error`. V001 also creates six destination-only columns, which V002 drops (see [Compatibility and versioning](#compatibility-and-versioning)). The producer writes `message_key`, `payload`, `headers` and `route`. `route` is `text`, nullable, checked by constraint `<prefix>_route_format` (see [Routes](#routes)).

EF Core migration:

<!-- snippet: ddl-ef-migration -->
```csharp
[DbContext(typeof(SalesDb))]
[Migration("20261108000000_CreateSalesOutbox")]
public sealed class CreateSalesOutbox : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        // One migration per outbox script. Scripts come in Version order: [0] is V001.
        var v001 = PostgreSqlOutboxSchema.Scripts(OutboxTable.Parse("sales.outbox"))[0];
        migrationBuilder.Sql(v001.Sql);
    }
}
```

DbUp:

<!-- snippet: ddl-dbup -->
```csharp
var builder = DeployChanges.To.PostgresqlDatabase(connectionString);
foreach (var script in PostgreSqlOutboxSchema.Scripts(OutboxTable.Parse("sales.outbox")))
{
    // DbUp journals by script name: the table text in the name keeps a second outbox table's scripts apart.
    builder.WithScript($"sales.outbox/{script.Name}", script.Sql);
}

var result = builder.LogToConsole().Build().PerformUpgrade();
if (!result.Successful)
{
    throw result.Error;
}
```

Flyway: commit each script under your project's next free Flyway version, for example `V42__sales_outbox_create_outbox.sql`. The script `Name` (`V001__create_outbox`) is not a Flyway file name: Flyway reads `V001` as version 1, which clashes with an existing `V1` or fails validation after later versions. Map each script `Version` to your file name:

<!-- snippet: ddl-flyway-files -->
```csharp
// Your Flyway file name for each outbox script version, at your project's next free Flyway version.
// Add an entry when a client version ships a new script.
internal static readonly IReadOnlyDictionary<int, string> FlywayFiles = new Dictionary<int, string>
{
    [1] = "V42__sales_outbox_create_outbox.sql",
    [2] = "V43__sales_outbox_drop_destination_columns.sql",
};
```

Render the scripts into your Flyway location once and commit the files:

<!-- snippet: ddl-flyway-render -->
```csharp
foreach (var script in PostgreSqlOutboxSchema.Scripts(OutboxTable.Parse("sales.outbox")))
{
    File.WriteAllText(Path.Combine(migrationsDir, FlywayFiles[script.Version]), script.Sql);
}
```

Then keep a unit test that fails when the committed files differ from the scripts of the installed package version. A new script with no `FlywayFiles` entry fails it too (`KeyNotFoundException`):

<!-- snippet: ddl-flyway-check -->
```csharp
// In a unit test: the committed Flyway files equal the scripts of the installed package.
foreach (var script in PostgreSqlOutboxSchema.Scripts(OutboxTable.Parse("sales.outbox")))
{
    var fileName = FlywayFiles[script.Version];
    var committed = File.ReadAllText(Path.Combine(migrationsDir, fileName));
    if (!string.Equals(committed, script.Sql, StringComparison.Ordinal))
    {
        throw new InvalidOperationException($"{fileName} differs from the rendered outbox script {script.Name}. Render it again.");
    }
}
```

Name prefix. The scripts' named indexes and constraints start with `namePrefix`, which defaults to the table name (`table.Name`, without the schema). PostgreSQL names the primary key (`<table>_pkey`) and the identity sequence (`<table>_id_seq`) from the table, not from `namePrefix`. Index and constraint names are unique per schema and PostgreSQL allows 63 bytes, so the prefix may be at most 63 bytes minus the longest name suffix of the shipped scripts: 37 bytes with V001 and V002 (suffix `_header_values_are_strings`, 26 bytes, in V001. V002 names no index or constraint). A longer prefix is refused with `ArgumentException` (CE7), never truncated. Pass a shorter prefix for a long table name, and a distinct prefix for each of two outbox tables in one schema:

<!-- snippet: ddl-name-prefix -->
```csharp
// The 43-character table name exceeds the 37-byte prefix limit, so pass a shorter prefix.
var table = OutboxTable.Parse("sales.customer_account_ledger_entry_events_outbox");
var scripts = PostgreSqlOutboxSchema.Scripts(table, namePrefix: "ledger_outbox");
```

Schema upgrades. A later additive script `Vnnn` ships in a client minor version, a destructive one in a major, and `PostgreSqlOutboxSchema.Version` rises. Add one migration per new script, in order. DDL is additive only, with one exception: V002 (client 2.0.0) drops columns and their data, see the 2.0.0 release note.

Migration tool notes. DbUp journals by script name and Flyway by version. A second outbox table therefore needs its own script names in DbUp (the example prefixes them with the table text). In Flyway it needs its own versions or a separate schema history table (`flyway.table`). A separate location alone is not enough, because all locations share the versions of one schema history table.

## Grants

The producer's application role needs `USAGE` on the schema and `INSERT` on the outbox table:

```sql
GRANT USAGE ON SCHEMA sales TO <app_role>;
GRANT INSERT ON sales.outbox TO <app_role>;
```

No `SELECT`: the insert reads nothing back (no `RETURNING`). No sequence grant: `id` is an identity column. The relay's own grants are in the [runbook, section 3](runbook.md#3-database-prerequisites).

## Table text

Give the table text schema-qualified (`sales.outbox`), identical to the relay's `Outboxes[].Database.Table` setting. The client writes the text unquoted, so PostgreSQL folds it to lower case. Unqualified text resolves through the connecting role's `search_path`, which can differ between the producer role and the relay role, so the two could write and read different tables.

`OutboxTable.Parse` accepts `name` or `schema.name`. Each part matches `[A-Za-z_][A-Za-z0-9_]*` and has at most 63 characters. A part that is a PostgreSQL reserved keyword (categories R and T, any case, either part, for example `order` or `user`) is refused, although the relay would accept it because it quotes the name. Rename such a table. A refused text throws `ArgumentException` (CE7).

## Register the publisher

One `PostgreSqlOutboxPublisher` per outbox table, registered as a singleton `IOutboxPublisher`:

<!-- snippet: register-publisher -->
```csharp
// Parses the table text now: a bad text fails at startup.
services.AddPostgreSqlOutboxPublisher("sales.outbox");
```

The table text is parsed at registration, so a bad text fails at startup. A second non-keyed `AddPostgreSqlOutboxPublisher` call wins `IOutboxPublisher` resolution (standard DI last-wins). Several tables use keyed registrations, one key per table:

<!-- snippet: register-keyed-publishers -->
```csharp
services.AddKeyedPostgreSqlOutboxPublisher("sales", "sales.outbox");
services.AddKeyedPostgreSqlOutboxPublisher("billing", "billing.outbox");
```

<!-- snippet: register-keyed-inject -->
```csharp
public sealed class InvoiceService([FromKeyedServices("billing")] IOutboxPublisher publisher)
{
    public IOutboxPublisher Publisher => publisher;
}
```

A publisher that writes one route on every message: [Route-scoped services](#route-scoped-services).

## ADO.NET and Dapper

Begin the transaction, make the business writes, publish, commit. The publisher inserts on the transaction's connection and never commits, rolls back or disposes the transaction.

<!-- snippet: ado-publish -->
```csharp
await using var connection = await dataSource.OpenConnectionAsync(ct);
await using var transaction = await connection.BeginTransactionAsync(ct);

int updated;
await using (var debit = new NpgsqlCommand("UPDATE sales.accounts SET balance = balance - $1 WHERE id = $2", connection, transaction))
{
    debit.Parameters.Add(new NpgsqlParameter<decimal> { TypedValue = amount });
    debit.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = accountId });
    updated = await debit.ExecuteNonQueryAsync(ct);
}

// No row changed: publish no event.
if (updated != 1)
{
    await transaction.RollbackAsync(ct);
    return;
}

// idempotency-key = the business event id, set by the producer.
var headers = OutboxHeaders.CreateBuilder(1).Add("idempotency-key", eventId.ToString()).Build();
var debited = new AccountDebited(eventId, accountId, amount);
var message = OutboxMessage.Json(accountId.ToString(), debited, AppJson.Default.AccountDebited, headers);

// After the business writes: the outbox row is the transaction's last write.
await publisher.PublishAsync(transaction, message, ct);
await transaction.CommitAsync(ct);
```

Several messages: use the batch overload. It sends one round trip, row ids ascend in list order, and an empty list returns without contacting the database. The batch has no size cap: a large batch holds your transaction open and its rows in memory until the round trip completes. Batch size is your choice.

<!-- snippet: ado-publish-batch -->
```csharp
var messages = new OutboxMessage[events.Count];
for (var i = 0; i < events.Count; i++)
{
    var placed = events[i];
    var headers = OutboxHeaders.CreateBuilder(1).Add("idempotency-key", placed.EventId.ToString()).Build();
    messages[i] = OutboxMessage.Json(placed.OrderId.ToString(), placed, AppJson.Default.OrderPlaced, headers);
}

await publisher.PublishAsync(transaction, messages, ct);
await transaction.CommitAsync(ct);
```

Dapper, with the transaction passed to the command:

<!-- snippet: dapper-publish -->
```csharp
await using var connection = await dataSource.OpenConnectionAsync(ct);
await using var transaction = await connection.BeginTransactionAsync(ct);

await connection.ExecuteAsync(new CommandDefinition(
    "INSERT INTO sales.orders (id, account_id, total) VALUES (@Id, @AccountId, @Total)",
    order,
    transaction,
    cancellationToken: ct));

var headers = OutboxHeaders.CreateBuilder(1).Add("idempotency-key", eventId.ToString()).Build();
var placed = new OrderPlaced(eventId, order.Id, order.AccountId, order.Total);
var message = OutboxMessage.Json(order.Id.ToString(), placed, AppJson.Default.OrderPlaced, headers);

await publisher.PublishAsync(transaction, message, ct);
await transaction.CommitAsync(ct);
```

There is no autocommit publish. Without a transaction, wrap the publish in one:

<!-- snippet: no-autocommit -->
```csharp
// There is no autocommit publish. Without a transaction, wrap the publish in one.
await using var transaction = await connection.BeginTransactionAsync(ct);
await publisher.PublishAsync(transaction, message, ct);
await transaction.CommitAsync(ct);
```

## TransactionScope

Create the scope with `TransactionScopeAsyncFlowOption.Enabled`, open the connection inside it, make the business writes, call `PublishEnlistedAsync`, then `Complete()`:

<!-- snippet: transaction-scope -->
```csharp
using var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);

// Open the connection inside the scope (Enlist=true, the default). A connection opened before the scope is not
// enlisted: its writes and the outbox row autocommit independently.
await using var connection = await dataSource.OpenConnectionAsync(ct);

int updated;
await using (var debit = new NpgsqlCommand("UPDATE sales.accounts SET balance = balance - $1 WHERE id = $2", connection))
{
    debit.Parameters.Add(new NpgsqlParameter<decimal> { TypedValue = amount });
    debit.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = accountId });
    updated = await debit.ExecuteNonQueryAsync(ct);
}

// No row changed: publish no event and do not complete the scope.
if (updated != 1)
{
    return;
}

var headers = OutboxHeaders.CreateBuilder(1).Add("idempotency-key", eventId.ToString()).Build();
var debited = new AccountDebited(eventId, accountId, amount);
var message = OutboxMessage.Json(accountId.ToString(), debited, AppJson.Default.AccountDebited, headers);

await publisher.PublishEnlistedAsync(connection, message, ct);
scope.Complete();
```

Rules:

- Open the connection inside the scope with enlistment on (`Enlist=true`, the Npgsql default). A connection opened before the scope is not enlisted, so its business writes and the outbox row autocommit independently.
- The publisher never enlists the connection. It inserts in whatever transaction the connection is in.
- Without an ambient scope `PublishEnlistedAsync` throws (CE4). A connection that is not open throws (CE5).

## EF Core: explicit transaction

`IOutboxPublisher.PublishAsync(DbContext, ...)` writes in `Database.CurrentTransaction`. It needs no `UseOutbox` and does not touch the outbox queue. Without a current EF transaction it throws E7 and writes nothing.

<!-- snippet: ef-o15-savechanges -->
```csharp
await using var transaction = await db.Database.BeginTransactionAsync(ct);

db.Orders.Add(order);
await db.SaveChangesAsync(ct);

var headers = OutboxHeaders.CreateBuilder(1).Add("idempotency-key", eventId.ToString()).Build();
var placed = new OrderPlaced(eventId, order.Id, order.AccountId, order.Total);
var message = OutboxMessage.Json(order.Id.ToString(), placed, AppJson.Default.OrderPlaced, headers);

// Needs no UseOutbox. Writes in db.Database.CurrentTransaction, refused without one.
await publisher.PublishAsync(db, message, ct);
await transaction.CommitAsync(ct);
```

`ExecuteUpdate` and raw SQL inside the same EF transaction:

<!-- snippet: ef-o15-executeupdate -->
```csharp
await using var transaction = await db.Database.BeginTransactionAsync(ct);

await db.Accounts
    .Where(a => a.Id == accountId)
    .ExecuteUpdateAsync(set => set.SetProperty(a => a.Balance, a => a.Balance - amount), ct);

var headers = OutboxHeaders.CreateBuilder(1).Add("idempotency-key", eventId.ToString()).Build();
var debited = new AccountDebited(eventId, accountId, amount);
var message = OutboxMessage.Json(accountId.ToString(), debited, AppJson.Default.AccountDebited, headers);

await publisher.PublishAsync(db, message, ct);
await transaction.CommitAsync(ct);
```

Use this path for writes committed where EF does not see the commit: a transaction committed through `GetDbTransaction().Commit()`, a `Database.UseTransaction` transaction committed outside EF, `ExecuteUpdate` or raw SQL without an EF transaction (begin one, as above). The [EF queue](#ef-core-queue) loses its messages on those paths.

Inside a `TransactionScope` EF has no current transaction, so this overload throws E7. Publish on the context's connection instead:

<!-- snippet: ef-o15-scope -->
```csharp
using var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);

// Open inside the scope and keep it open: PublishEnlistedAsync needs the open, enlisted connection.
await db.Database.OpenConnectionAsync(ct);

db.Orders.Add(order);
await db.SaveChangesAsync(ct);

await publisher.PublishEnlistedAsync(db.Database.GetDbConnection(), message, ct);
scope.Complete();
```

## EF Core queue

`UseOutbox(publisher)` gives each context instance its own outbox queue. Enqueued messages are written with one batch at `CommitAsync` of a transaction the context began with `Database.BeginTransactionAsync`, as that transaction's last writes. The same line works for `AddDbContext`, `AddDbContextPool`, `AddDbContextFactory` and `AddPooledDbContextFactory`. Each pooled lease starts with an empty queue.

<!-- snippet: ef-queue-register -->
```csharp
services.AddPostgreSqlOutboxPublisher("sales.outbox");
services.AddDbContext<AppDb>((sp, options) => options
    .UseNpgsql(connectionString)
    .UseOutbox(sp.GetRequiredService<IOutboxPublisher>()));
services.AddOutboxQueue<AppDb>();
```

<!-- snippet: ef-queue-register-pool -->
```csharp
services.AddDbContextPool<AppDb>((sp, options) => options
    .UseNpgsql(connectionString)
    .UseOutbox(sp.GetRequiredService<IOutboxPublisher>()));
services.AddOutboxQueue<AppDb>();
```

<!-- snippet: ef-queue-register-factory -->
```csharp
services.AddDbContextFactory<AppDb>((sp, options) => options
    .UseNpgsql(connectionString)
    .UseOutbox(sp.GetRequiredService<IOutboxPublisher>()));
```

<!-- snippet: ef-queue-register-pooled-factory -->
```csharp
services.AddPooledDbContextFactory<AppDb>((sp, options) => options
    .UseNpgsql(connectionString)
    .UseOutbox(sp.GetRequiredService<IOutboxPublisher>()));
```

Two contexts on two outbox tables use keyed publishers:

<!-- snippet: ef-queue-keyed -->
```csharp
services.AddKeyedPostgreSqlOutboxPublisher("sales", "sales.outbox");
services.AddKeyedPostgreSqlOutboxPublisher("app", "app.outbox");

services.AddDbContext<SalesDb>((sp, options) => options
    .UseNpgsql(salesConnectionString)
    .UseOutbox(sp.GetRequiredKeyedService<IOutboxPublisher>("sales")));
services.AddDbContext<AppDb>((sp, options) => options
    .UseNpgsql(appConnectionString)
    .UseOutbox(sp.GetRequiredKeyedService<IOutboxPublisher>("app")));

services.AddOutboxQueue<SalesDb>();
services.AddOutboxQueue<AppDb>();
```

`AddOutboxQueue<TContext>()` registers the scoped `IOutboxQueue<TContext>` facade. It needs a scoped context (`AddDbContext`, `AddDbContextPool`). Enqueue costs no database round trip.

A keyed queue that writes one route on every message: [Route-scoped services](#route-scoped-services).

<!-- snippet: ef-queue-facade -->
```csharp
public sealed class PlaceOrderHandler(AppDb db, IOutboxQueue<AppDb> outbox)
{
    public async Task HandleAsync(Order order, Guid eventId, CancellationToken ct)
    {
        await using var transaction = await db.Database.BeginTransactionAsync(ct);

        db.Orders.Add(order);

        var headers = OutboxHeaders.CreateBuilder(1).Add("idempotency-key", eventId.ToString()).Build();
        var placed = new OrderPlaced(eventId, order.Id, order.AccountId, order.Total);
        outbox.Enqueue(OutboxMessage.Json(order.Id.ToString(), placed, AppJson.Default.OrderPlaced, headers));

        await db.SaveChangesAsync(ct);
        await transaction.CommitAsync(ct);
    }
}
```

Contexts created by `IDbContextFactory<TContext>` have no facade. Call `EnqueueOutbox` on the context:

<!-- snippet: ef-queue-factory -->
```csharp
public sealed class OrderImporter(IDbContextFactory<AppDb> factory)
{
    public async Task ImportAsync(Order order, Guid eventId, CancellationToken ct)
    {
        await using var db = await factory.CreateDbContextAsync(ct);
        await using var transaction = await db.Database.BeginTransactionAsync(ct);

        db.Orders.Add(order);

        // Factory-created contexts have no IOutboxQueue<AppDb>: enqueue on the context.
        var headers = OutboxHeaders.CreateBuilder(1).Add("idempotency-key", eventId.ToString()).Build();
        var placed = new OrderPlaced(eventId, order.Id, order.AccountId, order.Total);
        db.EnqueueOutbox(OutboxMessage.Json(order.Id.ToString(), placed, AppJson.Default.OrderPlaced, headers));

        await db.SaveChangesAsync(ct);
        await transaction.CommitAsync(ct);
    }
}
```

An `EfUnitOfWork`-shaped handler works unchanged: enqueue, then `unitOfWork.CommitAsync`. The unit of work begins the transaction, saves and commits, and the messages are written at its `CommitAsync` as the transaction's last writes, with the same `tx_id` as the business writes. This holds with zero tracked changes too (event-only units of work).

<!-- snippet: ef-queue-uow-handler -->
```csharp
public sealed class DebitAccountHandler(IUnitOfWork unitOfWork, IAccountRepository accounts, IOutboxQueue<AppDb> outbox)
{
    public async Task HandleAsync(Guid accountId, decimal amount, Guid eventId, CancellationToken ct)
    {
        var account = await accounts.GetAsync(accountId, ct);
        account.Balance -= amount;

        var headers = OutboxHeaders.CreateBuilder(1).Add("idempotency-key", eventId.ToString()).Build();
        var debited = new AccountDebited(eventId, accountId, amount);
        outbox.Enqueue(OutboxMessage.Json(accountId.ToString(), debited, AppJson.Default.AccountDebited, headers));

        // Unchanged unit of work: the queue is written at its CommitAsync, after SaveChanges, in the same transaction.
        await unitOfWork.CommitAsync(cancellationToken: ct);
    }
}
```

Commit rules:

- Messages are written at `CommitAsync` only.
- `SaveChanges` without an EF transaction while the queue is non-empty throws E2 and saves nothing.
- Synchronous `Commit()` with a non-empty queue throws E3. The transaction is not committed.
- Transactions from `Database.UseTransaction` are refused (E4). Use `PublishAsync(DbContext, ...)` there.
- A failed write of the queue at commit poisons the transaction. A later commit of it throws E5.

End without commit:

- Rollback, a failed publish at commit, and dispose without commit clear the queue (dispose reports the [Discard Warning](#discard-warning)).
- A rollback to a savepoint does not clear messages enqueued after the savepoint.
- After any failed transaction, re-run the unit of work on a fresh context.

Execution strategy. Under a retrying strategy (`EnableRetryOnFailure` or a custom `IExecutionStrategy`), enqueue only after `BeginTransactionAsync`, inside the operation passed to `strategy.ExecuteAsync`. An enqueue with no current EF transaction throws E8 and queues nothing, because a retry could commit the business writes without the message. When a retry runs, the failed attempt's messages are discarded with the Warning and the retry writes the messages it enqueues.

<!-- snippet: ef-queue-retrying-strategy -->
```csharp
var strategy = db.Database.CreateExecutionStrategy();
await strategy.ExecuteAsync(async () =>
{
    db.ChangeTracker.Clear();
    await using var transaction = await db.Database.BeginTransactionAsync(ct);

    db.Orders.Add(order);
    // Enqueue after BeginTransactionAsync, inside the operation. Before it, Enqueue throws and queues nothing.
    db.EnqueueOutbox(message);

    await db.SaveChangesAsync(ct);
    await transaction.CommitAsync(ct);
});
```

`EfUnitOfWork` begins its transaction outside any strategy operation, so it cannot run under a retrying strategy.

Decorator caveat. A custom `IExecutionStrategy` that does not derive from `ExecutionStrategy` and forwards `RetriesOnFailure` to an EF strategy reads `false` inside `ExecuteAsync`. E8 then does not fire for an enqueue before `BeginTransactionAsync`, and a retry can write that message twice. With such a strategy, always enqueue after `BeginTransactionAsync`.

Buffers. The queue keeps references to each message's key and payload buffers until the transaction commits or rolls back, or the queue discards the message. Do not change or reuse those buffers before then.

Remaining loss case. Business writes commit without the queued messages when EF does not see the commit: an EF transaction committed through `GetDbTransaction().Commit()`, `ExecuteUpdate` or raw SQL without an EF transaction, or an ambient `TransactionScope` with no `SaveChanges` after the enqueue. Inside a scope, `SaveChanges` with a non-empty queue throws E2 and saves nothing. Use the [explicit transaction](#ef-core-explicit-transaction) path there.

## Errors

The client does not catch, wrap or log database errors. The codes are guide labels only, never in exception text. Each message column quotes the first sentence of the exception message, with variable parts as `<name>`.

<!-- docs-check: error-messages -->
EF package (`Pernix.OutboxRelay.Client.EntityFrameworkCore`):

| code | exception | message | cause | fix |
|---|---|---|---|---|
| E1 | `InvalidOperationException` | `DbContext type '<type>' has no outbox queue.` | `EnqueueOutbox` or `IOutboxQueue<TContext>.Enqueue` on a context configured without `UseOutbox`. | Call `UseOutbox(publisher)` on the context's options builder. |
| E2 | `InvalidOperationException` | `SaveChanges was called with queued outbox messages and no EF transaction, so the messages could not be written atomically.` | `SaveChanges` with a non-empty queue and no EF transaction. Nothing was saved. | Begin with `Database.BeginTransactionAsync` and commit with `CommitAsync`, or use `PublishAsync(DbContext, ...)` in your own transaction. |
| E3 | `InvalidOperationException` | `The EF transaction has queued outbox messages, which are written asynchronously at commit.` | Synchronous `Commit()` with a non-empty queue. The transaction was not committed. | Commit with `CommitAsync`. |
| E4 | `InvalidOperationException` | `The context's transaction came from Database.UseTransaction.` | `UseTransaction` with a non-empty queue, or an enqueue under a `UseTransaction` transaction. | Call `PublishAsync(DbContext, ...)` after the business writes. |
| E5 | `InvalidOperationException` | `Writing the queued outbox messages failed in an earlier commit of this EF transaction.` | Commit of a transaction whose queue write failed. | Roll back or dispose the transaction, re-run the unit of work on a fresh context. |
| E6 | `InvalidOperationException` | `UseOutbox was already called on this DbContextOptionsBuilder.` | Second `UseOutbox` on one options builder. | Call `UseOutbox` once per context configuration. |
| E7 | `InvalidOperationException` | `The context has no current EF transaction.` | `PublishAsync(DbContext, ...)` without `Database.CurrentTransaction`, including inside a `TransactionScope`. | Begin with `Database.BeginTransactionAsync`. Inside a scope use `PublishEnlistedAsync(context.Database.GetDbConnection(), ...)`. |
| E8 | `InvalidOperationException` | `The context's execution strategy retries on failure and the context has no current EF transaction, so a retry could commit the business writes without this message.` | Enqueue with no current EF transaction under a retrying execution strategy. Nothing was queued. | Enqueue inside `strategy.ExecuteAsync`, after `BeginTransactionAsync`. |

Publisher and core (`Pernix.OutboxRelay.Client`, `Pernix.OutboxRelay.Client.PostgreSql`):

| code | exception | message | cause | fix |
|---|---|---|---|---|
| CE1 | `ArgumentNullException`, `ArgumentOutOfRangeException` | .NET text | Null `transaction`, `connection`, `messages`, `services`, `key` (keyed DI), `payload` (string `Create`), `typeInfo`, `context`, `publisher` or `builder`. Negative `CreateBuilder` capacity. | Pass the argument. |
| CE2 | `InvalidOperationException` | `Transaction type '<type>' is not supported.` `Connection type '<type>' is not supported.` | A transaction or connection that is not Npgsql. | Use an `NpgsqlTransaction` or `NpgsqlConnection`. |
| CE3 | `InvalidOperationException` | `The transaction is completed.` | Publish after commit or rollback. | Publish inside the open transaction, before commit. |
| CE4 | `InvalidOperationException` | `No ambient TransactionScope.` | `PublishEnlistedAsync` without a scope. | Create a `TransactionScope`, or pass the `DbTransaction` to `PublishAsync`. |
| CE5 | `InvalidOperationException` | `The connection is <state>.` | `PublishEnlistedAsync` on a connection that is not open. | Open the connection inside the scope before publishing. |
| CE6 | `ArgumentException` | `Header <i>: <rule>.` Rules: `name is null or empty`, `value is null`, `name contains U+0000`, `value contains U+0000`, `name contains a lone surrogate`, `value contains a lone surrogate`, `name duplicates header <j> (ordinal)` | `OutboxHeaders.Builder.Build()` with a header that breaks a [header rule](#headers). `<i>` is the zero-based position. | Fix the header at that position. |
| CE7 | `ArgumentException` | `Outbox table text is null or empty.` `Outbox table '<text>' is invalid.` `Outbox table is default(OutboxTable).` `namePrefix '<prefix>'<source> is invalid.` `namePrefix '<prefix>'<source> is <n> bytes.` | Table text outside the [grammar](#table-text) or a reserved keyword (`OutboxTable.Parse`, registration). `default(OutboxTable)` (publisher constructor, `Scripts`). Prefix outside `[A-Za-z_][A-Za-z0-9_]*` or too long (`Scripts`, `<source>` says when it defaulted from the table name). | Fix the table text or rename the table. Create tables with `OutboxTable.Parse`. Pass a shorter `namePrefix`. |
| CE8 | `ArgumentException` | `Value contains a lone surrogate and has no UTF-8 form.` | String key or payload with a lone surrogate in `Create` or `Json`. | Pass valid UTF-16 text, or bytes through `FromBytes`. |
| CE9 | `OperationCanceledException` | .NET text | The `ct` was cancelled. | Decide the transaction's outcome yourself: the publisher does not roll back. |
| CE10 | `PostgresException` (not wrapped) | .NET text | For example `42P01` table not found (table text or `search_path` mismatch, see [Table text](#table-text)), `42501` missing `INSERT` or `USAGE` (see [Grants](#grants)). | Fix the table text or the grant. |
| CE11 | `ArgumentNullException` | .NET text | Null `route` in `WithRoute`, `AddKeyedPostgreSqlOutboxPublisher(key, table, route)` or `AddKeyedOutboxQueue<TContext>(key, route)`. | Pass a route, or omit `WithRoute` for a route-less message. |
| CE12 | `ArgumentException` | `Route must match ^[a-z0-9][a-z0-9._-]{0,62}$: 1 to 63 lowercase ASCII letters, digits, '.', '-' or '_', starting with a letter or digit.` | Route outside the [format](#routes), at the same three call sites. | Fix the route text. |
| CE13 | `ArgumentException` | `Message route '<route>' differs from scoped route '<scoped>'.` `Message <i> route '<route>' differs from scoped route '<scoped>'.` | A route-scoped publisher or keyed queue got a message with another route. Nothing written or queued. Through `UseOutbox(scopedPublisher)` it leaves `CommitAsync` and the transaction is not committed. | Remove `WithRoute`, or use the service registered for that route. |
| CE14 | `PostgresException` `23514` (not wrapped) | .NET text | Hand-written SQL inserted a route outside the format. Constraint `<prefix>_route_format`. The business transaction rolls back. | Fix the route, or set it through `WithRoute`. |
| CE15 | `PostgresException` `42703` (not wrapped) | .NET text | Routed publish on a table created from 1.0.0's V001 (no `route` column). Or a hand-SQL producer builds its column list from `OutboxColumns.ProducerWritten` or `OutboxColumns.All` against such a table. | Add the column and CHECK as in V001, or recreate the table from 1.1.0's V001. |

Log (EF package):

| code | exception | message | cause | fix |
|---|---|---|---|---|
| Warning | none, Warning `OutboxMessagesDiscarded`, event id 1 | `Discarded <count> queued outbox messages for outbox table <table>.` | Queued messages were dropped without being written. See [Discard Warning](#discard-warning). | Re-run the unit of work. Check for the remaining loss case. |
<!-- /docs-check: error-messages -->

## Discard Warning

`OutboxMessagesDiscarded` (event id 1, Warning, category `Pernix.OutboxRelay.Client.EntityFrameworkCore.OutboxQueue`) means queued messages were not written. It fires when:

- the context is disposed or returned to the pool with a non-empty queue;
- the EF transaction the messages were enqueued in ended without `CommitAsync` and without a rollback through EF, seen at the context's next use;
- messages were flushed at commit but that transaction did not commit;
- a retrying execution strategy starts a new attempt (the failed attempt's messages).

A re-run unit of work enqueues and writes them again. In every case except the [remaining loss case](#ef-core-queue) the business writes did not commit either. Only that case loses a message whose business change committed.

The Warning goes through the context's EF `ILoggerFactory`: the application logger factory under DI, or `UseLoggerFactory`. `LogTo` output does not receive it. Outside DI:

<!-- snippet: ef-logger-factory -->
```csharp
// Outside DI, pass the application ILoggerFactory: LogTo output does not receive the discard Warning.
var options = new DbContextOptionsBuilder<AppDb>()
    .UseNpgsql(connectionString)
    .UseLoggerFactory(loggerFactory)
    .UseOutbox(publisher)
    .Options;
```

It carries the table text and the count only, never a key, payload or header value.

## Write order

Make the write that serializes the key (the update or lock of the business row) the transaction's first write and the outbox insert its last. The relay delivers in gate order `(tx_id, id)`, and `tx_id` is the transaction id PostgreSQL assigns at the transaction's first write. An outbox insert as first write can reach consumers ahead of the state it describes. Details: [Write-order rule](delivery.md#write-order-rule).

- Order-safe by construction: the [EF queue](#ef-core-queue), which writes at commit as the last writes.
- Your job: [explicit EF transaction](#ef-core-explicit-transaction), [ADO.NET and Dapper](#adonet-and-dapper), [TransactionScope](#transactionscope). Publish after the business writes.

## Idempotency key and dedupe

Set header `idempotency-key` to your business event id on every message. The client generates nothing: no `idempotency-key`, no `traceparent`.

Delivery is at least once. Consumers deduplicate on `idempotency-key`. Dead-letter consumers deduplicate on `x-original-outbox` and `x-original-id` ([Dead letters](delivery.md#dead-letters)). When duplicates happen: [Duplicates](delivery.md#duplicates), [runbook section 7](runbook.md#7-duplicates).

## Headers

<!-- snippet: message-headers -->
```csharp
var headers = OutboxHeaders.CreateBuilder(2)
    .Add("idempotency-key", placed.EventId.ToString())
    .Add("event-type", "order-placed")
    .Build();
```

`Build()` checks every header, in order: name not null or empty; value not null (empty allowed); no U+0000 in name or value; no lone surrogate in name or value; name not ordinal-equal to an earlier name. Names that differ only by case are distinct. A broken rule throws `ArgumentException` naming the rule and the zero-based position, never a name or value (CE6).

Kafka header order is the `jsonb` key order PostgreSQL stores, not `Add` order. Headers named like relay dead-letter headers (`x-original-*`) pass through unchanged on normal delivery.

An `OutboxHeaders` instance is immutable and writes its JSON once, when built. Reuse one instance for headers that never change. Build per message only when a value changes per message, such as `idempotency-key`.

## Keys and factories

Every factory takes the key first. The `OutboxMessage` constructor is internal.

`Create`: string key (UTF-8), bytes or string payload:

<!-- snippet: message-create -->
```csharp
var fromBytes = OutboxMessage.Create(orderId.ToString(), payloadBytes, headers);
var fromText = OutboxMessage.Create(orderId.ToString(), payloadText, headers);
```

`Json<T>`: payload serialized with source-generated metadata only (`JsonTypeInfo<T>`):

<!-- snippet: message-json -->
```csharp
// Source-generated metadata only: AppJson is a JsonSerializerContext with [JsonSerializable(typeof(OrderPlaced))].
var message = OutboxMessage.Json(placed.OrderId.ToString(), placed, AppJson.Default.OrderPlaced, headers);
```

`FromBytes`: `byte[]?` or `ReadOnlyMemory<byte>?` key. Arrays are not copied.

<!-- snippet: message-frombytes -->
```csharp
var fromArray = OutboxMessage.FromBytes(keyArray, payload, headers);
var fromMemory = OutboxMessage.FromBytes(keyMemory, payload, headers);
```

Keys:

- A null key writes SQL NULL. NULL keys get no ordering under `PerKey` and are a permanent fault on a compacted topic. An empty key writes an empty `bytea`.
- The client checks no size and no key presence. The relay classifies them.

Target-typing trap. In a conditional, `null` converts to the other branch's type. With a `ReadOnlyMemory<byte>` branch the key becomes an empty `ReadOnlyMemory<byte>`, not NULL:

<!-- snippet: frombytes-trap -->
```csharp
// Wrong: the null converts to an empty ReadOnlyMemory<byte>. The key is an empty bytea, not SQL NULL.
var message = OutboxMessage.FromBytes(noKey ? null : key, payload);
```

Cast one branch to `ReadOnlyMemory<byte>?` to write SQL NULL:

<!-- snippet: frombytes-null-key -->
```csharp
// Right: cast one branch to ReadOnlyMemory<byte>?, so the null stays null and writes SQL NULL.
var message = OutboxMessage.FromBytes(noKey ? null : (ReadOnlyMemory<byte>?)key, payload);
```

A unit test pins both behaviours.

## Routes

A route names the destination subset a row is meant for. `OutboxMessage.Route` is null on every factory result, and a null route writes SQL NULL: the row has no route. `WithRoute(route)` returns a copy with the route set. Key, payload and headers are unchanged, and an existing route is replaced.

Format: `^[a-z0-9][a-z0-9._-]{0,62}$`, 1 to 63 lowercase ASCII letters, digits, `.`, `-` or `_`, starting with a letter or digit. `WithRoute` checks it and throws `ArgumentException` (CE12). The table's `<prefix>_route_format` constraint checks the same rule for hand-written SQL (CE14).

The relay delivers a row only to an address configured for its exact route, see [Routing](delivery.md#routing). A routed row on a destination with one address is held, not delivered. A Kafka destination with `Routes` delivers routed rows whose route it maps. A destination with `Topic` holds routed rows, so write routes only once the relay maps them.

Routed publish (ADO.NET, Dapper):

<!-- snippet: route-publish -->
```csharp
var headers = OutboxHeaders.CreateBuilder(1).Add("idempotency-key", placed.EventId.ToString()).Build();
var message = OutboxMessage.Json(placed.OrderId.ToString(), placed, AppJson.Default.OrderPlaced, headers)
    .WithRoute("billing");

await publisher.PublishAsync(transaction, message, ct);
await transaction.CommitAsync(ct);
```

Broadcast or subset: one message per route, in one batch.

<!-- snippet: route-broadcast -->
```csharp
// One message per route, one batch: one round trip, all rows commit or none.
var headers = OutboxHeaders.CreateBuilder(1).Add("idempotency-key", placed.EventId.ToString()).Build();
var message = OutboxMessage.Json(placed.OrderId.ToString(), placed, AppJson.Default.OrderPlaced, headers);
OutboxMessage[] messages = [message.WithRoute("billing"), message.WithRoute("shipping")];

await publisher.PublishAsync(transaction, messages, ct);
await transaction.CommitAsync(ct);
```

Routed EF enqueue, through `IOutboxQueue<TContext>` or `EnqueueOutbox`:

<!-- snippet: route-ef-enqueue -->
```csharp
var message = OutboxMessage.Json(placed.OrderId.ToString(), placed, AppJson.Default.OrderPlaced, headers);
outbox.Enqueue(message.WithRoute("billing"));
```

### Route-scoped services

A route-scoped service writes one route on every message. ADO.NET and Dapper use a keyed publisher per route. Several keys may share one table:

<!-- snippet: route-scoped-publishers -->
```csharp
// Validates each route and parses the table now: a bad text fails at startup.
services.AddKeyedPostgreSqlOutboxPublisher("billing", "sales.outbox", "billing");
services.AddKeyedPostgreSqlOutboxPublisher("shipping", "sales.outbox", "shipping");
```

<!-- snippet: route-scoped-publisher-inject -->
```csharp
public sealed class BillingNotifier([FromKeyedServices("billing")] IOutboxPublisher publisher)
{
    // Every message this publisher writes gets route 'billing'.
    public Task NotifyAsync(NpgsqlTransaction transaction, OutboxMessage message, CancellationToken ct) =>
        publisher.PublishAsync(transaction, message, ct);
}
```

EF Core uses a keyed `IOutboxQueue<TContext>` per route. It targets the scope's context instance, as `AddOutboxQueue<TContext>()` does, and works next to it. Keyed queues of one context share that context's queue, so their messages are written in enqueue order in one batch at `CommitAsync`.

<!-- snippet: route-scoped-queues -->
```csharp
services.AddKeyedOutboxQueue<AppDb>("billing", "billing");
services.AddKeyedOutboxQueue<AppDb>("shipping", "shipping");
```

<!-- snippet: route-scoped-queue-inject -->
```csharp
public sealed class ShipOrderHandler(AppDb db, [FromKeyedServices("shipping")] IOutboxQueue<AppDb> outbox)
{
    public async Task HandleAsync(Order order, Guid eventId, CancellationToken ct)
    {
        await using var transaction = await db.Database.BeginTransactionAsync(ct);

        db.Orders.Add(order);

        // No WithRoute: the keyed queue writes route 'shipping'.
        var headers = OutboxHeaders.CreateBuilder(1).Add("idempotency-key", eventId.ToString()).Build();
        var placed = new OrderPlaced(eventId, order.Id, order.AccountId, order.Total);
        outbox.Enqueue(OutboxMessage.Json(order.Id.ToString(), placed, AppJson.Default.OrderPlaced, headers));

        await db.SaveChangesAsync(ct);
        await transaction.CommitAsync(ct);
    }
}
```

A route-scoped service never overrides a message's route:

| `message.Route` | Result |
|---|---|
| null | Written or queued with the scoped route. |
| equal to the scoped route (ordinal) | Written or queued as is. |
| different | `ArgumentException` (CE13). Nothing written or queued. In a batch, no row of the batch is written. |

Both registrations validate `route` at the call (CE11, CE12). A second registration under the same key replaces the first.

`UseOutbox` with a route-scoped publisher is allowed. Queued messages are stamped at `CommitAsync` by the rule above. A different route makes `CommitAsync` throw CE13, the transaction is not committed, and a later commit of that transaction throws E5. Prefer the keyed `IOutboxQueue<TContext>` for per-route EF enqueue: it refuses the message at `Enqueue`.

## Compatibility and versioning

| Item | Value |
|---|---|
| Client `1.x` | This relay needs V001 with `route` (client 1.1.0+), and a routed row needs a destination that maps its route (producer contract PCV1: outbox columns, header rules, `tx_id` default). The `route` column added in 1.1.0 is an optional additive extension of PCV1: nullable, no default, and a route-less row is delivered as before, so 1.1.0 is a minor. The relay requires the column: startup refuses an outbox table created from 1.0.0's V001, which lacks it. |
| Client `2.x` | Same producer contract as 1.1.0 for rows. Ships V002, which drops the six destination-only columns. The relay accepts the outbox table with or without them, so V002 and the relay deploy can go in either order, except on a table that is a Database destination target (see the 2.0.0 release note). |
| Schema | `PostgreSqlOutboxSchema.Version` = 2 (V001, with `route` since 1.1.0, and V002 since 2.0.0). |
| Asset | `net8.0`, runs on net8, net9 and net10 runtimes. |
| Dependency floors | As in [Packages](#packages). The EF floor is tested (`Client.EntityFrameworkCore.Floor.Integration.Tests`). The Npgsql floor is not tested (known gap). |
| PostgreSQL tested | 13.23 (DDL apply), 17.11 (all suites). |

SemVer: major = producer contract change or breaking API, minor = new API or new `Vnnn` script, patch = fixes. A destructive `Vnnn` is a major: V002 is the one such script (2.0.0). The package version is independent of the relay version line (tags `client/v*` vs `relay/v*`, [ADR 0013](adr/0013-adopt-pernix-ci-and-tag-only-versions.md)). A relay release that needs schema version N or later says so, and the matching client minor ships `V00N`. Prerelease versions come from `client/vX.Y.Z-<pre>` tags and are for trial only.

Release note 2.0.0. Breaking. The relay's Database destination is removed (ADR 0010). `OutboxColumns` loses `SourceOutbox`, `SourceId`, `SourceDestination`, `DeadLetteredAt`, `DeadLetterReason` and `DeadLetterAttempts`, and `OutboxColumns.All` lists 10 columns. `OutboxColumns.ProducerWritten` is unchanged. New script V002 (`V002__drop_destination_columns`), `PostgreSqlOutboxSchema.Version` = 2. V002 drops `source_outbox`, `source_id`, `source_destination`, `dead_lettered_at`, `dead_letter_reason` and `dead_letter_attempts`, and with them the `<prefix>_source_complete` constraint and the `<prefix>_source_uq` index. This is the one exception to "DDL is additive only". Data in those columns is lost. Only a table that received rows as a Database destination target holds such data. Upgrade: add one migration for V002 after V001. The relay accepts the outbox table before and after V002, so for an outbox table there is no ordering constraint with the relay deploy. A table that is a Database destination target takes V002 only after every relay delivering into it runs this build or has dropped that destination. An older relay names the dropped columns: its inserts into that table fail with `42703`, rows stall in its source outbox, and on restart its target validation refuses the table with "missing columns". V002 takes an `ACCESS EXCLUSIVE` lock: no table rewrite, but it waits for open transactions on the table and blocks inserts while it waits. Set `lock_timeout` (for example `SET lock_timeout = '5s'`) in the migration session and retry on timeout. A view, rule or foreign key on a dropped column fails V002 and nothing is applied: drop or rewrite that object first.

Release note 1.1.0. Additive: `OutboxMessage.Route`, `WithRoute`, `OutboxColumns.Route` and the route-scoped registrations are new. `OutboxColumns.ProducerWritten` and `OutboxColumns.All` gained `route`, and in `All` every column after `headers` moved one position. V001 gained the `route` column and the `<prefix>_route_format` constraint in place, with `PostgreSqlOutboxSchema.Version` still 1. This is a one-time exception to "DDL is additive only", made because no consumer had applied V001. Later schema changes ship as new `Vnnn` scripts. A table created from 1.0.0's V001 lacks `route`: route-less publish still works on it, and routed publish fails with `PostgresException` `42703` (CE15).
