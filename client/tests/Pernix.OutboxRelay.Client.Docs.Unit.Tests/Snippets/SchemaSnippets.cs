using Pernix.OutboxRelay.Client.PostgreSql;
using DbUp;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

namespace Pernix.OutboxRelay.Client.Docs.Unit.Tests.Snippets;

#region docs:ddl-ef-migration
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
#endregion

internal static class SchemaSnippets
{
    public static void ApplyWithDbUp(string connectionString)
    {
        #region docs:ddl-dbup
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
        #endregion
    }

    #region docs:ddl-flyway-files
    // Your Flyway file name for each outbox script version, at your project's next free Flyway version.
    // Add an entry when a client version ships a new script.
    internal static readonly IReadOnlyDictionary<int, string> FlywayFiles = new Dictionary<int, string>
    {
        [1] = "V42__sales_outbox_create_outbox.sql",
        [2] = "V43__sales_outbox_drop_destination_columns.sql",
    };
    #endregion

    public static void RenderFlyway(string migrationsDir)
    {
        #region docs:ddl-flyway-render
        foreach (var script in PostgreSqlOutboxSchema.Scripts(OutboxTable.Parse("sales.outbox")))
        {
            File.WriteAllText(Path.Combine(migrationsDir, FlywayFiles[script.Version]), script.Sql);
        }
        #endregion
    }

    public static void CheckFlyway(string migrationsDir)
    {
        #region docs:ddl-flyway-check
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
        #endregion
    }

    public static IReadOnlyList<OutboxSchemaScript> NamePrefix()
    {
        #region docs:ddl-name-prefix
        // The 43-character table name exceeds the 37-byte prefix limit, so pass a shorter prefix.
        var table = OutboxTable.Parse("sales.customer_account_ledger_entry_events_outbox");
        var scripts = PostgreSqlOutboxSchema.Scripts(table, namePrefix: "ledger_outbox");
        #endregion
        return scripts;
    }
}
