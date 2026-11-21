using Pernix.OutboxRelay.Client.EntityFrameworkCore;
using Pernix.OutboxRelay.Client.PostgreSql;
using Dapper;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace Pernix.OutboxRelay.Client.Docs.Unit.Tests.Snippets;

#region docs:readme-ddl-ef-migration
[DbContext(typeof(RiskDb))]
[Migration("20261108000000_CreateRiskOutbox")]
public sealed class CreateRiskOutbox : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        // One migration per outbox script. Scripts come in Version order: [0] is V001.
        var v001 = PostgreSqlOutboxSchema.Scripts(OutboxTable.Parse("risk.outbox"))[0];
        migrationBuilder.Sql(v001.Sql);
    }
}
#endregion

internal static class ReadmeSnippets
{
    public static void Register(IServiceCollection services, string connectionString)
    {
        #region docs:readme-register
        services.AddPostgreSqlOutboxPublisher("risk.outbox");
        services.AddDbContext<RiskDb>((sp, options) => options
            .UseNpgsql(connectionString)
            .UseOutbox(sp.GetRequiredService<IOutboxPublisher>()));
        services.AddOutboxQueue<RiskDb>();
        #endregion
    }

    public static async Task DapperPublishAsync(
        NpgsqlDataSource dataSource, IOutboxPublisher publisher, RuleSet ruleSet, Guid eventId, CancellationToken ct)
    {
        #region docs:readme-dapper-publish
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);

        // Business write first.
        var updated = await connection.ExecuteAsync(new CommandDefinition(
            "UPDATE risk.rule_sets SET definition = @Definition, version = @Version WHERE id = @Id",
            ruleSet,
            transaction,
            cancellationToken: ct));

        // No row changed: publish no event.
        if (updated != 1)
        {
            await transaction.RollbackAsync(ct);
            return;
        }

        var headers = OutboxHeaders.CreateBuilder(1).Add("idempotency-key", eventId.ToString()).Build();
        var changed = new RuleSetChanged(eventId, ruleSet.Id);
        var message = OutboxMessage.Json(ruleSet.Id.ToString(), changed, RiskJson.Default.RuleSetChanged, headers);

        // Outbox row last, on the same transaction.
        await publisher.PublishAsync(transaction, message, ct);
        await transaction.CommitAsync(ct);
        #endregion
    }
}

#region docs:readme-ef-uow-handler
public sealed class UpdateRuleSetHandler(IUnitOfWork unitOfWork, IRuleSetRepository ruleSets, IOutboxQueue<RiskDb> outbox)
{
    public async Task HandleAsync(Guid ruleSetId, string definition, Guid eventId, CancellationToken ct)
    {
        var ruleSet = await ruleSets.GetAsync(ruleSetId, ct);
        ruleSet.Definition = definition;

        var headers = OutboxHeaders.CreateBuilder(1).Add("idempotency-key", eventId.ToString()).Build();
        var changed = new RuleSetChanged(eventId, ruleSetId);
        outbox.Enqueue(OutboxMessage.Json(ruleSetId.ToString(), changed, RiskJson.Default.RuleSetChanged, headers));

        // The queued row is written at commit, after SaveChanges, in the same transaction.
        await unitOfWork.CommitAsync(cancellationToken: ct);
    }
}
#endregion
