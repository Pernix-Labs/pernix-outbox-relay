using Pernix.OutboxRelay.Core.Abstractions;
using Xunit;
using static Pernix.OutboxRelay.Integration.Tests.PostgreSql.PostgreSqlOutboxStoreTests;

namespace Pernix.OutboxRelay.Integration.Tests.PostgreSql;

public sealed class PostgreSql18OutboxStoreTests(PostgreSql18Fixture pg18) : IClassFixture<PostgreSql18Fixture>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Not_valid_not_null_column_fails_on_pg18()
    {
        var provider = Provider(("Db", pg18.ConnectionString));

        var v = PostgreSqlFixture.NewSchema();
        await PostgreSqlFixture.CreateOutboxSchemaAsync(pg18.ConnectionString, v);
        await provider.CreateStore(Outbox("Orders", "Db", $"{v}.outbox")).ValidateAsync(Ct);
        Assert.Equal("7/7", await pg18.ScalarAsync<string>(
            $"SELECT count(*) FILTER (WHERE convalidated) || '/' || count(*) FROM pg_catalog.pg_constraint WHERE conrelid = '{v}.outbox'::pg_catalog.regclass AND contype = 'n'"));

        var nv = PostgreSqlFixture.NewSchema();
        await PostgreSqlFixture.CreateOutboxSchemaAsync(pg18.ConnectionString, nv);
        await pg18.ExecuteAsync(
            $"""
            ALTER TABLE {nv}.outbox ALTER COLUMN tx_id DROP NOT NULL;
            INSERT INTO {nv}.outbox (tx_id, payload) VALUES (NULL, '\x00');
            ALTER TABLE {nv}.outbox ADD CONSTRAINT outbox_tx_id_not_null NOT NULL tx_id NOT VALID;
            """);
        Assert.Equal("true n false", await pg18.ScalarAsync<string>(
            $"SELECT a.attnotnull::pg_catalog.text || ' ' || k.contype::pg_catalog.text || ' ' || k.convalidated::pg_catalog.text FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_constraint k ON k.conrelid = a.attrelid AND k.contype = 'n' AND a.attnum = ANY(k.conkey) WHERE a.attrelid = '{nv}.outbox'::pg_catalog.regclass AND a.attname = 'tx_id'"));
        Assert.Equal(1L, await pg18.ScalarAsync<long>($"SELECT count(*) FROM {nv}.outbox WHERE tx_id IS NULL"));

        var ex = await Assert.ThrowsAsync<OutboxValidationException>(
            () => provider.CreateStore(Outbox("Orders", "Db", $"{nv}.outbox")).ValidateAsync(Ct));

        Assert.Equal($"Outboxes[Orders]: table '{nv}.outbox' columns differ from V001: tx_id xid8 NULL (expected xid8 NOT NULL)", ex.Message);
    }
}
