using Pernix.OutboxRelay.ContractVectors;
using Npgsql;
using NpgsqlTypes;
using Xunit;

namespace Pernix.OutboxRelay.Client.PostgreSql.Integration.Tests;

public sealed class RouteCheckTests(PostgreSqlFixture postgres)
{
    private const string CheckViolation = "23514";

    public static TheoryData<string> VectorIds()
    {
        var ids = new TheoryData<string>();
        foreach (var vector in RouteVectors.All)
        {
            ids.Add(vector.Id);
        }

        return ids;
    }

    [Theory]
    [MemberData(nameof(VectorIds))]
    public async Task Check_accepts_exactly_when_the_vector_and_with_route_accept(string id)
    {
        var vector = RouteVectors.Get(id);
        var table = await postgres.CreateOutboxAsync("t", "p");

        var checkAccepts = await TryInsertAsync(table, vector.Route);

        Assert.Equal(vector.Accepts, checkAccepts);
        Assert.Equal(ClientAccepts(vector.Route), checkAccepts);
        Assert.Equal(checkAccepts ? 1 : 0, await CountAsync(table, vector.Route));
    }

    [Fact]
    public async Task Null_route_passes_the_check()
    {
        var table = await postgres.CreateOutboxAsync("t", "p");

        Assert.True(await TryInsertAsync(table, null));
    }

    private async Task<bool> TryInsertAsync(OutboxTable table, string? route)
    {
        await using var connection = new NpgsqlConnection(postgres.ConnectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await using var command = new NpgsqlCommand($"INSERT INTO {table} (payload, route) VALUES ($1, $2)", connection);
        command.Parameters.Add(new NpgsqlParameter<byte[]> { NpgsqlDbType = NpgsqlDbType.Bytea, TypedValue = [1] });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)route ?? DBNull.Value });

        try
        {
            await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
            return true;
        }
        catch (PostgresException ex) when (ex.SqlState == CheckViolation)
        {
            Assert.Equal("p_route_format", ex.ConstraintName);
            return false;
        }
    }

    private async Task<long> CountAsync(OutboxTable table, string route)
    {
        await using var connection = new NpgsqlConnection(postgres.ConnectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await using var command = new NpgsqlCommand($"SELECT count(*) FROM {table} WHERE route = $1", connection);
        command.Parameters.Add(new NpgsqlParameter<string> { NpgsqlDbType = NpgsqlDbType.Text, TypedValue = route });
        return (long)(await command.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;
    }

    private static bool ClientAccepts(string route)
    {
        try
        {
            _ = OutboxMessage.Create("k", "p").WithRoute(route);
            return true;
        }
        catch (ArgumentException ex) when (ex is not ArgumentNullException)
        {
            return false;
        }
    }
}
