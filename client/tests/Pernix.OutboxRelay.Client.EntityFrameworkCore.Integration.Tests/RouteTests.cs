using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Xunit;

namespace Pernix.OutboxRelay.Client.EntityFrameworkCore.Integration.Tests;

public sealed class RouteTests(EfPostgreSqlFixture postgres)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public enum EnqueuePath
    {
        EnqueueOutbox,
        Facade,
    }

    public enum Shape
    {
        Single,
        Batch,
    }

    [Theory]
    [InlineData(EnqueuePath.EnqueueOutbox)]
    [InlineData(EnqueuePath.Facade)]
    public async Task T16_mixed_routes_are_written_in_enqueue_order_each_with_its_route(EnqueuePath path)
    {
        var schema = await postgres.CreateSchemaAsync();
        var services = new ServiceCollection();
        services.AddDbContext<ItemsDbContext>(options => schema.Configure(options));
        services.AddOutboxQueue<ItemsDbContext>();
        await using var provider = services.BuildServiceProvider();

        await using (var scope = provider.CreateAsyncScope())
        {
            var context = scope.ServiceProvider.GetRequiredService<ItemsDbContext>();
            var queue = scope.ServiceProvider.GetRequiredService<IOutboxQueue<ItemsDbContext>>();
            Action<OutboxMessage> enqueue = path == EnqueuePath.Facade ? queue.Enqueue : context.EnqueueOutbox;

            enqueue(OutboxMessage.Create("k", "m0"));
            enqueue(OutboxMessage.Create("k", "m1").WithRoute("alerts"));
            enqueue(OutboxMessage.Create("k", "m2").WithRoute("audit"));
            enqueue(OutboxMessage.Create("k", "m3"));
            enqueue(OutboxMessage.Create("k", "m4").WithRoute("alerts"));

            context.Items.Add(new Item { Name = "t16" });
            await EfUnitOfWorkShape.CommitAsync(context, Ct);
        }

        Assert.Equal(
            [("m0", null), ("m1", "alerts"), ("m2", "audit"), ("m3", null), ("m4", "alerts")],
            await OutboxRouteRows.ReadAsync(schema));
        Assert.Empty(schema.OutboxLogs());
    }

    [Theory]
    [InlineData(Shape.Single)]
    [InlineData(Shape.Batch)]
    public async Task T16_publish_extension_writes_each_message_route(Shape shape)
    {
        var schema = await postgres.CreateSchemaAsync();
        await using (var context = schema.CreateContext())
        {
            await using var transaction = await context.Database.BeginTransactionAsync(Ct);
            context.Items.Add(new Item { Name = "t16" });
            await context.SaveChangesAsync(Ct);

            if (shape == Shape.Single)
            {
                await schema.Publisher.PublishAsync(context, OutboxMessage.Create("k", "m0").WithRoute("alerts"), Ct);
                await schema.Publisher.PublishAsync(context, OutboxMessage.Create("k", "m1"), Ct);
            }
            else
            {
                await schema.Publisher.PublishAsync(
                    context,
                    [OutboxMessage.Create("k", "m0").WithRoute("alerts"), OutboxMessage.Create("k", "m1"), OutboxMessage.Create("k", "m2").WithRoute("audit")],
                    Ct);
            }

            await transaction.CommitAsync(Ct);
        }

        (string, string?)[] expected = shape == Shape.Single
            ? [("m0", "alerts"), ("m1", null)]
            : [("m0", "alerts"), ("m1", null), ("m2", "audit")];
        Assert.Equal(expected, await OutboxRouteRows.ReadAsync(schema));
    }
}

internal static class OutboxRouteRows
{
    public static async Task<List<(string Payload, string? Route)>> ReadAsync(EfTestSchema schema)
    {
        await using var connection = new NpgsqlConnection(schema.ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand($"SELECT convert_from(payload, 'UTF8'), route FROM {schema.OutboxTable} ORDER BY id", connection);
        await using var reader = await command.ExecuteReaderAsync();
        var rows = new List<(string, string?)>();
        while (await reader.ReadAsync())
        {
            rows.Add((reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1)));
        }

        return rows;
    }
}
