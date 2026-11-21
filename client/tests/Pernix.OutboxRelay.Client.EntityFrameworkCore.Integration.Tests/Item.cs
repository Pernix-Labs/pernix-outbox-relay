namespace Pernix.OutboxRelay.Client.EntityFrameworkCore.Integration.Tests;

public sealed class Item
{
    public int Id { get; set; }

    public required string Name { get; set; }
}
