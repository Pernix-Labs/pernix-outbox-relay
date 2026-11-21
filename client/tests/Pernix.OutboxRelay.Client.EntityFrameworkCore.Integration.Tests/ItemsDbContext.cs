using Microsoft.EntityFrameworkCore;

namespace Pernix.OutboxRelay.Client.EntityFrameworkCore.Integration.Tests;

public class ItemsDbContext(DbContextOptions<ItemsDbContext> options) : DbContext(options)
{
    public DbSet<Item> Items => Set<Item>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Item>(item =>
        {
            item.ToTable("items");
            item.Property(x => x.Id).HasColumnName("id");
            item.Property(x => x.Name).HasColumnName("name");
        });
    }
}
