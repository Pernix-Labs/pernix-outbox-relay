using System.Text.Json.Serialization;
using Microsoft.EntityFrameworkCore;

namespace Pernix.OutboxRelay.Client.Docs.Unit.Tests.Snippets;

public sealed class Account
{
    public Guid Id { get; set; }

    public decimal Balance { get; set; }
}

public sealed class Order
{
    public Guid Id { get; set; }

    public Guid AccountId { get; set; }

    public decimal Total { get; set; }
}

public sealed record OrderPlaced(Guid EventId, Guid OrderId, Guid AccountId, decimal Total);

public sealed record AccountDebited(Guid EventId, Guid AccountId, decimal Amount);

[JsonSerializable(typeof(OrderPlaced))]
[JsonSerializable(typeof(AccountDebited))]
public sealed partial class AppJson : JsonSerializerContext
{
}

public sealed class AppDb(DbContextOptions<AppDb> options) : DbContext(options)
{
    public DbSet<Account> Accounts => Set<Account>();

    public DbSet<Order> Orders => Set<Order>();
}

public sealed class SalesDb(DbContextOptions<SalesDb> options) : DbContext(options)
{
    public DbSet<Order> Orders => Set<Order>();
}

// Same member shape as a typical repository-abstractions IUnitOfWork, interceptor typed object?.
public interface IUnitOfWork
{
    Task CommitAsync(object? interceptor = null, CancellationToken cancellationToken = default);
}

public interface IAccountRepository
{
    Task<Account> GetAsync(Guid id, CancellationToken ct);
}

public sealed class RuleSet
{
    public Guid Id { get; set; }

    public int Version { get; set; }

    public string Definition { get; set; } = string.Empty;
}

public sealed record RuleSetChanged(Guid EventId, Guid RuleSetId);

[JsonSerializable(typeof(RuleSetChanged))]
public sealed partial class RiskJson : JsonSerializerContext
{
}

public sealed class RiskDb(DbContextOptions<RiskDb> options) : DbContext(options)
{
    public DbSet<RuleSet> RuleSets => Set<RuleSet>();
}

public interface IRuleSetRepository
{
    Task<RuleSet> GetAsync(Guid id, CancellationToken ct);
}
