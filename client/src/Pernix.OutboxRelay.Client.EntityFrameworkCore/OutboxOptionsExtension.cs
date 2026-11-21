using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

namespace Pernix.OutboxRelay.Client.EntityFrameworkCore;

internal sealed class OutboxOptionsExtension : IDbContextOptionsExtension
{
    private DbContextOptionsExtensionInfo? info;

    public OutboxOptionsExtension(OutboxTable table)
    {
        Table = table;
    }

    public OutboxTable Table { get; }

    public DbContextOptionsExtensionInfo Info => info ??= new ExtensionInfo(this);

    public void ApplyServices(IServiceCollection services)
    {
        services.AddScoped<OutboxQueue>();
        services.AddScoped<IResettableService>(static sp => sp.GetRequiredService<OutboxQueue>());
    }

    public void Validate(IDbContextOptions options)
    {
    }

    private sealed class ExtensionInfo : DbContextOptionsExtensionInfo
    {
        public ExtensionInfo(IDbContextOptionsExtension extension)
            : base(extension)
        {
        }

        public override bool IsDatabaseProvider => false;

        public override string LogFragment => "using Pernix outbox ";

        public override int GetServiceProviderHashCode() => 0;

        public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other) => other is ExtensionInfo;

        public override void PopulateDebugInfo(IDictionary<string, string> debugInfo) => debugInfo["Pernix:Outbox"] = "1";
    }
}
