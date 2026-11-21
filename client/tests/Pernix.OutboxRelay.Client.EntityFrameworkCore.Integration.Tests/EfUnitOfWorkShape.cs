using Microsoft.EntityFrameworkCore;

namespace Pernix.OutboxRelay.Client.EntityFrameworkCore.Integration.Tests;

// EfUnitOfWork.ConcreteCommitAsync at HEAD 0954087, no explicit rollback.
public static class EfUnitOfWorkShape
{
    public static async Task CommitAsync(DbContext context, CancellationToken ct)
    {
        await using var transaction = await context.Database.BeginTransactionAsync(ct);
        context.ChangeTracker.DetectChanges();
        await context.SaveChangesAsync(ct);
        await transaction.CommitAsync(ct);
    }
}
