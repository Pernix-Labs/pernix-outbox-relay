using System.Data.Common;
using Microsoft.EntityFrameworkCore.Diagnostics;

namespace Pernix.OutboxRelay.Client.EntityFrameworkCore.Integration.Tests;

public sealed class CommandCountingInterceptor : IDbCommandInterceptor, IDbConnectionInterceptor
{
    private int commands;
    private int connectionOpens;

    public int Commands => Volatile.Read(ref commands);

    public int ConnectionOpens => Volatile.Read(ref connectionOpens);

    public InterceptionResult<DbDataReader> ReaderExecuting(DbCommand command, CommandEventData eventData, InterceptionResult<DbDataReader> result)
    {
        Interlocked.Increment(ref commands);
        return result;
    }

    public ValueTask<InterceptionResult<DbDataReader>> ReaderExecutingAsync(
        DbCommand command, CommandEventData eventData, InterceptionResult<DbDataReader> result, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref commands);
        return new(result);
    }

    public InterceptionResult<int> NonQueryExecuting(DbCommand command, CommandEventData eventData, InterceptionResult<int> result)
    {
        Interlocked.Increment(ref commands);
        return result;
    }

    public ValueTask<InterceptionResult<int>> NonQueryExecutingAsync(
        DbCommand command, CommandEventData eventData, InterceptionResult<int> result, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref commands);
        return new(result);
    }

    public InterceptionResult<object> ScalarExecuting(DbCommand command, CommandEventData eventData, InterceptionResult<object> result)
    {
        Interlocked.Increment(ref commands);
        return result;
    }

    public ValueTask<InterceptionResult<object>> ScalarExecutingAsync(
        DbCommand command, CommandEventData eventData, InterceptionResult<object> result, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref commands);
        return new(result);
    }

    public InterceptionResult ConnectionOpening(DbConnection connection, ConnectionEventData eventData, InterceptionResult result)
    {
        Interlocked.Increment(ref connectionOpens);
        return result;
    }

    public ValueTask<InterceptionResult> ConnectionOpeningAsync(
        DbConnection connection, ConnectionEventData eventData, InterceptionResult result, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref connectionOpens);
        return new(result);
    }
}
