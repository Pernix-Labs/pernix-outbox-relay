namespace Pernix.OutboxRelay.Benchmarks.Infrastructure;

// Marks suites that need PostgreSQL, so the bdn host starts one container for the run.
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class NeedsPostgreSqlAttribute : Attribute
{
}
