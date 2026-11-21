namespace Pernix.OutboxRelay.Core.Configuration;

public sealed class RelayStartupValidationException(IReadOnlyList<string> failures)
    : Exception(FormatMessage(failures))
{
    public IReadOnlyList<string> Failures { get; } = failures;

    private static string FormatMessage(IReadOnlyList<string> failures) =>
        $"Relay startup validation failed ({failures.Count}):" + string.Concat(failures.Select(f => $"\n- {f}"));
}
