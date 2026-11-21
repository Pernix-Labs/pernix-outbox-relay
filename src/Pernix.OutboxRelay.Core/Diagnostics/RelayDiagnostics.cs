using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Pernix.OutboxRelay.Core.Diagnostics;

public static class RelayDiagnostics
{
    public const string Name = "Pernix.OutboxRelay";

    public static readonly Meter Meter = new(Name);

    public static readonly ActivitySource ActivitySource = new(Name);
}
