using Microsoft.Extensions.Configuration;

namespace Pernix.OutboxRelay.Core.Configuration;

// The relay reads only this section; keys inside it are section-relative.
public sealed class RelayConfiguration(IConfiguration section)
{
    public const string SectionName = "OutboxRelay";

    public IConfiguration Section { get; } = section ?? throw new ArgumentNullException(nameof(section));
}
