using Microsoft.Extensions.Configuration;

namespace Pernix.OutboxRelay.Core.Abstractions;

// Reports failures for a provider-owned key under the OutboxRelay section the validator bound from, in the configuration stage,
// before any IDestinationFactory.Validate. RelayStartupValidator passes that same section. Failure paths are section-relative and never carry values.
public interface IRelaySectionValidator
{
    IReadOnlyList<string> Validate(IConfiguration section);
}
