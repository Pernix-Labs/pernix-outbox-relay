using Microsoft.Extensions.Configuration;

namespace Pernix.OutboxRelay.Core.Abstractions;

// Binds and checks a provider-owned key under the OutboxRelay section in the configuration stage, before any IDestinationFactory.Validate.
// Failure paths are section-relative and never carry values.
public interface IRelaySectionValidator
{
    IReadOnlyList<string> Validate(IConfiguration section);
}
