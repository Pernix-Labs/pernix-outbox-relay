using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;

namespace Pernix.OutboxRelay.Core.Leadership;

public sealed record DestinationBinding(DestinationOptions Options, IDestinationFactory Factory)
{
    public IDestination Create(DestinationContext context) => Factory.Create(Options, context);
}

public sealed record ValidatedOutbox(OutboxOptions Options, IValidatedOutboxStore Store, DestinationOptions Destination, IDestinationFactory DestinationFactory)
{
    // Null when DeadLetter.Destination is absent or empty (dead-lettering off).
    public DestinationBinding? DeadLetterDestination { get; init; }
}

public sealed class ValidatedOutboxes
{
    private readonly List<ValidatedOutbox> entries = [];

    public IReadOnlyList<ValidatedOutbox> Entries => entries;

    public void Add(ValidatedOutbox outbox)
    {
        ArgumentNullException.ThrowIfNull(outbox);
        entries.Add(outbox);
    }
}
