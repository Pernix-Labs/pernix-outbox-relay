namespace Pernix.OutboxRelay.Core.Abstractions;

public sealed class DestinationValidationException(string message) : Exception(message);
