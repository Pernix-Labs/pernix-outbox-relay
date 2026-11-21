namespace Pernix.OutboxRelay.Core.Abstractions;

public sealed class OutboxValidationException(string message) : Exception(message);
