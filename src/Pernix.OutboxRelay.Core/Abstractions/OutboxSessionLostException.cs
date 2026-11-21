namespace Pernix.OutboxRelay.Core.Abstractions;

public sealed class OutboxSessionLostException(string message) : Exception(message);
