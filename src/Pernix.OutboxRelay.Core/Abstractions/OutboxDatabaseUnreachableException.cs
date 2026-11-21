namespace Pernix.OutboxRelay.Core.Abstractions;

public sealed class OutboxDatabaseUnreachableException(string message) : Exception(message);
