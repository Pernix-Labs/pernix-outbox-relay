namespace Pernix.OutboxRelay.Core.Configuration;

public enum OrderingMode { None, PerKey, Strict }

public enum OnPermanentFailure { Continue, Block }
