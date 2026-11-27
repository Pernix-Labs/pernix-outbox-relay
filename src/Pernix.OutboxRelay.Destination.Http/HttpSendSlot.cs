namespace Pernix.OutboxRelay.Destination.Http;

internal sealed class HttpSendSlot : IDisposable
{
    internal const int InitialUrlBufferLength = 256;

    public HttpSendSlot(int index, RequestSigner? signer = null, PrefixedNameCache? names = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(index);
        Index = index;
        Signer = signer;
        Names = names;
    }

    public int Index { get; }

    // Owns the reused IncrementalHash and the per-second timestamp cache.
    public RequestSigner? Signer { get; }

    public HeaderValueCache Values { get; } = new();

    public PrefixedNameCache? Names { get; }

    public char[] UrlBuffer = new char[InitialUrlBufferLength];

    public void Dispose() => Signer?.Dispose();
}
