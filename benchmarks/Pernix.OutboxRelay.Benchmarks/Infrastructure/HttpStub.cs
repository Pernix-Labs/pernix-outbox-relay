using System.Net;

namespace Pernix.OutboxRelay.Benchmarks.Infrastructure;

// One shared response: its Dispose is idempotent and its content is never created, so it survives every send.
internal sealed class HttpStub : HttpMessageHandler
{
    private readonly Task<HttpResponseMessage> response;
    private long requests;

    public HttpStub(int status)
    {
        response = Task.FromResult(new HttpResponseMessage((HttpStatusCode)status));
    }

    public long Requests => Interlocked.Read(ref requests);

    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref requests);
        return response;
    }

    protected override HttpResponseMessage Send(HttpRequestMessage request, CancellationToken cancellationToken) =>
        throw new NotSupportedException("stub: synchronous send");
}
