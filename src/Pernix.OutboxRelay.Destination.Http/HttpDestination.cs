using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Security.Authentication;
using Microsoft.Extensions.Logging;
using Pernix.OutboxRelay.Core.Abstractions;

namespace Pernix.OutboxRelay.Destination.Http;

internal sealed class HttpDestination : IDestination
{
    private const string ContentTypeHeader = "Content-Type";
    private const int Unauthorized = 401;

    private static readonly UriCreationOptions RenderedUri = new() { DangerousDisablePathAndQueryCanonicalization = true };

    private static readonly DeliveryResult DisposedFault = new DeliveryResult.DestinationFault(nameof(ObjectDisposedException));

    private readonly HttpDestinationPlan plan;
    private readonly bool probe;
    private readonly HttpMessageInvoker invoker;
    private readonly HttpSendGate gate;
    private readonly ILogger logger;
    private readonly TimeProvider time;
    private readonly DeliveryResult timeoutFault;

    // Indexed by slot: the case-duplicate set HeaderForwarding reuses for rows with many headers.
    private readonly HashSet<string>?[] seenBySlot;
    private int disposed;

    public HttpDestination(
        HttpDestinationPlan plan,
        DestinationContext context,
        ILogger logger,
        Func<HandlerSettings, HttpMessageHandler>? handlers = null,
        TimeProvider? time = null)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(logger);

        this.plan = plan;
        this.logger = logger;
        this.time = time ?? TimeProvider.System;
        probe = context.IsProbe;
        timeoutFault = new DeliveryResult.DestinationFault($"timeout after {plan.SendTimeout}");
        seenBySlot = new HashSet<string>?[plan.MaxRequestsInFlight];
        gate = new HttpSendGate(plan.MaxRequestsInFlight, CreateSlot, this.time);

        var handler = handlers is null ? HttpHandlerFactory.Create(plan.Handler) : handlers(plan.Handler);
        invoker = new HttpMessageInvoker(handler, disposeHandler: true);
        logger.HttpClientCreated(plan.Name, plan.Host, plan.MaxRequestsInFlight);
    }

    public TimeSpan? SendBound => plan.SendTimeout;

    public IReadOnlySet<string>? Routes => plan.Endpoints.Routes;

    internal HttpSendGate Gate => gate;

    public async Task ValidateAsync(CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed) != 0, this);

        if (plan.Auth.TokenCache is { } tokens)
        {
            var token = await tokens.GetAsync(ct).ConfigureAwait(false);
            if (token.Fault is { } fault)
            {
                throw new DestinationValidationException(
                    $"Destinations[{plan.Name}]: OAuth2 token from host '{tokens.TokenHost}' failed: {fault.Error}");
            }
        }

        if (plan.Probe is { } health)
        {
            await ProbeAsync(health, ct).ConfigureAwait(false);
        }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<DeliveryResult> SendAsync(OutgoingMessage message, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (probe)
        {
            throw new InvalidOperationException($"Destinations[{plan.Name}]: a probe instance only validates");
        }

        var endpoint = plan.Endpoints.Resolve(message)
                       ?? throw new ArgumentException(ContractViolation(message), nameof(message));

        SendContext? context = null;
        HttpSendSlot? slot = null;
        try
        {
            context = gate.Rent(plan.SendTimeout, ct);
            slot = await gate.AcquireAsync(context).ConfigureAwait(false);
            var deadline = context.Deadline.Token;
            var row = RenderContext.From(message);

            Uri uri;
            if (endpoint.StaticUri is { } fixedUri)
            {
                uri = fixedUri;
            }
            else
            {
                if (!endpoint.UrlTemplate!.TryRenderUrl(row, ref slot.UrlBuffer, out var url, out var urlFault))
                {
                    return urlFault;
                }

                uri = new Uri(url, in RenderedUri);
            }

            using var request = new HttpRequestMessage(endpoint.Method, uri)
            {
                Version = plan.Handler.RequestVersion,
                VersionPolicy = plan.Handler.VersionPolicy,
            };

            // Content first: forwarded and Headers-map content headers land on it.
            var content = new ReadOnlyMemoryContent(message.Payload);
            request.Content = content;

            if (!plan.Forwarding.TryForward(
                    message.Headers,
                    request.Headers,
                    content.Headers,
                    endpoint.Headers.Overridden,
                    slot.Values,
                    slot.Names,
                    ref seenBySlot[slot.Index],
                    out var forwardedContentType,
                    out var forwardFault))
            {
                return forwardFault;
            }

            content.Headers.TryAddWithoutValidation(ContentTypeHeader, forwardedContentType ?? endpoint.ContentType);

            if (!endpoint.Headers.TryApply(row, request.Headers, content.Headers, out var headerFault))
            {
                return headerFault;
            }

            // Relay-owned headers go last and win.
            string? token = null;
            if (plan.Auth.TokenCache is { } tokens)
            {
                var fetched = await tokens.GetAsync(deadline).ConfigureAwait(false);
                if (fetched.Fault is { } tokenFault)
                {
                    return tokenFault;
                }

                token = fetched.HeaderValue!;
                request.Headers.TryAddWithoutValidation(AuthPlan.AuthorizationHeader, token);
            }
            else if (plan.Auth.HeaderName is { } authName)
            {
                request.Headers.TryAddWithoutValidation(authName, plan.Auth.HeaderValue);
            }

            if (message.DeadLetter is not null)
            {
                HttpDeadLetterHeaders.Apply(message, request.Headers);
            }

            if (slot.Signer is { } signer)
            {
                string? webhookId = null;
                if (endpoint.WebhookId is { } idTemplate && !idTemplate.TryRenderHeaderValue(row, out webhookId, out var idFault))
                {
                    return idFault;
                }

                if (!signer.TrySign(request, message.Payload, row, out var signFault, webhookId))
                {
                    return signFault;
                }
            }

            using var response = await invoker.SendAsync(request, deadline).ConfigureAwait(false);
            var status = (int)response.StatusCode;
            if (status == Unauthorized && token is not null)
            {
                plan.Auth.TokenCache!.Invalidate(token);
            }

            // The body is never read: disposal drains it up to MaxResponseDrainSize so the connection is reused.
            return endpoint.Classify(status);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            return slot is null ? HttpSendGate.SlotWaitTimeout : timeoutFault;
        }
        catch (ObjectDisposedException) when (ct.IsCancellationRequested)
        {
            throw new OperationCanceledException(ct);
        }
        catch (ObjectDisposedException)
        {
            return DisposedFault;
        }
        catch (HttpRequestException exception)
        {
            return new DeliveryResult.DestinationFault($"{nameof(HttpRequestException)}: {exception.HttpRequestError}");
        }
        catch (Exception exception) when (exception is IOException or SocketException or AuthenticationException)
        {
            return new DeliveryResult.DestinationFault(exception.GetType().Name);
        }
        finally
        {
            if (slot is not null)
            {
                gate.Release(slot);
            }

            if (context is not null)
            {
                gate.Return(context);
            }
        }
    }

    public ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref disposed, 1) == 0)
        {
            gate.Dispose();
            invoker.Dispose();
        }

        return ValueTask.CompletedTask;
    }

    private HttpSendSlot CreateSlot(int index) =>
        new(index, plan.Signing is { } signing ? new RequestSigner(signing, time) : null, plan.Forwarding.CreateNameCache());

    private async Task ProbeAsync(HealthProbePlan health, CancellationToken ct)
    {
        var host = health.Url.Host;
        var method = health.Method.Method;
        string failure;
        using (var deadline = CancellationTokenSource.CreateLinkedTokenSource(ct))
        {
            deadline.CancelAfter(plan.SendTimeout);
            try
            {
                using var request = new HttpRequestMessage(health.Method, health.Url)
                {
                    Version = plan.Handler.RequestVersion,
                    VersionPolicy = plan.Handler.VersionPolicy,
                };
                using var response = await invoker.SendAsync(request, deadline.Token).ConfigureAwait(false);
                var status = (int)response.StatusCode;
                if (health.IsExpected(status))
                {
                    return;
                }

                failure = $"HTTP {status}";
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (OperationCanceledException)
            {
                failure = $"timeout after {plan.SendTimeout}";
            }
            catch (HttpRequestException exception)
            {
                failure = $"{nameof(HttpRequestException)}: {exception.HttpRequestError}";
            }
            catch (Exception exception) when (exception is IOException or SocketException or AuthenticationException)
            {
                failure = exception.GetType().Name;
            }
        }

        logger.HttpProbeFailed(plan.Name, host, method, failure);
        throw new DestinationValidationException($"Destinations[{plan.Name}]: health probe {method} to host '{host}' failed: {failure}");
    }

    // Failure branch only. Unreachable under the IDestination contract: Core sends only accepted rows.
    private string ContractViolation(OutgoingMessage message)
    {
        if (plan.Endpoints.Routes is not null && message.DeadLetter is not null)
        {
            return $"Destinations[{plan.Name}]: dead letter on a Routes destination";
        }

        var shown = message.Route is { } route ? $"'{route}'" : "(none)";
        return $"Destinations[{plan.Name}]: route {shown} from outbox '{message.Source.Outbox}' is not accepted by this destination";
    }
}
