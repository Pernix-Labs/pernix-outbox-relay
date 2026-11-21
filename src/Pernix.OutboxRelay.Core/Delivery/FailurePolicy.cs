using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;

namespace Pernix.OutboxRelay.Core.Delivery;

// Closed: the term handles every decision.
internal abstract record FailureDecision
{
    private FailureDecision()
    {
    }

    public sealed record Retry(Outcome.Fail Outcome) : FailureDecision;

    public sealed record DeadLetter(DeadLetterInfo Info) : FailureDecision;

    public sealed record Drop(Outcome.Remove Outcome, string Reason) : FailureDecision;

    public sealed record Block(Outcome.Block Outcome, string Reason) : FailureDecision;
}

// Pure retry, dead-letter and Continue/Block rules. Saturating int math throughout.
// Block is never configured with Ordering.Mode None (startup refusal).
internal sealed class FailurePolicy(RetryOptions retry, bool deadLetterConfigured, OnPermanentFailure onPermanent)
{
    internal const string PendingReason = "dead letter pending";

    // Hold marker in last_error. Read only inside this class.
    internal const string HoldPrefix = "route not mapped: ";
    internal const string NoRoute = "(none)";
    internal const int HoldRouteMax = 63;
    internal const string HoldRouteTruncated = "...(truncated)";
    private const string NoRouteHoldError = HoldPrefix + NoRoute;
    internal const string FaultEscape = "fault: ";

    private int Max => retry.MaxRetryAttempts;

    // The marker test runs only past Max, so the route-less path pays nothing.
    public bool IsDeadLetterPending(OutboxMessage row) => row.RetryCount > Max && !IsHeld(row);

    // Cut at 62 when char 63 is a high surrogate, so the capped text still encodes on commit.
    internal static string HoldRoute(string? route)
    {
        if (route is null)
        {
            return NoRoute;
        }

        if (route.Length <= HoldRouteMax)
        {
            return route;
        }

        var n = char.IsHighSurrogate(route[HoldRouteMax - 1]) ? HoldRouteMax - 1 : HoldRouteMax;
        return string.Concat(route.AsSpan(0, n), HoldRouteTruncated);
    }

    public Outcome.Fail Hold(OutboxMessage row, string heldRoute) => new(row.Id)
    {
        ReadRetryCount = row.RetryCount,
        RetryCount = Math.Max(row.RetryCount, 1),
        Delay = retry.MaxDelay,
        Error = row.Route is null ? NoRouteHoldError : HoldPrefix + heldRoute,
    };

    public FailureDecision OnMessageFault(OutboxMessage row, DeliveryResult.MessageFault fault, DateTimeOffset now)
    {
        var next = Increment(Counted(row));
        if (!fault.Permanent && next <= Max)
        {
            return new FailureDecision.Retry(new Outcome.Fail(row.Id)
            {
                ReadRetryCount = row.RetryCount,
                RetryCount = next,
                Delay = Backoff.Delay(retry, next),
                Error = Escape(fault.Error),
            });
        }

        return PastRetries(row, fault.Error, now);
    }

    public FailureDecision OnDeadLetterPendingRead(OutboxMessage row, DateTimeOffset now) =>
        PastRetries(row, Unescape(row.LastError) ?? PendingReason, now);

    // reason is the original failure reason, not the dead-letter publish result.
    public FailureDecision AfterDeadLetter(OutboxMessage row, string reason) => FinalDecision(row, reason);

    // reason is the original failure reason, not the dead-letter publish error.
    public FailureDecision.Retry OnDeadLetterFailed(OutboxMessage row, string reason)
    {
        var retryCount = Math.Max(Increment(Counted(row)), Increment(Max));
        return new FailureDecision.Retry(new Outcome.Fail(row.Id)
        {
            ReadRetryCount = row.RetryCount,
            RetryCount = retryCount,
            Delay = Backoff.Delay(retry, Math.Max(1, retryCount - Max)),
            Error = Escape(reason),
        });
    }

    private FailureDecision PastRetries(OutboxMessage row, string reason, DateTimeOffset now)
    {
        if (!deadLetterConfigured)
        {
            return FinalDecision(row, reason);
        }

        var attempts = Math.Min(Increment(Counted(row)), Increment(Max));
        return new FailureDecision.DeadLetter(new DeadLetterInfo(reason, attempts, now));
    }

    private FailureDecision FinalDecision(OutboxMessage row, string reason) => onPermanent switch
    {
        OnPermanentFailure.Block => new FailureDecision.Block(new Outcome.Block(row.Id)
        {
            ReadRetryCount = row.RetryCount,
            RetryCount = Increment(Counted(row)),
            Error = Escape(reason),
        }, reason),
        OnPermanentFailure.Continue => new FailureDecision.Drop(new Outcome.Remove(row.Id), reason),
        _ => throw new ArgumentOutOfRangeException(nameof(onPermanent), onPermanent, "Unknown Ordering.OnPermanentFailure."),
    };

    // Text starting with HoldPrefix or FaultEscape gains one FaultEscape, so it never reads as held
    // and Unescape restores it exactly. Same instance otherwise.
    private static string Escape(string error) =>
        StartsWithHoldMarker(error) || error.StartsWith(FaultEscape, StringComparison.Ordinal)
            ? FaultEscape + error
            : error;

    // Reverses Escape on read, so a re-sent dead letter carries the original reason. Same instance otherwise.
    private static string? Unescape(string? error) =>
        error is not null && error.StartsWith(FaultEscape, StringComparison.Ordinal)
            ? error[FaultEscape.Length..]
            : error;

    private static int Counted(OutboxMessage row) => IsHeld(row) ? 0 : row.RetryCount;

    private static bool IsHeld(OutboxMessage row) => StartsWithHoldMarker(row.LastError);

    private static bool StartsWithHoldMarker(string? text) => text is not null && text.StartsWith(HoldPrefix, StringComparison.Ordinal);

    private static int Increment(int value) => value == int.MaxValue ? value : value + 1;
}
