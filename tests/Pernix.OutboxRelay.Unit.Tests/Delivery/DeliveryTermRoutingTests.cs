using System.Collections.Frozen;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Core.Delivery;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Delivery;

public sealed partial class DeliveryTermTests
{
    private const int MessageHeldEvent = 125;
    private static readonly TimeSpan HoldDelay = TimeSpan.FromSeconds(30);
    private static readonly FrozenSet<string> AcceptedRoutes = new[] { "orders", "payments" }.ToFrozenSet(StringComparer.Ordinal);

    public static TheoryData<OrderingMode> Modes => new(OrderingMode.None, OrderingMode.PerKey, OrderingMode.Strict);

    public static TheoryData<OrderingMode, bool, string?, bool> AcceptanceMatrix
    {
        get
        {
            var data = new TheoryData<OrderingMode, bool, string?, bool>();
            foreach (var ordered in new[] { OrderingMode.None, OrderingMode.PerKey, OrderingMode.Strict })
            {
                data.Add(ordered, false, null, true);
                data.Add(ordered, false, "orders", false);
                data.Add(ordered, false, "billing", false);
                data.Add(ordered, true, null, false);
                data.Add(ordered, true, "orders", true);
                data.Add(ordered, true, "billing", false);
            }

            return data;
        }
    }

    public static TheoryData<OrderingMode, int, bool, OnPermanentFailure> HoldBudgetCases
    {
        get
        {
            var data = new TheoryData<OrderingMode, int, bool, OnPermanentFailure>();
            foreach (var ordered in new[] { OrderingMode.None, OrderingMode.PerKey, OrderingMode.Strict })
            {
                foreach (var max in new[] { 3, 0 })
                {
                    foreach (var dead in new[] { false, true })
                    {
                        data.Add(ordered, max, dead, OnPermanentFailure.Continue);

                        // Block is refused for Mode None at startup.
                        if (ordered != OrderingMode.None)
                        {
                            data.Add(ordered, max, dead, OnPermanentFailure.Block);
                        }
                    }
                }
            }

            return data;
        }
    }

    [Theory]
    [MemberData(nameof(AcceptanceMatrix))]
    public async Task Term_sends_only_accepted_rows_with_the_row_route(OrderingMode ordered, bool routed, string? routeText, bool accepted)
    {
        UseMode(ordered);
        destination.Routes = routed ? AcceptedRoutes : null;
        // A fresh instance, so reference equality proves the copy.
        var route = routeText is null ? null : new string(routeText.AsSpan());
        session.Insert(RoutedRow(1, 0xA, route));

        await StartAsync();

        if (accepted)
        {
            var message = Assert.Single(destination.Messages);
            Assert.Same(route, message.Route);
            Assert.Empty(logger.WithEvent(MessageHeldEvent));
            Assert.Empty(session.TableIds);
            return;
        }

        Assert.Empty(destination.Sent);
        Assert.Equal(
            [new Outcome.Fail(1) { ReadRetryCount = 0, RetryCount = 1, Delay = HoldDelay, Error = FailurePolicy.HoldPrefix + (routeText ?? FailurePolicy.NoRoute) }],
            session.Commits.SelectMany(c => c.Outcomes));

        // Never in flight: stop finishes without the drain deadline.
        await ActAsync(stopping.Cancel);
        Assert.IsType<DeliveryTermResult.Stopped>(await WaitForTermAsync());
        Assert.Equal(TimeSpan.Zero, Now);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task Hold_scope_follows_the_ordering_mode(OrderingMode ordered)
    {
        UseMode(ordered);
        destination.Routes = AcceptedRoutes;
        session.Insert(RoutedRow(1, 0xA, "billing"), RoutedRow(2, 0xA, "orders"), RoutedRow(3, 0xB, "orders"));

        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(5));

        long[] expected = ordered switch
        {
            OrderingMode.None => [2L, 3L],
            OrderingMode.PerKey => [3L],
            _ => [],
        };
        Assert.Equal(expected, destination.Sent.Order());
        Assert.Single(logger.WithEvent(MessageHeldEvent));
        Assert.Contains(1L, session.TableIds);
    }

    [Theory]
    [MemberData(nameof(HoldBudgetCases))]
    public async Task Hold_spends_no_retry_and_never_dead_letters_drops_or_blocks(OrderingMode ordered, int max, bool dead, OnPermanentFailure onPermanent)
    {
        UseMode(ordered, onPermanent);
        options.Retry.MaxRetryAttempts = max;
        withDeadLetter = dead;
        destination.Routes = AcceptedRoutes;
        session.Insert(RoutedRow(1, 0xA, "billing"));

        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(65));

        var outcomes = session.Commits.SelectMany(c => c.Outcomes).ToList();
        Assert.True(outcomes.Count >= 2, $"expected a re-hold, got {outcomes.Count} outcomes");
        Assert.All(outcomes, o =>
        {
            var fail = Assert.IsType<Outcome.Fail>(o);
            Assert.Equal(1, fail.RetryCount);
            Assert.Equal("route not mapped: billing", fail.Error);
        });
        Assert.Equal(outcomes.Count, logger.WithEvent(MessageHeldEvent).Count());
        Assert.Empty(deadLetter.Sent);
        Assert.Empty(destination.Sent);
        var row = session.Get(1)!;
        Assert.Equal(1, row.RetryCount);
        Assert.NotNull(row.NextAttemptAt);
        Assert.Empty(logger.WithEvent(118));
        Assert.Empty(logger.WithEvent(120));
        Assert.Empty(logger.WithEvent(123));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task Hold_starts_no_pause_records_no_metric_and_logs_once(OrderingMode ordered)
    {
        using var measurements = new MeasurementRecorder(outbox);
        UseMode(ordered);
        session.Insert(RoutedRow(1, 0xA, null), RoutedRow(2, 0xB, "orders"), RoutedRow(3, 0xC, null));

        await StartAsync();

        long[] expected = ordered == OrderingMode.Strict ? [1L] : [1L, 3L];
        Assert.Equal(expected, destination.Sent.Order());
        Assert.Empty(measurements.Tagged);
        Assert.Equal(expected.Length, measurements.Delivered);
        Assert.Empty(logger.WithEvent(110));
        Assert.Empty(logger.WithEvent(117));
        var heldLog = Assert.Single(logger.WithEvent(MessageHeldEvent));
        Assert.Equal(LogLevel.Warning, heldLog.Level);
        Assert.Equal($"Message 2 of outbox {outbox} held: destination {Destination} does not accept route orders", heldLog.Message);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task Hold_caps_the_route_text_in_the_log_and_the_error(OrderingMode ordered)
    {
        UseMode(ordered);
        var exact = new string('a', 63);
        var longer = new string('b', 63) + "cde";
        session.Insert(RoutedRow(1, 0xA, exact), RoutedRow(2, 0xB, longer));

        await StartAsync();

        var capped = new string('b', 63) + FailurePolicy.HoldRouteTruncated;
        var errors = session.Commits.SelectMany(c => c.Outcomes).OfType<Outcome.Fail>().ToDictionary(f => f.Id, f => f.Error);
        var logs = logger.WithEvent(MessageHeldEvent).Select(e => e.Message).ToList();
        if (ordered == OrderingMode.Strict)
        {
            Assert.Equal([1L], errors.Keys);
        }
        else
        {
            Assert.Equal(FailurePolicy.HoldPrefix + capped, errors[2]);
            Assert.Contains(logs, m => m.EndsWith($"does not accept route {capped}", StringComparison.Ordinal));
        }

        Assert.Equal(FailurePolicy.HoldPrefix + exact, errors[1]);
        Assert.Contains(logs, m => m.EndsWith($"does not accept route {exact}", StringComparison.Ordinal));
        Assert.DoesNotContain(logs, m => m.Contains(longer, StringComparison.Ordinal));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task Still_unaccepted_held_row_is_held_again(OrderingMode ordered)
    {
        UseMode(ordered);
        destination.Routes = AcceptedRoutes;
        session.Insert(FakeOutboxSession.Row(1, retryCount: 2, lastError: "route not mapped: billing") with { Key = [0xA], Route = "billing" });

        await StartAsync();

        Assert.Equal(
            [new Outcome.Fail(1) { ReadRetryCount = 2, RetryCount = 2, Delay = HoldDelay, Error = "route not mapped: billing" }],
            session.Commits.SelectMany(c => c.Outcomes));
        Assert.Single(logger.WithEvent(MessageHeldEvent));
        Assert.Empty(destination.Sent);
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task Now_accepted_held_row_is_sent(OrderingMode ordered)
    {
        UseMode(ordered);
        destination.Routes = AcceptedRoutes;
        var route = new string("orders".AsSpan());
        session.Insert(FakeOutboxSession.Row(1, retryCount: 1, lastError: "route not mapped: orders") with { Key = [0xA], Route = route });

        await StartAsync();

        Assert.Same(route, Assert.Single(destination.Messages).Route);
        Assert.Empty(session.TableIds);
        Assert.Empty(logger.WithEvent(MessageHeldEvent));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task Real_fault_after_release_counts_from_zero(OrderingMode ordered)
    {
        UseMode(ordered);
        withDeadLetter = true;
        options.Retry.MaxRetryAttempts = 3;
        destination.Routes = AcceptedRoutes;
        session.Insert(FakeOutboxSession.Row(1, retryCount: 5, lastError: "route not mapped: orders") with { Key = [0xA], Route = "orders" });
        destination.Respond = _ => new DeliveryResult.MessageFault("boom", Permanent: false);

        await StartAsync();

        Assert.Equal([1L], destination.Sent);
        Assert.Empty(deadLetter.Sent);
        Assert.Equal(
            [new Outcome.Fail(1) { ReadRetryCount = 5, RetryCount = 1, Delay = TimeSpan.FromSeconds(10), Error = "boom" }],
            session.Commits.SelectMany(c => c.Outcomes));
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task Dead_letter_pending_row_with_unaccepted_route_is_dead_lettered_with_its_route(OrderingMode ordered)
    {
        UseMode(ordered);
        withDeadLetter = true;
        options.Retry.MaxRetryAttempts = 3;
        destination.Routes = AcceptedRoutes;
        var route = new string("billing".AsSpan());
        session.Insert(FakeOutboxSession.Row(1, retryCount: 4, lastError: "boom") with { Key = [0xA], Route = route });

        await StartAsync();

        Assert.Same(route, Assert.Single(deadLetter.Messages).Route);
        Assert.Empty(destination.Sent);
        Assert.Empty(logger.WithEvent(MessageHeldEvent));
        Assert.Empty(session.TableIds);
        Assert.DoesNotContain(
            session.Commits.SelectMany(c => c.Outcomes),
            o => o is Outcome.Fail { Error: { } e } && e.StartsWith(FailurePolicy.HoldPrefix, StringComparison.Ordinal));
    }

    private void UseMode(OrderingMode ordered, OnPermanentFailure onPermanent = OnPermanentFailure.Continue)
    {
        if (ordered != OrderingMode.None)
        {
            Ordered(ordered, onPermanent);
        }
    }

    private static OutboxMessage RoutedRow(long id, byte key, string? route) => Keyed(id, key) with { Route = route };
}
