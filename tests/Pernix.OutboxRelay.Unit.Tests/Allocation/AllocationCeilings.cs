namespace Pernix.OutboxRelay.Unit.Tests.Allocation;

// One ceiling per guard cell. Limit = Median + Margin. NaN = unset, the guard fails the cell.
internal sealed record Ceiling(
    string Cell,
    string Unit,
    double Limit,
    double Median,
    double Spread,
    double Margin,
    string MarginReason,
    string Exceeds,
    string Within);

internal sealed record MeasuredOn(string Machine, string Commit, string Date, int Runs);

internal static class AllocationCeilings
{
    public const string TenPercentReason = "10% of median";
    public const string ThreeSpread = "3 × spread of the ceiling runs exceeds 10%";
    public const string IdleNoise = "idle noise, --rounds 1";

    // term kafka and dlq rows re-measured at 835b04a. Http rows measured at c9fe4f1 on the M6. Every other row is from 85e6bf6.
    public static readonly MeasuredOn MeasuredOn = new(
        "Apple M5, 10 cores, 24 GB RAM, macOS 27.2 (26B123), arm64; Http rows Apple M6, 12 cores, 32 GB RAM, macOS 27.2.1 (26B131), arm64",
        "85e6bf6, term kafka/dlq 835b04a, Http c9fe4f1",
        "2026-11-10, term kafka/dlq 2026-11-14, Http 2026-11-26",
        5);

    public static readonly Ceiling[] All =
    [
        new("term None null b500 h1", "msg", 212.1, 192.8, 0.0, 19.3, TenPercentReason, "none", "none"),
        new("term None kafka b500 h1", "msg", 260.9, 213.8, 15.7, 47.1, ThreeSpread, "raw produce −676.3, Kafka routes −234.0, core loop −399.5", "none"),
        new("term None dlq b500 h1", "dlq-msg", 1010.7, 918.8, 8.8, 91.9, TenPercentReason, "raw produce −1,221.1, Kafka routes −1,080.0, core loop −222.2, dead-letter headers −342.0", "none"),
        new("term PerKey null b500 h1", "msg", 212.2, 192.9, 0.1, 19.3, TenPercentReason, "core loop −51.2", "none"),
        new("term PerKey kafka b500 h1", "msg", 210.8, 191.6, 0.1, 19.2, TenPercentReason, "raw produce −631.8, Kafka routes −252.7, core loop −470.7", "none"),
        new("term PerKey dlq b500 h1", "dlq-msg", 1022.8, 929.8, 1.3, 93.0, TenPercentReason, "raw produce −1,171.2, Kafka routes −1,073.7, core loop −295.2, dead-letter headers −365.5", "none"),
        new("term Strict null b500 h1", "msg", 212.2, 192.9, 0.0, 19.3, TenPercentReason, "none", "none"),
        new("term Strict kafka b500 h1", "msg", 273.2, 248.4, 0.0, 24.8, TenPercentReason, "raw produce −632.0, Kafka routes −224.0, core loop −1,346.9", "none"),
        new("term Strict dlq b500 h1", "dlq-msg", 1091.6, 992.4, 0.1, 99.2, TenPercentReason, "raw produce −1,176.0, Kafka routes −1,115.0, core loop −1,160.1, dead-letter headers −360.0", "none"),
        new("session None batch 500 h1", "row", 352.3, 320.3, 1.7, 32.0, TenPercentReason, "PostgreSQL session −277.9", "none"),
        new("session PerKey batch 500 h1", "row", 351.2, 319.3, 0.5, 31.9, TenPercentReason, "PostgreSQL session −282.4", "none"),
        new("session Strict batch 500 h1", "row", 351.9, 319.9, 1.3, 32.0, TenPercentReason, "PostgreSQL session −280.3", "none"),
        new("None read-empty", "call", 1170.7, 1064.3, 0.0, 106.4, TenPercentReason, "PostgreSQL session −2,536.6", "none"),
        new("None check", "call", 1461.1, 1328.3, 0.0, 132.8, TenPercentReason, "PostgreSQL session −1,504.0", "none"),
        new("PerKey read-empty", "call", 1170.7, 1064.3, 0.0, 106.4, TenPercentReason, "PostgreSQL session −3,936.0", "none"),
        new("PerKey check", "call", 1461.1, 1328.3, 0.0, 132.8, TenPercentReason, "PostgreSQL session −1,504.0", "none"),
        new("Strict read-empty", "call", 1170.7, 1064.3, 0.0, 106.4, TenPercentReason, "PostgreSQL session −2,632.0", "none"),
        new("Strict check", "call", 1461.1, 1328.3, 0.0, 132.8, TenPercentReason, "PostgreSQL session −1,504.0", "none"),

        // Ceilings from guard runs with idle --rounds 1. Margin max(3 x spread, 200 B/s). 3 x spread > 1,000 B/s: STOP.
        new("idle None null interval 1s", "s", 335.2, 135.2, 62.6, 200.0, IdleNoise, "core loop −3,058.2", "none"),
        new("idle None kafka interval 1s", "s", 292.0, 92.0, 31.1, 200.0, IdleNoise, "core loop −2,894.5", "none"),
        new("idle PerKey null interval 1s", "s", 296.9, 96.9, 29.5, 200.0, IdleNoise, "core loop −4,723.7", "none"),
        new("idle PerKey kafka interval 1s", "s", 295.7, 95.7, 30.3, 200.0, IdleNoise, "core loop −4,813.5", "none"),
        new("idle Strict null interval 1s", "s", 284.5, 84.5, 39.8, 200.0, IdleNoise, "core loop −4,253.8", "none"),
        new("idle Strict kafka interval 1s", "s", 295.0, 95.0, 32.7, 200.0, IdleNoise, "core loop −3,712.6", "none"),

        // Http rows: 5 guard runs on c9fe4f1. Exceeds: the spike's UseProxy default vs off delta (http-floor), for cells that send through SocketsHttpHandler over HTTP/1.1.
        new("http-stub None w500 h1", "msg", 466.4, 424.0, 0.0, 42.4, TenPercentReason, "none", "none"),
        new("term None http b500 h1", "msg", 1655.3, 1504.8, 0.0, 150.5, TenPercentReason, "macOS proxy lookup −539.4 (UseProxy env-only)", "none"),
        new("term PerKey http b500 h1", "msg", 1655.4, 1504.9, 0.0, 150.5, TenPercentReason, "macOS proxy lookup −539.4 (UseProxy env-only)", "none"),
        new("term Strict http b500 h1", "msg", 1655.4, 1504.9, 0.0, 150.5, TenPercentReason, "macOS proxy lookup −539.4 (UseProxy env-only)", "none"),
        new("term None http-tpl b500 h1", "msg", 2076.8, 1888.0, 0.0, 188.8, TenPercentReason, "macOS proxy lookup −539.4 (UseProxy env-only)", "none"),
        new("term None http-sat b500 h1", "msg", 1655.3, 1504.8, 0.0, 150.5, TenPercentReason, "macOS proxy lookup −539.4 (UseProxy env-only)", "none"),
        new("term None http-dlq b500 h1", "dlq-msg", 2200.9, 2000.8, 0.0, 200.1, TenPercentReason, "macOS proxy lookup −539.4 (UseProxy env-only)", "none"),
        new("term None http2 b500 h1", "msg", 4464.0, 4058.2, 11.1, 405.8, TenPercentReason, "none", "none"),
        new("idle None http interval 1s", "s", 299.5, 99.5, 21.3, 200.0, IdleNoise, "none", "none"),
    ];
}
