using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Pernix.OutboxRelay.Client.EntityFrameworkCore.Integration.Tests;

public sealed record CapturedLogEntry(
    string Category,
    LogLevel Level,
    EventId EventId,
    string Message,
    IReadOnlyList<KeyValuePair<string, object?>> State)
{
    public object? Value(string name)
    {
        foreach (var pair in State)
        {
            if (pair.Key == name)
            {
                return pair.Value;
            }
        }

        return null;
    }
}

// Shared by parallel tests: filter by outbox table.
public sealed class CapturingLoggerProvider : ILoggerProvider
{
    private readonly ConcurrentQueue<CapturedLogEntry> entries = new();

    public IReadOnlyList<CapturedLogEntry> Entries => entries.ToArray();

    public IReadOnlyList<CapturedLogEntry> ForOutboxTable(OutboxTable table)
    {
        var text = table.ToString();
        var matches = new List<CapturedLogEntry>();
        foreach (var entry in entries)
        {
            if (entry.Value("OutboxTable") is string value && value == text)
            {
                matches.Add(entry);
            }
        }

        return matches;
    }

    public ILogger CreateLogger(string categoryName) => new CapturingLogger(this, categoryName);

    public void Dispose()
    {
    }

    private sealed class CapturingLogger(CapturingLoggerProvider provider, string category) : ILogger
    {
        private readonly LogLevel minimum = category.StartsWith("Microsoft.", StringComparison.Ordinal) ? LogLevel.Warning : LogLevel.Trace;

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= minimum;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (!IsEnabled(logLevel))
            {
                return;
            }

            IReadOnlyList<KeyValuePair<string, object?>> values = state is IReadOnlyList<KeyValuePair<string, object?>> list ? [.. list] : [];
            provider.entries.Enqueue(new CapturedLogEntry(category, logLevel, eventId, formatter(state, exception), values));
        }
    }
}
