using System.Text;
using Pernix.OutboxRelay.Core.Abstractions;

namespace Pernix.OutboxRelay.Unit.Tests;

internal static class TestHeaders
{
    public static MessageHeaders Of(IEnumerable<KeyValuePair<string, string>> headers) =>
        new([.. headers.Select(h => new MessageHeader(h.Key, Encoding.UTF8.GetBytes(h.Value)))]);

    public static Dictionary<string, string> ToStrings(this MessageHeaders headers) =>
        headers.ToDictionary(h => h.Name, h => Encoding.UTF8.GetString(h.Value), StringComparer.Ordinal);
}
