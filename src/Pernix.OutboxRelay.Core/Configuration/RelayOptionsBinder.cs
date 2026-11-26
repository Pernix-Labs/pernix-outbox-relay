using System.Globalization;
using System.Reflection;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Configuration;

namespace Pernix.OutboxRelay.Core.Configuration;

// Never throws: failures go to RelayOptions.BindingFailures so Validate reports them with every other check.
public static partial class RelayOptionsBinder
{
    public static RelayOptions Bind(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var options = new RelayOptions();
        var failures = new List<string>();

        options.Destinations.AddRange(configuration.GetSection(nameof(RelayOptions.Destinations)).GetChildren()
            .Select(section => new DestinationOptions
            {
                Name = section[nameof(DestinationOptions.Name)],
                Transport = section[nameof(DestinationOptions.Transport)],
                Settings = section,
            }));
        // Elements are bound one by one: binding a whole list silently drops elements that fail to convert.
        options.Outboxes.AddRange(configuration.GetSection(nameof(RelayOptions.Outboxes)).GetChildren()
            .Select((section, i) => BindSection<OutboxOptions>(section, ElementPath(nameof(RelayOptions.Outboxes), section, i), failures)));

        options.BindingFailures.AddRange(failures.Distinct(StringComparer.Ordinal));
        return options;
    }

    private static string ElementPath(string section, IConfigurationSection element, int index) =>
        RelayConfigurationValidator.PathOf(section, element[nameof(OutboxOptions.Name)], index);

    public static T BindSection<T>(IConfigurationSection section, string path, ICollection<string> failures)
        where T : new()
    {
        ArgumentNullException.ThrowIfNull(section);
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(failures);

        var settings = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        foreach (var (key, value) in section.AsEnumerable(makePathsRelative: true))
        {
            if (value is not null && IsAccepted(typeof(T), key, value, path, failures))
            {
                settings[key] = value;
            }
        }

        // The binder stops at the first value it cannot convert, so drop that key and bind again.
        while (true)
        {
            var element = new T();
            try
            {
                new ConfigurationBuilder().AddInMemoryCollection(settings).Build().Bind(element);
                return element;
            }
            catch (InvalidOperationException ex)
            {
                // The binder message embeds the raw value, which may be secret. Keep only the key and target type.
                var match = KeyAndType().Match(ex.Message);
                if (!match.Success || !settings.Remove(match.Groups["key"].Value))
                {
                    failures.Add($"{path}: a value cannot be converted to its setting type");
                    return new T();
                }

                failures.Add($"{path}.{match.Groups["key"].Value.Replace(':', '.')}: cannot convert value to {match.Groups["type"].Value}");
            }
        }
    }

    private static bool IsAccepted(Type type, string key, string value, string path, ICollection<string> failures)
    {
        var segments = key.Split(ConfigurationPath.KeyDelimiter);
        var isElement = false;
        for (var i = 0; i < segments.Length; i++)
        {
            // A property without a public setter (DestinationOptions.Settings) is not a setting.
            var property = type.GetProperty(segments[i], BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
            if (property?.SetMethod is not { IsPublic: true })
            {
                failures.Add($"{path}: unknown setting '{segments[i]}'");
                return false;
            }

            path = $"{path}.{property.Name}";
            type = property.PropertyType;
            if (IsMap(type))
            {
                return true;
            }

            if (ElementOf(type) is { } element && i == segments.Length - 2 && int.TryParse(segments[i + 1], NumberStyles.None, CultureInfo.InvariantCulture, out _))
            {
                path = $"{path}[{segments[i + 1]}]";
                type = element;
                isElement = true;
                break;
            }

            if (i < segments.Length - 1 && !IsSection(type))
            {
                failures.Add($"{path}: unknown setting '{segments[i + 1]}'");
                return false;
            }
        }

        var target = Nullable.GetUnderlyingType(type) ?? type;
        if (target == typeof(TimeSpan) && !IsDuration(value))
        {
            failures.Add($"{path}: must be a duration in hh:mm:ss form");
            return false;
        }

        // The binder drops a list element it cannot convert instead of throwing, so check it here.
        if (isElement && target == typeof(int) && !int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out _))
        {
            failures.Add($"{path}: must be an integer");
            return false;
        }

        if (target.IsEnum && !(target != type && string.IsNullOrWhiteSpace(value)) && !Enum.GetNames(target).Contains(value.Trim(), StringComparer.OrdinalIgnoreCase))
        {
            failures.Add($"{path}: must be one of {string.Join(", ", Enum.GetNames(target))}");
            return false;
        }

        return true;
    }

    // A bare number would bind as days.
    private static bool IsDuration(string value) =>
        Duration().IsMatch(value) && TimeSpan.TryParse(value, CultureInfo.InvariantCulture, out _);

    private static bool IsSection(Type type) => type.IsClass && type != typeof(string) && ElementOf(type) is null;

    private static Type? ElementOf(Type type)
    {
        var element = type.IsArray
            ? type.GetElementType()
            : type.IsGenericType && (type.GetGenericTypeDefinition() == typeof(List<>) || type.GetGenericTypeDefinition() == typeof(IReadOnlyList<>))
                ? type.GetGenericArguments()[0]
                : null;
        return element is not null && IsScalar(element) ? element : null;
    }

    private static bool IsScalar(Type type)
    {
        var target = Nullable.GetUnderlyingType(type) ?? type;
        return target == typeof(string) || target == typeof(int) || target == typeof(TimeSpan) || target.IsEnum;
    }

    private static bool IsMap(Type type) => type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Dictionary<,>);

    [GeneratedRegex(@"^\s*-?(\d+\.)?\d+:\d+:\d+(\.\d+)?\s*$")]
    private static partial Regex Duration();

    [GeneratedRegex(@"at '(?<key>[^']+)' to type '(?<type>[^']+)'\.?$")]
    private static partial Regex KeyAndType();
}
