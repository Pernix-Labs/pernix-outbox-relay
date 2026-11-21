using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Xunit;

namespace Pernix.OutboxRelay.Client.Unit.Tests.Core;

// Public surface of the Core assembly.
public sealed class CoreApiShapeTests
{
    private const BindingFlags Declared = BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly;

    private static readonly Assembly Core = typeof(OutboxMessage).Assembly;

    private static readonly string[] ExpectedSurface =
    [
        "Pernix.OutboxRelay.Client.IOutboxPublisher: interface",
        "Pernix.OutboxRelay.Client.IOutboxPublisher: OutboxTable Table { get; }",
        "Pernix.OutboxRelay.Client.IOutboxPublisher: Task PublishAsync(DbTransaction transaction, OutboxMessage message, CancellationToken ct = default)",
        "Pernix.OutboxRelay.Client.IOutboxPublisher: Task PublishAsync(DbTransaction transaction, IReadOnlyList<OutboxMessage> messages, CancellationToken ct = default)",
        "Pernix.OutboxRelay.Client.IOutboxPublisher: Task PublishEnlistedAsync(DbConnection connection, OutboxMessage message, CancellationToken ct = default)",
        "Pernix.OutboxRelay.Client.IOutboxPublisher: Task PublishEnlistedAsync(DbConnection connection, IReadOnlyList<OutboxMessage> messages, CancellationToken ct = default)",

        "Pernix.OutboxRelay.Client.OutboxColumns: static class",
        "Pernix.OutboxRelay.Client.OutboxColumns: const string CreatedAt = \"created_at\"",
        "Pernix.OutboxRelay.Client.OutboxColumns: const string Headers = \"headers\"",
        "Pernix.OutboxRelay.Client.OutboxColumns: const string Id = \"id\"",
        "Pernix.OutboxRelay.Client.OutboxColumns: const string LastError = \"last_error\"",
        "Pernix.OutboxRelay.Client.OutboxColumns: const string MessageKey = \"message_key\"",
        "Pernix.OutboxRelay.Client.OutboxColumns: const string NextAttemptAt = \"next_attempt_at\"",
        "Pernix.OutboxRelay.Client.OutboxColumns: const string Payload = \"payload\"",
        "Pernix.OutboxRelay.Client.OutboxColumns: const string RetryCount = \"retry_count\"",
        "Pernix.OutboxRelay.Client.OutboxColumns: const string Route = \"route\"",
        "Pernix.OutboxRelay.Client.OutboxColumns: const string TxId = \"tx_id\"",
        "Pernix.OutboxRelay.Client.OutboxColumns: static IReadOnlyList<string> All { get; }",
        "Pernix.OutboxRelay.Client.OutboxColumns: static IReadOnlyList<string> ProducerWritten { get; }",

        "Pernix.OutboxRelay.Client.OutboxHeaders: sealed class : IEnumerable, IEnumerable<KeyValuePair<string, string>>, IReadOnlyCollection<KeyValuePair<string, string>>, IReadOnlyList<KeyValuePair<string, string>>",
        "Pernix.OutboxRelay.Client.OutboxHeaders: IEnumerator<KeyValuePair<string, string>> GetEnumerator()",
        "Pernix.OutboxRelay.Client.OutboxHeaders: KeyValuePair<string, string> this[int index] { get; }",
        "Pernix.OutboxRelay.Client.OutboxHeaders: int Count { get; }",
        "Pernix.OutboxRelay.Client.OutboxHeaders: static OutboxHeaders Empty { get; }",
        "Pernix.OutboxRelay.Client.OutboxHeaders: static OutboxHeaders.Builder CreateBuilder(int capacity = 4)",
        "Pernix.OutboxRelay.Client.OutboxHeaders: string Json { get; }",

        "Pernix.OutboxRelay.Client.OutboxHeaders.Builder: sealed class",
        "Pernix.OutboxRelay.Client.OutboxHeaders.Builder: OutboxHeaders Build()",
        "Pernix.OutboxRelay.Client.OutboxHeaders.Builder: OutboxHeaders.Builder Add(string name, string value)",

        "Pernix.OutboxRelay.Client.OutboxMessage: readonly struct",
        "Pernix.OutboxRelay.Client.OutboxMessage: OutboxHeaders Headers { get; }",
        "Pernix.OutboxRelay.Client.OutboxMessage: ReadOnlyMemory<byte> Payload { get; }",
        "Pernix.OutboxRelay.Client.OutboxMessage: ReadOnlyMemory<byte>? Key { get; }",
        "Pernix.OutboxRelay.Client.OutboxMessage: OutboxMessage WithRoute(string route)",
        "Pernix.OutboxRelay.Client.OutboxMessage: string? Route { get; }",
        "Pernix.OutboxRelay.Client.OutboxMessage: static OutboxMessage FromBytes(ReadOnlyMemory<byte>? key, ReadOnlyMemory<byte> payload, OutboxHeaders? headers = null)",
        "Pernix.OutboxRelay.Client.OutboxMessage: static OutboxMessage FromBytes(byte[]? key, ReadOnlyMemory<byte> payload, OutboxHeaders? headers = null)",
        "Pernix.OutboxRelay.Client.OutboxMessage: static OutboxMessage Create(string? key, ReadOnlyMemory<byte> payload, OutboxHeaders? headers = null)",
        "Pernix.OutboxRelay.Client.OutboxMessage: static OutboxMessage Create(string? key, string payload, OutboxHeaders? headers = null)",
        "Pernix.OutboxRelay.Client.OutboxMessage: static OutboxMessage Json<T>(string? key, T value, JsonTypeInfo<T> typeInfo, OutboxHeaders? headers = null)",

        "Pernix.OutboxRelay.Client.OutboxTable: readonly struct",
        "Pernix.OutboxRelay.Client.OutboxTable: override string ToString()",
        "Pernix.OutboxRelay.Client.OutboxTable: static OutboxTable Parse(string text)",
        "Pernix.OutboxRelay.Client.OutboxTable: string Name { get; }",
        "Pernix.OutboxRelay.Client.OutboxTable: string? Schema { get; }",
    ];

    private static readonly Dictionary<Type, string> Keywords = new()
    {
        [typeof(void)] = "void",
        [typeof(bool)] = "bool",
        [typeof(byte)] = "byte",
        [typeof(int)] = "int",
        [typeof(long)] = "long",
        [typeof(string)] = "string",
        [typeof(object)] = "object",
    };

    private readonly NullabilityInfoContext nullability = new();

    [Fact]
    public void Public_surface_equals_the_design_code_block()
    {
        Assert.Equal(ExpectedSurface.Order(StringComparer.Ordinal), PublicSurface().Order(StringComparer.Ordinal));
    }

    [Fact]
    public void No_public_member_takes_json_serializer_options()
    {
        var offenders = new List<string>();
        foreach (var type in Core.GetExportedTypes())
        {
            foreach (var member in type.GetMembers(Declared))
            {
                var parameters = member switch
                {
                    MethodBase method => method.GetParameters(),
                    PropertyInfo property => property.GetIndexParameters(),
                    _ => [],
                };

                var types = parameters.Select(p => p.ParameterType).ToList();
                if (member is PropertyInfo p)
                {
                    types.Add(p.PropertyType);
                }

                if (types.Any(MentionsJsonSerializerOptions))
                {
                    offenders.Add($"{type.FullName}.{member.Name}");
                }
            }
        }

        Assert.Empty(offenders);
    }

    [Fact]
    public void Outbox_message_is_a_plain_readonly_struct_not_a_record()
    {
        var type = typeof(OutboxMessage);
        const BindingFlags all = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static;

        Assert.True(type.IsValueType);
        Assert.NotNull(type.GetCustomAttribute<IsReadOnlyAttribute>());
        Assert.Null(type.GetMethod("PrintMembers", all));
        Assert.Null(type.GetMethod("op_Equality", all));
        Assert.DoesNotContain(typeof(IEquatable<OutboxMessage>), type.GetInterfaces());
    }

    private static bool MentionsJsonSerializerOptions(Type type)
    {
        if (type.HasElementType)
        {
            return MentionsJsonSerializerOptions(type.GetElementType()!);
        }

        return type == typeof(JsonSerializerOptions)
            || (type.IsGenericType && type.GetGenericArguments().Any(MentionsJsonSerializerOptions));
    }

    private List<string> PublicSurface()
    {
        var lines = new List<string>();
        foreach (var type in Core.GetExportedTypes())
        {
            var owner = type.Namespace + "." + TypeName(type, null);
            lines.Add($"{owner}: {TypeHeader(type)}");

            foreach (var member in type.GetMembers(Declared))
            {
                var line = Member(member);
                if (line is not null)
                {
                    lines.Add($"{owner}: {line}");
                }
            }
        }

        return lines;
    }

    private static string TypeHeader(Type type)
    {
        string kind;
        if (type.IsInterface)
        {
            kind = "interface";
        }
        else if (type.IsValueType)
        {
            kind = type.GetCustomAttribute<IsReadOnlyAttribute>() is null ? "struct" : "readonly struct";
        }
        else if (type.IsAbstract && type.IsSealed)
        {
            kind = "static class";
        }
        else
        {
            kind = type.IsSealed ? "sealed class" : "class";
        }

        var interfaces = type.GetInterfaces().Select(i => TypeName(i, null)).Order(StringComparer.Ordinal).ToArray();
        return interfaces.Length == 0 ? kind : $"{kind} : {string.Join(", ", interfaces)}";
    }

    private string? Member(MemberInfo member)
    {
        switch (member)
        {
            case ConstructorInfo ctor:
                return $"ctor({Parameters(ctor)})";

            case MethodInfo method when method.IsSpecialName && !method.Name.StartsWith("op_", StringComparison.Ordinal):
                return null;

            case MethodInfo method:
                var generic = method.IsGenericMethodDefinition
                    ? "<" + string.Join(", ", method.GetGenericArguments().Select(a => a.Name)) + ">"
                    : string.Empty;
                var modifier = method.IsStatic ? "static " : method.GetBaseDefinition().DeclaringType != method.DeclaringType ? "override " : string.Empty;
                return $"{modifier}{TypeName(method.ReturnType, nullability.Create(method.ReturnParameter))} {method.Name}{generic}({Parameters(method)})";

            case PropertyInfo property:
                var accessors = "get;" + (property.SetMethod?.IsPublic == true ? " set;" : string.Empty);
                var isStatic = property.GetMethod!.IsStatic ? "static " : string.Empty;
                var index = property.GetIndexParameters();
                var name = index.Length == 0 ? property.Name : $"this[{string.Join(", ", index.Select(Parameter))}]";
                return $"{isStatic}{TypeName(property.PropertyType, nullability.Create(property))} {name} {{ {accessors} }}";

            case FieldInfo field when field.IsLiteral:
                return $"const {TypeName(field.FieldType, null)} {field.Name} = \"{field.GetRawConstantValue()}\"";

            case FieldInfo field:
                return $"{(field.IsStatic ? "static " : string.Empty)}{(field.IsInitOnly ? "readonly " : string.Empty)}{TypeName(field.FieldType, nullability.Create(field))} {field.Name}";

            case Type:
                return null;

            default:
                return $"{member.MemberType} {member.Name}";
        }
    }

    private string Parameters(MethodBase method) => string.Join(", ", method.GetParameters().Select(Parameter));

    private string Parameter(ParameterInfo parameter)
    {
        var text = $"{TypeName(parameter.ParameterType, nullability.Create(parameter))} {parameter.Name}";
        if (!parameter.HasDefaultValue)
        {
            return text;
        }

        var value = parameter.DefaultValue switch
        {
            null when parameter.ParameterType.IsValueType && Nullable.GetUnderlyingType(parameter.ParameterType) is null => "default",
            null => "null",
            var v => v.ToString(),
        };
        return $"{text} = {value}";
    }

    private static string TypeName(Type type, NullabilityInfo? info)
    {
        if (Nullable.GetUnderlyingType(type) is { } underlying)
        {
            return TypeName(underlying, null) + "?";
        }

        string name;
        if (Keywords.TryGetValue(type, out var keyword))
        {
            name = keyword;
        }
        else if (type.IsGenericParameter)
        {
            name = type.Name;
        }
        else if (type.IsArray)
        {
            name = TypeName(type.GetElementType()!, info?.ElementType) + "[]";
        }
        else
        {
            var simple = type.IsGenericType ? type.Name[..type.Name.IndexOf('`')] : type.Name;
            if (type.IsNested)
            {
                simple = TypeName(type.DeclaringType!, null) + "." + simple;
            }

            if (type.IsGenericType)
            {
                var arguments = type.GetGenericArguments();
                var names = new string[arguments.Length];
                for (var i = 0; i < arguments.Length; i++)
                {
                    var argumentInfo = info is not null && info.GenericTypeArguments.Length == arguments.Length ? info.GenericTypeArguments[i] : null;
                    names[i] = TypeName(arguments[i], argumentInfo);
                }

                simple += "<" + string.Join(", ", names) + ">";
            }

            name = simple;
        }

        var nullable = info is not null && !type.IsValueType && !type.IsGenericParameter && info.ReadState == NullabilityState.Nullable;
        return nullable ? name + "?" : name;
    }
}
