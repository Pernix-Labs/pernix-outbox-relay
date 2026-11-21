using System.Reflection;
using Pernix.OutboxRelay.Client.EntityFrameworkCore;
using Xunit;

namespace Pernix.OutboxRelay.Client.Unit.Tests.EntityFrameworkCore;

// Public surface of the EF assembly:
// 4 static classes, 1 interface, 8 members.
public sealed class ApiShapeTests
{
    private const BindingFlags Declared = BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly;
    private const string Ns = "Pernix.OutboxRelay.Client.EntityFrameworkCore.";

    private static readonly Assembly EntityFrameworkCore = typeof(OutboxDbContextExtensions).Assembly;

    private static readonly string[] ExpectedSurface =
    [
        Ns + "IOutboxQueue<TContext>: interface where TContext : DbContext",
        Ns + "IOutboxQueue<TContext>: void Enqueue(OutboxMessage message)",

        Ns + "OutboxDbContextExtensions: static class",
        Ns + "OutboxDbContextExtensions: static void EnqueueOutbox(DbContext context, OutboxMessage message)",

        Ns + "OutboxDbContextOptionsBuilderExtensions: static class",
        Ns + "OutboxDbContextOptionsBuilderExtensions: static DbContextOptionsBuilder UseOutbox(DbContextOptionsBuilder builder, IOutboxPublisher publisher)",
        Ns + "OutboxDbContextOptionsBuilderExtensions: static DbContextOptionsBuilder<TContext> UseOutbox<TContext>(DbContextOptionsBuilder<TContext> builder, IOutboxPublisher publisher) where TContext : DbContext",

        Ns + "OutboxEntityFrameworkServiceCollectionExtensions: static class",
        Ns + "OutboxEntityFrameworkServiceCollectionExtensions: static IServiceCollection AddOutboxQueue<TContext>(IServiceCollection services) where TContext : DbContext",
        Ns + "OutboxEntityFrameworkServiceCollectionExtensions: static IServiceCollection AddKeyedOutboxQueue<TContext>(IServiceCollection services, Object key, String route) where TContext : DbContext",

        Ns + "OutboxPublisherDbContextExtensions: static class",
        Ns + "OutboxPublisherDbContextExtensions: static Task PublishAsync(IOutboxPublisher publisher, DbContext context, IReadOnlyList<OutboxMessage> messages, CancellationToken ct = default)",
        Ns + "OutboxPublisherDbContextExtensions: static Task PublishAsync(IOutboxPublisher publisher, DbContext context, OutboxMessage message, CancellationToken ct = default)",
    ];

    [Fact]
    public void Public_surface_equals_the_design_code_blocks()
    {
        var lines = new List<string>();
        foreach (var type in EntityFrameworkCore.GetExportedTypes())
        {
            var owner = type.Namespace + "." + TypeName(type);
            lines.Add($"{owner}: {TypeHeader(type)}");
            foreach (var member in type.GetMembers(Declared))
            {
                lines.Add($"{owner}: {Member(member)}");
            }
        }

        Assert.Equal(ExpectedSurface.Order(StringComparer.Ordinal), lines.Order(StringComparer.Ordinal));
    }

    private static string TypeHeader(Type type)
    {
        if (type.IsInterface)
        {
            return "interface" + Constraints(type.GetGenericArguments());
        }

        return type.IsAbstract && type.IsSealed ? "static class" : type.IsValueType ? "struct" : "class";
    }

    private static string Member(MemberInfo member)
    {
        if (member is not MethodInfo method)
        {
            return $"{member.MemberType} {member.Name}";
        }

        var arguments = method.GetGenericArguments();
        var generic = arguments.Length == 0 ? string.Empty : "<" + string.Join(", ", arguments.Select(a => a.Name)) + ">";
        var parameters = string.Join(", ", method.GetParameters().Select(Parameter));
        var modifier = method.IsStatic ? "static " : string.Empty;
        return $"{modifier}{TypeName(method.ReturnType)} {method.Name}{generic}({parameters}){Constraints(arguments)}";
    }

    private static string Parameter(ParameterInfo parameter)
    {
        var text = $"{TypeName(parameter.ParameterType)} {parameter.Name}";
        return parameter.HasDefaultValue ? text + " = default" : text;
    }

    private static string Constraints(Type[] arguments) =>
        string.Concat(arguments.Select(a =>
            $" where {a.Name} : {string.Join(", ", a.GetGenericParameterConstraints().Select(TypeName))}"));

    private static string TypeName(Type type)
    {
        if (type == typeof(void))
        {
            return "void";
        }

        if (type.IsGenericParameter || !type.IsGenericType)
        {
            return type.Name;
        }

        var simple = type.Name[..type.Name.IndexOf('`')];
        return simple + "<" + string.Join(", ", type.GetGenericArguments().Select(TypeName)) + ">";
    }
}
