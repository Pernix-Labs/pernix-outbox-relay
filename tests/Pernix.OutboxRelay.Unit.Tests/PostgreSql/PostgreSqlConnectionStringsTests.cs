using Pernix.OutboxRelay.PostgreSql;
using Npgsql;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.PostgreSql;

public sealed class PostgreSqlConnectionStringsTests
{
    private const string Overridden =
        "Host=db;Database=orders;Pooling=true;Timeout=60;Tcp Keepalive=false;Multiplexing=true;Command Timeout=30;Cancellation Timeout=2000;" +
        "Tcp Keepalive Time=60;Tcp Keepalive Interval=30;Target Session Attributes=any;Load Balance Hosts=true";

    [Fact]
    public void ForSession_forces_every_session_setting_over_configured_values()
    {
        var result = new NpgsqlConnectionStringBuilder(PostgreSqlConnectionStrings.ForSession(Overridden));

        Assert.Null(result.TargetSessionAttributes);
        AssertForced(result);
        Assert.Equal("db", result.Host);
    }

    [Fact]
    public void ForSession_requires_a_read_write_host_from_a_host_list()
    {
        var result = new NpgsqlConnectionStringBuilder(PostgreSqlConnectionStrings.ForSession(Overridden.Replace("Host=db", "Host=db-a,db-b", StringComparison.Ordinal)));

        Assert.Equal("read-write", result.TargetSessionAttributes);
        AssertForced(result);
    }

    [Fact]
    public void ForValidation_prefers_the_primary_from_a_host_list()
    {
        var result = new NpgsqlConnectionStringBuilder(PostgreSqlConnectionStrings.ForValidation(Overridden.Replace("Host=db", "Host=db-a,db-b", StringComparison.Ordinal)));

        Assert.Equal("prefer-primary", result.TargetSessionAttributes);
        AssertForced(result);
    }

    [Fact]
    public void ForSession_keeps_configured_application_name()
    {
        var result = new NpgsqlConnectionStringBuilder(PostgreSqlConnectionStrings.ForSession("Host=db;Application Name=orders-relay"));

        Assert.Equal("orders-relay", result.ApplicationName);
    }

    [Fact]
    public void ForSession_defaults_application_name_when_unset()
    {
        var result = new NpgsqlConnectionStringBuilder(PostgreSqlConnectionStrings.ForSession("Host=db"));

        Assert.Equal("outbox-relay", result.ApplicationName);
    }

    [Fact]
    public void ForStats_pools_one_connection_per_outbox_and_keeps_the_session_values()
    {
        var result = new NpgsqlConnectionStringBuilder(PostgreSqlConnectionStrings.ForStats(Overridden, "orders"));

        Assert.True(result.Pooling);
        Assert.Equal(0, result.MinPoolSize);
        Assert.Equal(1, result.MaxPoolSize);
        Assert.Equal(60, result.ConnectionIdleLifetime);
        Assert.Equal("outbox-relay-stats:orders", result.ApplicationName);
        Assert.Null(result.TargetSessionAttributes);
        AssertSessionValues(result);
    }

    [Fact]
    public void ForStats_requires_a_read_write_host_from_a_host_list()
    {
        var result = new NpgsqlConnectionStringBuilder(
            PostgreSqlConnectionStrings.ForStats(Overridden.Replace("Host=db", "Host=db-a,db-b", StringComparison.Ordinal), "orders"));

        Assert.Equal("read-write", result.TargetSessionAttributes);
        AssertSessionValues(result);
    }

    [Fact]
    public void ForStats_overrides_configured_application_name()
    {
        var result = new NpgsqlConnectionStringBuilder(PostgreSqlConnectionStrings.ForStats("Host=db;Application Name=orders-relay", "orders"));

        Assert.Equal("outbox-relay-stats:orders", result.ApplicationName);
    }

    [Fact]
    public void ForStats_gives_each_outbox_its_own_pool_key()
    {
        Assert.NotEqual(
            PostgreSqlConnectionStrings.ForStats(Overridden, "orders"),
            PostgreSqlConnectionStrings.ForStats(Overridden, "payments"));
    }

    [Fact]
    public void ForStats_adds_server_keepalives_to_configured_options()
    {
        var result = new NpgsqlConnectionStringBuilder(
            PostgreSqlConnectionStrings.ForStats("Host=db;Options=-c statement_timeout=0 -c tcp_keepalives_idle=60", "orders"));

        Assert.Equal(
            "-c statement_timeout=0 -c tcp_keepalives_idle=60 -c tcp_keepalives_idle=5 -c tcp_keepalives_interval=2 -c tcp_keepalives_count=3",
            result.Options);
    }

    [Fact]
    public void ForStats_sets_server_keepalives_without_configured_options()
    {
        var result = new NpgsqlConnectionStringBuilder(PostgreSqlConnectionStrings.ForStats("Host=db", "orders"));

        Assert.Equal("-c tcp_keepalives_idle=5 -c tcp_keepalives_interval=2 -c tcp_keepalives_count=3", result.Options);
    }

    [Fact]
    public void Session_and_validation_strings_carry_no_startup_options()
    {
        Assert.Null(new NpgsqlConnectionStringBuilder(PostgreSqlConnectionStrings.ForSession("Host=db")).Options);
        Assert.Null(new NpgsqlConnectionStringBuilder(PostgreSqlConnectionStrings.ForValidation("Host=db")).Options);
    }

    [Theory]
    [InlineData("session", "Host=db;Database=orders",
        "Host=db;Database=orders;Pooling=False;Multiplexing=False;Timeout=5;Command Timeout=5;Cancellation Timeout=-1;TCP Keepalive=True;TCP Keepalive Time=5;TCP Keepalive Interval=2;Load Balance Hosts=False;Application Name=outbox-relay")]
    [InlineData("session", "Host=db-a,db-b;Database=orders;Pooling=true;Timeout=60;Options=-c x=1",
        "Host=db-a,db-b;Database=orders;Pooling=False;Timeout=5;Options=\"-c x=1\";Multiplexing=False;Command Timeout=5;Cancellation Timeout=-1;TCP Keepalive=True;TCP Keepalive Time=5;TCP Keepalive Interval=2;Load Balance Hosts=False;Target Session Attributes=read-write;Application Name=outbox-relay")]
    [InlineData("validation", "Host=db-a,db-b;Database=orders;Application Name=orders-relay",
        "Host=db-a,db-b;Database=orders;Application Name=orders-relay;Pooling=False;Multiplexing=False;Timeout=5;Command Timeout=5;Cancellation Timeout=-1;TCP Keepalive=True;TCP Keepalive Time=5;TCP Keepalive Interval=2;Load Balance Hosts=False;Target Session Attributes=prefer-primary")]
    [InlineData("stats", "Host=db;Database=orders",
        "Host=db;Database=orders;Pooling=True;Multiplexing=False;Timeout=5;Command Timeout=5;Cancellation Timeout=-1;TCP Keepalive=True;TCP Keepalive Time=5;TCP Keepalive Interval=2;Load Balance Hosts=False;Minimum Pool Size=0;Maximum Pool Size=1;Connection Idle Lifetime=60;Options=\"-c tcp_keepalives_idle=5 -c tcp_keepalives_interval=2 -c tcp_keepalives_count=3\";Application Name=outbox-relay-stats:orders")]
    [InlineData("stats", "Host=db-a,db-b;Database=orders;Options=-c statement_timeout=0",
        "Host=db-a,db-b;Database=orders;Options=\"-c statement_timeout=0 -c tcp_keepalives_idle=5 -c tcp_keepalives_interval=2 -c tcp_keepalives_count=3\";Pooling=True;Multiplexing=False;Timeout=5;Command Timeout=5;Cancellation Timeout=-1;TCP Keepalive=True;TCP Keepalive Time=5;TCP Keepalive Interval=2;Load Balance Hosts=False;Target Session Attributes=read-write;Minimum Pool Size=0;Maximum Pool Size=1;Connection Idle Lifetime=60;Application Name=outbox-relay-stats:orders")]
    public void Existing_strings_are_unchanged(string kind, string configured, string expected)
    {
        var actual = kind switch
        {
            "session" => PostgreSqlConnectionStrings.ForSession(configured),
            "validation" => PostgreSqlConnectionStrings.ForValidation(configured),
            _ => PostgreSqlConnectionStrings.ForStats(configured, "orders"),
        };

        Assert.Equal(expected, actual);
    }

    private static void AssertForced(NpgsqlConnectionStringBuilder result)
    {
        Assert.False(result.Pooling);
        AssertSessionValues(result);
    }

    private static void AssertSessionValues(NpgsqlConnectionStringBuilder result)
    {
        Assert.False(result.LoadBalanceHosts);
        Assert.False(result.Multiplexing);
        Assert.Equal(5, result.Timeout);
        Assert.Equal(5, result.CommandTimeout);
        Assert.Equal(-1, result.CancellationTimeout);
        Assert.True(result.TcpKeepAlive);
        Assert.Equal(5, result.TcpKeepAliveTime);
        Assert.Equal(2, result.TcpKeepAliveInterval);
        Assert.Equal("orders", result.Database);
    }
}
