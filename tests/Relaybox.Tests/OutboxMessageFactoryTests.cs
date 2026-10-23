using Microsoft.Extensions.DependencyInjection;
using Relaybox.Serialization;

namespace Relaybox.Tests;

public sealed class OutboxMessageFactoryTests
{
    private readonly OutboxMessageFactory _factory;

    public OutboxMessageFactoryTests()
    {
        var services = new ServiceCollection();
        services.AddRelaybox().MapMessage<ImportCompleted>("imports.completed.v1");
        _factory = services.BuildServiceProvider().GetRequiredService<OutboxMessageFactory>();
    }

    [Fact]
    public void Serializes_the_contract_with_its_stable_name()
    {
        var message = _factory.Create("acme", new ImportCompleted("imp-1"));

        Assert.Equal("imports.completed.v1", message.Type);
        Assert.Equal("""{"importId":"imp-1"}""", message.Payload);
        Assert.Equal("acme", message.TenantId);
        Assert.Null(message.Destination);
    }

    [Fact]
    public void Keeps_the_destination_routing_key_and_headers_of_the_message()
    {
        var message = _factory.Create("acme", new ImportCompleted("imp-1"), new OutboxMessageOptions
        {
            Destination = "billing",
            RoutingKey = "imports.acme.completed",
            Headers = new Dictionary<string, string> { ["priority"] = "high" },
        });

        Assert.Equal("billing", message.Destination);
        Assert.Equal("imports.acme.completed", message.RoutingKey);
        Assert.Equal("high", message.Headers!["priority"]);
    }

    [Theory]
    [InlineData("tenant-id")]
    [InlineData("TraceParent")]
    public void Reserved_headers_cannot_be_overridden(string header)
    {
        var options = new OutboxMessageOptions { Headers = new Dictionary<string, string> { [header] = "x" } };

        Assert.Throws<ArgumentException>(() => _factory.Create("acme", new ImportCompleted("imp-1"), options));
    }

    private sealed record ImportCompleted(string ImportId);
}
