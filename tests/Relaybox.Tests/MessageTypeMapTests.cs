using Relaybox.Serialization;

namespace Relaybox.Tests;

public sealed class MessageTypeMapTests
{
    private readonly MessageTypeMap _map = new();

    [Fact]
    public void Uses_the_mapped_name_in_both_directions()
    {
        _map.Map(typeof(OrderPlaced), "orders.placed");

        Assert.Equal("orders.placed", _map.GetName(typeof(OrderPlaced)));
        Assert.True(_map.TryGetType("orders.placed", out var type));
        Assert.Equal(typeof(OrderPlaced), type);
    }

    [Fact]
    public void Falls_back_to_the_type_name()
    {
        Assert.Equal(nameof(OrderPlaced), _map.GetName(typeof(OrderPlaced)));
    }

    [Fact]
    public void Rejects_a_name_used_by_another_type()
    {
        _map.Map(typeof(OrderPlaced), "orders.placed");

        Assert.Throws<InvalidOperationException>(() => _map.Map(typeof(OrderShipped), "orders.placed"));
    }

    [Fact]
    public void Rejects_a_second_name_for_the_same_type()
    {
        _map.Map(typeof(OrderPlaced), "orders.placed");

        Assert.Throws<InvalidOperationException>(() => _map.Map(typeof(OrderPlaced), "orders.created"));
    }

    private sealed record OrderPlaced;

    private sealed record OrderShipped;
}
