using System.Text.Json;

namespace Relaybox.Serialization;

internal sealed class JsonMessageSerializer : IMessageSerializer
{
    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);

    public string Serialize(object message) => JsonSerializer.Serialize(message, message.GetType(), Options);

    public object Deserialize(string payload, Type type) =>
        JsonSerializer.Deserialize(payload, type, Options)
        ?? throw new JsonException($"Payload deserialized to null for '{type.FullName}'.");
}
