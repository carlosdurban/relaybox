namespace Relaybox.Serialization;

/// <summary>Serializes message contracts to and from the outbox payload.</summary>
public interface IMessageSerializer
{
    string Serialize(object message);

    object Deserialize(string payload, Type type);
}
