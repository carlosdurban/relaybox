using System.Collections.Concurrent;

namespace Relaybox.Serialization;

/// <summary>
/// Maps message contract types to the stable names written to the outbox and sent on the wire.
/// Unmapped types fall back to their CLR type name.
/// </summary>
public sealed class MessageTypeMap
{
    private readonly ConcurrentDictionary<Type, string> _names = new();
    private readonly ConcurrentDictionary<string, Type> _types = new(StringComparer.Ordinal);

    public void Map(Type type, string name)
    {
        ArgumentNullException.ThrowIfNull(type);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        var existingType = _types.GetOrAdd(name, type);
        if (existingType != type)
        {
            throw new InvalidOperationException(
                $"Message name '{name}' is already mapped to '{existingType.FullName}'.");
        }

        var existingName = _names.GetOrAdd(type, name);
        if (existingName != name)
        {
            throw new InvalidOperationException(
                $"Type '{type.FullName}' is already mapped to message name '{existingName}'.");
        }
    }

    public string GetName(Type type) => _names.TryGetValue(type, out var name) ? name : type.Name;

    public bool TryGetType(string name, out Type type) => _types.TryGetValue(name, out type!);

    internal void MapIfAbsent(Type type)
    {
        if (!_names.ContainsKey(type))
        {
            Map(type, type.Name);
        }
    }
}
