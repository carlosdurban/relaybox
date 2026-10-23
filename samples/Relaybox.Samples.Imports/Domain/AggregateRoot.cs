namespace Relaybox.Samples.Imports.Domain;

public abstract class AggregateRoot<TId>
{
    private readonly List<IDomainEvent> _domainEvents = [];

    public TId Id { get; protected set; } = default!;

    public int Version { get; private set; }

    public IReadOnlyList<IDomainEvent> DomainEvents => _domainEvents;

    protected void Raise(IDomainEvent @event) => _domainEvents.Add(@event);

    public void ClearDomainEvents() => _domainEvents.Clear();

    public void IncrementVersion() => Version++;
}

public interface IDomainEvent
{
    DateTime OccurredAt { get; }
}

public sealed class DomainException(string message) : Exception(message);
