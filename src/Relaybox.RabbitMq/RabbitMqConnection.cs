using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace Relaybox.RabbitMq;

/// <summary>
/// Single connection shared by the publisher and the consumers. Created on first use; once created,
/// network failures are handled by the client's automatic recovery.
/// </summary>
internal sealed class RabbitMqConnection(IOptions<RabbitMqOptions> options) : IAsyncDisposable
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private IConnection? _connection;

    public async ValueTask<IConnection> GetAsync(CancellationToken ct)
    {
        if (_connection is { } connection)
        {
            return connection;
        }

        await _gate.WaitAsync(ct);
        try
        {
            if (_connection is null)
            {
                var settings = options.Value;
                var factory = new ConnectionFactory
                {
                    Uri = new Uri(settings.ConnectionString),
                    ClientProvidedName = settings.ClientProvidedName,
                    AutomaticRecoveryEnabled = true,
                    TopologyRecoveryEnabled = true,
                };
                _connection = await factory.CreateConnectionAsync(ct);
            }

            return _connection;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_connection is { } connection)
        {
            await connection.CloseAsync();
            await connection.DisposeAsync();
        }

        _gate.Dispose();
    }
}
