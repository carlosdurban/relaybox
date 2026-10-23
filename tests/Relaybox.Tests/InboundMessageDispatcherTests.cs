using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Relaybox.Inbound;

namespace Relaybox.Tests;

public sealed class InboundMessageDispatcherTests
{
    private readonly InMemoryInbox _inbox = new();
    private readonly HandlerProbe _probe = new();
    private readonly InboundMessageDispatcher _dispatcher;

    public InboundMessageDispatcherTests()
    {
        var services = new ServiceCollection()
            .AddLogging()
            .AddSingleton(_probe)
            .AddSingleton(_inbox)
            .AddScoped<IInbox, InMemoryInboxAdapter>();
        services.AddRelaybox()
            .MapMessage<ImportCompleted>("import.completed")
            .AddMessageHandler<ImportCompleted, ImportCompletedHandler>()
            .ConfigureInbound(o =>
            {
                o.MaxAttempts = 3;
                o.RetryBaseDelay = TimeSpan.FromMilliseconds(1);
            });
        _dispatcher = services.BuildServiceProvider().GetRequiredService<InboundMessageDispatcher>();
    }

    [Fact]
    public async Task Runs_the_handler_in_a_scope_of_the_message_tenant()
    {
        var outcome = await _dispatcher.DispatchAsync(Message(), default);

        Assert.Equal(DispatchOutcome.Ack, outcome);
        var call = Assert.Single(_probe.Calls);
        Assert.Equal("imp-1", call.Message.ImportId);
        Assert.Equal("acme", call.ScopeTenant);
        Assert.Equal("msg-1", call.Context.MessageId);
    }

    [Fact]
    public async Task A_duplicate_is_acknowledged_without_running_the_handler_again()
    {
        await _dispatcher.DispatchAsync(Message(), default);

        var outcome = await _dispatcher.DispatchAsync(Message(), default);

        Assert.Equal(DispatchOutcome.Ack, outcome);
        Assert.Single(_probe.Calls);
    }

    [Fact]
    public async Task The_same_message_id_in_another_tenant_is_not_a_duplicate()
    {
        await _dispatcher.DispatchAsync(Message(), default);

        await _dispatcher.DispatchAsync(Message() with { TenantId = "globex" }, default);

        Assert.Equal(2, _probe.Calls.Count);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public async Task A_message_without_tenant_goes_to_the_dead_letter_queue(string? tenantId)
    {
        var outcome = await _dispatcher.DispatchAsync(Message() with { TenantId = tenantId }, default);

        Assert.Equal(DispatchOutcome.DeadLetter, outcome);
        Assert.Empty(_probe.Calls);
    }

    [Fact]
    public async Task A_message_without_handler_goes_to_the_dead_letter_queue()
    {
        var outcome = await _dispatcher.DispatchAsync(Message() with { MessageType = "unknown" }, default);

        Assert.Equal(DispatchOutcome.DeadLetter, outcome);
    }

    [Fact]
    public async Task An_unreadable_payload_goes_to_the_dead_letter_queue()
    {
        var outcome = await _dispatcher.DispatchAsync(Message() with { Body = "not json"u8.ToArray() }, default);

        Assert.Equal(DispatchOutcome.DeadLetter, outcome);
    }

    [Fact]
    public async Task A_domain_error_goes_to_the_dead_letter_queue_without_retrying()
    {
        _probe.Failures.Enqueue(new InvalidOperationException("Import already closed"));

        var outcome = await _dispatcher.DispatchAsync(Message(), default);

        Assert.Equal(DispatchOutcome.DeadLetter, outcome);
        Assert.Equal(1, _probe.Attempts);
        Assert.Equal(0, _inbox.Count);
    }

    [Fact]
    public async Task A_transient_error_is_retried_until_it_succeeds()
    {
        _probe.Failures.Enqueue(new TimeoutException());
        _probe.Failures.Enqueue(new TimeoutException());

        var outcome = await _dispatcher.DispatchAsync(Message(), default);

        Assert.Equal(DispatchOutcome.Ack, outcome);
        Assert.Equal(3, Assert.Single(_probe.Calls).Context.Attempt);
    }

    [Fact]
    public async Task A_persistent_transient_error_goes_to_the_dead_letter_queue_after_the_last_attempt()
    {
        for (var i = 0; i < 3; i++)
        {
            _probe.Failures.Enqueue(new TimeoutException());
        }

        var outcome = await _dispatcher.DispatchAsync(Message(), default);

        Assert.Equal(DispatchOutcome.DeadLetter, outcome);
        Assert.Equal(3, _probe.Attempts);
    }

    [Fact]
    public async Task Stopping_the_consumer_requeues_the_message()
    {
        using var cts = new CancellationTokenSource();
        _probe.OnHandle = () => cts.Cancel();
        _probe.Failures.Enqueue(new OperationCanceledException(cts.Token));

        var outcome = await _dispatcher.DispatchAsync(Message(), cts.Token);

        Assert.Equal(DispatchOutcome.Requeue, outcome);
    }

    private static InboundMessage Message() => new(
        MessageId: "msg-1",
        TenantId: "acme",
        MessageType: "import.completed",
        Body: Encoding.UTF8.GetBytes(JsonSerializer.Serialize(new ImportCompleted("imp-1", 42), new JsonSerializerOptions(JsonSerializerDefaults.Web))),
        TraceParent: null);

    public sealed record ImportCompleted(string ImportId, int Rows);

    private sealed class HandlerProbe
    {
        public List<(ImportCompleted Message, MessageContext Context, string ScopeTenant)> Calls { get; } = [];
        public Queue<Exception> Failures { get; } = new();
        public int Attempts { get; set; }
        public Action? OnHandle { get; set; }
    }

    private sealed class ImportCompletedHandler(HandlerProbe probe, ITenantContext tenantContext) : IMessageHandler<ImportCompleted>
    {
        public Task HandleAsync(ImportCompleted message, MessageContext context, CancellationToken ct)
        {
            probe.Attempts++;
            probe.OnHandle?.Invoke();
            if (probe.Failures.TryDequeue(out var failure))
            {
                throw failure;
            }

            probe.Calls.Add((message, context, tenantContext.TenantId));
            return Task.CompletedTask;
        }
    }
}
