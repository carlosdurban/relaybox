using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Relaybox.Relay;

namespace Relaybox.Tests;

public sealed class TenantOutboxProcessorTests
{
    private readonly FakeTimeProvider _time = new(new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero));
    private readonly InMemoryOutboxStore _store;
    private readonly RecordingPublisher _publisher = new();
    private readonly OutboxRelayOptions _options = new();

    public TenantOutboxProcessorTests() => _store = new InMemoryOutboxStore(_time);

    [Fact]
    public async Task Publishes_pending_messages_oldest_first_and_marks_them_processed()
    {
        var first = _store.Add("acme");
        _time.Advance(TimeSpan.FromSeconds(1));
        var second = _store.Add("acme");

        var hasMore = await CreateProcessor().ProcessAsync("acme", CancellationToken.None);

        Assert.False(hasMore);
        Assert.Equal([first.Id, second.Id], _publisher.Published.Select(p => p.Message.Id));
        Assert.Empty(_store.Pending("acme"));
    }

    [Fact]
    public async Task Publishes_from_a_scope_that_belongs_to_the_tenant()
    {
        _store.Add("acme");

        await CreateProcessor().ProcessAsync("acme", CancellationToken.None);

        Assert.Equal("acme", Assert.Single(_publisher.Published).ScopeTenant);
    }

    [Fact]
    public async Task A_failed_publication_keeps_the_message_pending_with_backoff_and_stops_the_round()
    {
        var message = _store.Add("acme");
        _store.Add("acme");
        _publisher.Down = true;

        var hasMore = await CreateProcessor().ProcessAsync("acme", CancellationToken.None);

        Assert.False(hasMore);
        var failure = Assert.Single(_store.Retries);
        Assert.Equal(message.Id, failure.Id);
        Assert.Equal(_time.GetUtcNow().UtcDateTime + _options.RetryBaseDelay, failure.RetryAt);
        Assert.Equal(2, _store.Pending("acme").Count);
    }

    [Fact]
    public async Task A_failed_message_is_published_once_its_backoff_expires()
    {
        _store.Add("acme");
        _publisher.Down = true;
        var processor = CreateProcessor();
        await processor.ProcessAsync("acme", CancellationToken.None);
        _publisher.Down = false;

        await processor.ProcessAsync("acme", CancellationToken.None);
        Assert.Empty(_publisher.Published);

        _time.Advance(_options.RetryBaseDelay + TimeSpan.FromMilliseconds(1));
        await processor.ProcessAsync("acme", CancellationToken.None);

        Assert.Equal(2, Assert.Single(_publisher.Published).Message.Attempts);
    }

    [Fact]
    public async Task A_permanently_rejected_message_is_failed_at_once_and_the_rest_keep_flowing()
    {
        var poison = _store.Add("acme", type: "Poison");
        var next = _store.Add("acme");
        _publisher.Rejected["Poison"] = true;

        await CreateProcessor().ProcessAsync("acme", CancellationToken.None);

        Assert.Equal(poison.Id, Assert.Single(_store.Failed("acme")).Id);
        Assert.Contains("MessageRejectedException", _store.ErrorOf("acme", poison.Id));
        Assert.Equal(next.Id, Assert.Single(_publisher.Published).Message.Id);
        Assert.Empty(_store.Pending("acme"));
    }

    [Fact]
    public async Task A_rejected_message_is_retried_and_failed_after_the_maximum_attempts()
    {
        _options.MaxAttempts = 3;
        var message = _store.Add("acme", type: "Rejected");
        _publisher.Rejected["Rejected"] = false;
        var processor = CreateProcessor();

        for (var attempt = 1; attempt <= 3; attempt++)
        {
            Assert.Empty(_store.Failed("acme"));
            await processor.ProcessAsync("acme", CancellationToken.None);
            _time.Advance(_options.RetryMaxDelay);
        }

        Assert.Equal(2, _store.Retries.Count);
        Assert.Equal(message.Id, Assert.Single(_store.Failed("acme")).Id);
    }

    [Fact]
    public async Task An_unreachable_broker_never_fails_messages_however_long_it_lasts()
    {
        _options.MaxAttempts = 2;
        _store.Add("acme");
        _publisher.Down = true;
        var processor = CreateProcessor();

        for (var attempt = 0; attempt < 5; attempt++)
        {
            await processor.ProcessAsync("acme", CancellationToken.None);
            _time.Advance(_options.RetryMaxDelay);
        }

        Assert.Empty(_store.Failed("acme"));
        Assert.Equal(5, _store.Retries.Count);
    }

    [Fact]
    public async Task Stops_when_a_message_belongs_to_another_tenant()
    {
        _store.Add("acme", ownerTenantId: "globex");

        await Assert.ThrowsAsync<TenantMismatchException>(() => CreateProcessor().ProcessAsync("acme", CancellationToken.None));

        Assert.Empty(_publisher.Published);
    }

    [Fact]
    public async Task Yields_after_the_per_round_limit_and_reports_more_work()
    {
        _options.MaxMessagesPerTenantPerRound = 2;
        for (var i = 0; i < 3; i++)
        {
            _store.Add("acme");
        }

        var hasMore = await CreateProcessor().ProcessAsync("acme", CancellationToken.None);

        Assert.True(hasMore);
        Assert.Equal(2, _publisher.Published.Count);
    }

    [Theory]
    [InlineData(1, 5)]
    [InlineData(2, 10)]
    [InlineData(4, 40)]
    [InlineData(20, 300)]
    public void Retry_delay_doubles_up_to_the_maximum(int attempts, int expectedSeconds)
    {
        Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), _options.RetryDelay(attempts));
    }

    private TenantOutboxProcessor CreateProcessor()
    {
        var services = new ServiceCollection()
            .AddLogging()
            .AddSingleton<TimeProvider>(_time)
            .AddSingleton<IOutboxStore>(_store)
            .AddSingleton(_publisher)
            .AddScoped<IMessagePublisher, ScopedRecordingPublisher>()
            .AddSingleton(Options.Create(_options))
            .AddSingleton<TenantOutboxProcessor>();
        services.AddRelaybox();

        return services.BuildServiceProvider().GetRequiredService<TenantOutboxProcessor>();
    }
}
