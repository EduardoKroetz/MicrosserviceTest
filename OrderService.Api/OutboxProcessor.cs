using BuildingBlocks;
using Microsoft.EntityFrameworkCore;
using OrderService.Api.Data;
using Shared.Contracts;
using System.Text.Json;

namespace OrderService.Api;

public class OutboxProcessor : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly IMessageBusConnection _messageBusConnection;
    private readonly ILogger<OutboxProcessor> _logger;

    public OutboxProcessor(IServiceProvider serviceProvider, IMessageBusConnection messageBusConnection, ILogger<OutboxProcessor> logger)
    {
        _serviceProvider = serviceProvider;
        _messageBusConnection = messageBusConnection;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        const int maxRetryCount = 5;
        const int maxLimitMessages = 100;
        const string exchange = "order.exchange";

        while (!stoppingToken.IsCancellationRequested)
        {
            using var scope = _serviceProvider.CreateScope();
            var dbContext = scope.ServiceProvider.GetRequiredService<OrderDbContext>();

            var messages = await dbContext.OutboxMessages
                .Where(m => m.ProcessedOnUtc == null && m.RetryCount < maxRetryCount)
                .OrderBy(m => m.OccurredOnUtc)
                .Take(maxLimitMessages)
                .ToListAsync(stoppingToken);

            foreach (var message in messages)
            {
                try
                {
                    var (ev, routingKey) = message.Type switch
                    {
                        nameof(OrderCreatedEvent) => ((Event)JsonSerializer.Deserialize<OrderCreatedEvent>(message.Content)!, "order.created"),
                        _ => throw new InvalidOperationException($"Unknown event type: {message.Type}")
                    };

                    await _messageBusConnection.PublishAsync(ev, routingKey, exchange, stoppingToken);

                    message.ProcessedOnUtc = DateTime.UtcNow;

                    _logger.LogInformation("Published event {EventType} with ID {EventId} to exchange {Exchange} with routing key {RoutingKey}.", message.Type, message.EventId, exchange, routingKey);
                }
                catch (Exception ex) when (ex is JsonException or InvalidOperationException or NotSupportedException)
                {
                    message.RetryCount = maxRetryCount;
                    message.Error = ex.Message;
                }
                catch (Exception ex)
                {
                    message.RetryCount++;
                    message.Error = ex.Message;
                    _logger.LogError(ex, "Failed to publish event {EventType} with ID {EventId}. Retry count: {RetryCount}", message.Type, message.EventId, message.RetryCount);
                }
            }

            await dbContext.SaveChangesAsync(stoppingToken);

            await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
        }

    }
}