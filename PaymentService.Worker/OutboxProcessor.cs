using BuildingBlocks;
using Microsoft.EntityFrameworkCore;
using PaymentService.Worker.Data;
using Shared.Contracts;
using System.Text.Json;

namespace PaymentService.Worker;

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
        const int maxLimitMessages = 20;

        while (!stoppingToken.IsCancellationRequested)
        {
            using var scope = _serviceProvider.CreateScope();
            var dbContext = scope.ServiceProvider.GetRequiredService<PaymentDbContext>();

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
                        nameof(PaymentApprovedEvent) => ((Event)JsonSerializer.Deserialize<PaymentApprovedEvent>(message.Content)!, "payment.approved"),
                        nameof(PaymentRejectedEvent) => ((Event)JsonSerializer.Deserialize<PaymentRejectedEvent>(message.Content)!, "payment.rejected"),
                        _ => throw new InvalidOperationException($"Unknown event type: {message.Type}")
                    };

                    await _messageBusConnection.PublishAsync(ev, routingKey, "payment.exchange", stoppingToken);

                    message.ProcessedOnUtc = DateTime.UtcNow;

                    _logger.LogInformation("Published event {EventType} with ID {EventId} to exchange {Exchange} with routing key {RoutingKey}.", message.Type, message.EventId, "payment.exchange", routingKey);
                }
                catch (Exception ex) when (ex is JsonException or InvalidOperationException or NotSupportedException)
                {
                    // PERMANENTE: JSON inválido, tipo desconhecido no switch, serialização impossível. Nunca vai funcionar em retry.
                    message.RetryCount = maxRetryCount;
                    message.Error = ex.Message;
                }
                catch (Exception ex)
                {
                    // TRANSITÓRIO: broker fora, rede, timeout. Pode funcionar depois.
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