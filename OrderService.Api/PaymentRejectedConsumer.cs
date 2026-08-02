using BuildingBlocks;
using OrderService.Api.Handlers;
using RabbitMQ.Client.Events;
using Shared.Contracts;
using System.Diagnostics;
using System.Text.Json;

namespace OrderService.Api;

public class PaymentRejectedConsumer : BackgroundService
{
    private readonly IServiceScopeFactory _serviceScopeFactory;
    private readonly ILogger<PaymentRejectedConsumer> _logger;
    private readonly IMessageBusConnection _messageBusConnection;

    private const string Exchange = "payment.exchange";
    private const string Queue = "payment.rejected.orders";
    private const string Event = "payment.rejected";

    private static readonly ActivitySource ActivitySource = new("OrderService.PaymentRejectedConsumer");

    public PaymentRejectedConsumer(ILogger<PaymentRejectedConsumer> logger, IServiceScopeFactory serviceScopeFactory, IMessageBusConnection messageBusConnection)
    {
        _logger = logger;
        _serviceScopeFactory = serviceScopeFactory;
        _messageBusConnection = messageBusConnection;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        await _messageBusConnection.ConsumeAsync(
            eventHandler: OnMessageAsync,
            exchange: Exchange,
            queue: Queue,
            routingKey: Event,
            ct: ct
        );
    }

    private async Task OnMessageAsync(object sender, BasicDeliverEventArgs ea)
    {
        var channel = ((AsyncEventingBasicConsumer)sender).Channel;
        var messageId = ea.BasicProperties.MessageId;

        using var activity = ea.CreateActivityFromEventArgs("Consume PaymentRejectedEvent", ActivitySource);

        try
        {
            var ev = ea.DeserializeEvent<PaymentRejectedEvent>();
            if (ev is null || string.IsNullOrEmpty(messageId))
            {
                await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
                return;
            }

            using var scope = _serviceScopeFactory.CreateScope();
            var paymentRejectedHandler = scope.ServiceProvider.GetRequiredService<PaymentRejectedHandler>();
            await paymentRejectedHandler.HandleAsync(ev, ea.CancellationToken);

            await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
        }
        catch (JsonException ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            await channel.BasicNackAsync(ea.DeliveryTag, false, requeue: false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Falha ao processar {MessageId}", messageId);
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            await channel.BasicNackAsync(ea.DeliveryTag, false, requeue: true);
        }
    }
}