using OrderService.Api.Handlers;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Shared.Contracts;
using System.Text.Json;

namespace OrderService.Api;

public class PaymentRejectedConsumer : BackgroundService
{
    private readonly IConnection _connection;
    private readonly IServiceScopeFactory _serviceScopeFactory;
    private readonly ILogger<PaymentRejectedConsumer> _logger;
    private IChannel? _channel;

    private const string Exchange = "payment.exchange";
    private const string Queue = "payment.rejected.orders";
    private const string Event = "payment.rejected";

    public PaymentRejectedConsumer(IConnection connection, ILogger<PaymentRejectedConsumer> logger, IServiceScopeFactory serviceScopeFactory)
    {
        _connection = connection;
        _logger = logger;
        _serviceScopeFactory = serviceScopeFactory;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        _channel = await _connection.CreateChannelAsync(cancellationToken: ct);

        await _channel.ExchangeDeclareAsync(Exchange, ExchangeType.Topic, durable: true, autoDelete: false, cancellationToken: ct);

        await _channel.QueueDeclareAsync(Queue, durable: true, exclusive: false, autoDelete: false, cancellationToken: ct);

        await _channel.QueueBindAsync(Queue, Exchange, Event, cancellationToken: ct);

        var consumer = new AsyncEventingBasicConsumer(_channel);
        consumer.ReceivedAsync += OnMessageAsync;

        await _channel.BasicConsumeAsync(Queue, autoAck: false, consumer: consumer, cancellationToken: ct);
    }

    private async Task OnMessageAsync(object sender, BasicDeliverEventArgs ea)
    {
        var channel = ((AsyncEventingBasicConsumer)sender).Channel;
        var messageId = ea.BasicProperties.MessageId;

        try
        {
            var ev = JsonSerializer.Deserialize<PaymentRejectedEvent>(ea.Body.ToArray());
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
        catch (JsonException)
        {
            await channel.BasicNackAsync(ea.DeliveryTag, false, requeue: false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Falha ao processar {MessageId}", messageId);
            await channel.BasicNackAsync(ea.DeliveryTag, false, requeue: false);
        }
    }
}