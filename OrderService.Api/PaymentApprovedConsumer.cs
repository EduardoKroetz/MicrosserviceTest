using OrderService.Api.Handlers;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Shared.Contracts;
using System.Text.Json;

namespace OrderService.Api;

public class PaymentApprovedConsumer : BackgroundService
{
    private readonly IConnection _connection;
    private readonly ILogger<PaymentApprovedConsumer> _logger;
    private readonly IServiceScopeFactory _serviceScopeFactory;
    private IChannel? _channel;

    private const string Exchange = "payment.exchange";
    private const string Queue = "payment.approved.orders";
    private const string Event = "payment.approved";

    public PaymentApprovedConsumer(IConnection connection, ILogger<PaymentApprovedConsumer> logger, IServiceScopeFactory serviceScopeFactory)
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
            var ev = JsonSerializer.Deserialize<PaymentApprovedEvent>(ea.Body.ToArray());
            if (ev is null || string.IsNullOrEmpty(messageId))
            {
                await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
                return;
            }

            using var scope = _serviceScopeFactory.CreateScope();
            var paymentApprovedHandler = scope.ServiceProvider.GetRequiredService<PaymentApprovedHandler>();
            await paymentApprovedHandler.HandleAsync(ev, ea.CancellationToken);

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