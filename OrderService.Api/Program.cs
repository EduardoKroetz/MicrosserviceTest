using Microsoft.EntityFrameworkCore;
using OrderService.Api.Data;
using OrderService.Api.DTOs;
using OrderService.Api.Models;
using RabbitMQ.Client;
using Shared.Contracts;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenApi();

builder.Services.AddDbContext<OrderDbContext>(options =>
    options.UseSqlite(builder.Configuration.GetConnectionString("DefaultConnection")));

builder.Services.AddSingleton<IConnection>(sp =>
{
    var factory = new ConnectionFactory
    {
        HostName = builder.Configuration["RabbitMQ:HostName"] ?? throw new InvalidOperationException("RabbitMQ HostName is not configured"),
        Port = int.Parse(builder.Configuration["RabbitMQ:Port"] ?? throw new InvalidOperationException("RabbitMQ Port is not configured")),
        UserName = builder.Configuration["RabbitMQ:UserName"] ?? throw new InvalidOperationException("RabbitMQ UserName is not configured"),
        Password = builder.Configuration["RabbitMQ:Password"] ?? throw new InvalidOperationException("RabbitMQ Password is not configured")
    };
    return factory.CreateConnectionAsync().GetAwaiter().GetResult();
});

builder.Services.AddSingleton<IEventBusPublisher, RabbitMqPublisher>();
builder.Services.AddSingleton<OrderPublisher>();

var app = builder.Build();

using var scope = app.Services.CreateScope();
var dbContext = scope.ServiceProvider.GetRequiredService<OrderDbContext>();
await dbContext.Database.MigrateAsync();

app.UseHttpsRedirection();

app.MapPost("/orders", async (CreateOrderRequest request, OrderDbContext dbContext, OrderPublisher orderPublisher) =>
{
    var order = new Order
    {
        Id = Guid.NewGuid(),
        Status = OrderStatus.Pending,
        TotalAmount = request.TotalAmount,
        OrderDate = DateTime.UtcNow
    };

    await dbContext.Orders.AddAsync(order);
    await dbContext.SaveChangesAsync();

    var orderCreatedEvent = new OrderCreatedEvent(order.Id, order.TotalAmount);
    await orderPublisher.PublishOrderCreatedEventAsync(orderCreatedEvent, CancellationToken.None);

    return Results.Created($"/orders/{order.Id}", order);
});

app.MapGet("/orders/{id:guid}", async (Guid id, OrderDbContext dbContext) =>
{
    var order = await dbContext.Orders.FindAsync(id);
    if (order == null)
    {
        return Results.NotFound("Order not found");
    }

    return Results.Ok(order);
});

app.Run();

public class OrderPublisher
{
    private readonly IEventBusPublisher _eventBusPublisher;
    private const string Exchange = "order.exchange";

    public OrderPublisher(IEventBusPublisher eventBusPublisher)
    {
        _eventBusPublisher = eventBusPublisher;
    }

    public async Task PublishOrderCreatedEventAsync(OrderCreatedEvent orderCreatedEvent, CancellationToken ct)
    {
        await _eventBusPublisher.PublishAsync(orderCreatedEvent, Exchange, "order.created", ct);
    }
}

public interface IEventBusPublisher
{
    Task PublishAsync(Event ev, string exchange, string routingKey, CancellationToken ct);
}

public class RabbitMqPublisher : IEventBusPublisher, IAsyncDisposable
{
    private readonly IConnection _connection;
    private IChannel? _channel;

    public RabbitMqPublisher(IConnection connection)
    {
        _connection = connection;
    }

    private async Task<IChannel> GetChannelAsync(CancellationToken ct)
    {
        if (_channel is { IsOpen: true })
        {
            return _channel;
        }

        _channel = await _connection.CreateChannelAsync(options: null, ct);

        await _channel.ExchangeDeclareAsync(
            exchange: "order.exchange",
            type: ExchangeType.Topic,
            durable: true,
            autoDelete: false,
            cancellationToken: ct);

        return _channel;
    }

    public async Task PublishAsync(Event ev, string exchange, string routingKey, CancellationToken ct)
    {
        var channel = await GetChannelAsync(ct);

        var body = JsonSerializer.SerializeToUtf8Bytes(ev, ev.GetType());

        var props = new BasicProperties
        {
            MessageId = ev.EventId.ToString(),
            ContentType = "application/json",
            DeliveryMode = DeliveryModes.Persistent,
            Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
            Type = ev.GetType().Name
        };


        await channel.BasicPublishAsync(
            exchange: exchange,
            routingKey: routingKey,
            mandatory: true,
            basicProperties: props,
            body: body,
            cancellationToken: ct);
    }

    public async ValueTask DisposeAsync()
    {
        if (_channel is not null)
            await _channel.DisposeAsync();
    }
}