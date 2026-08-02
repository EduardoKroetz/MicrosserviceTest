using BuildingBlocks;
using Microsoft.EntityFrameworkCore;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using PaymentService.Worker;
using PaymentService.Worker.Data;
using PaymentService.Worker.Handlers;
using RabbitMQ.Client;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHostedService<OrderCreatedConsumer>();
builder.Services.AddHostedService<OutboxProcessor>();

builder.Services.AddDbContext<PaymentDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("DefaultConnection")));

builder.Services.AddOpenTelemetry()
    .ConfigureResource(x => x.AddService("PaymentService"))
    .WithTracing(tracing => tracing
        .AddSource("PaymentService.OrderCreatedConsumer")
        .AddSource("PaymentService.OrderCreatedHandler")
        .AddSource("PaymentService.OutboxProcessor")
        .AddOtlpExporter(options =>
        {
            options.Endpoint = new Uri(builder.Configuration["OpenTelemetry:OltpEndpoint"] ?? throw new InvalidOperationException("OpenTelemetry OltpEndpoint is not configured"));
        }));

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

builder.Services.AddScoped<OrderCreatedHandler>();

builder.Services.AddSingleton<IMessageBusConnection, RabbitMqConnection>();

var host = builder.Build();

using var scope = host.Services.CreateScope();
var dbContext = scope.ServiceProvider.GetRequiredService<PaymentDbContext>();
await dbContext.Database.MigrateAsync();

host.Run();
