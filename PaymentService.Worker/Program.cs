using BuildingBlocks;
using Microsoft.EntityFrameworkCore;
using OpenTelemetry.Metrics;
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

var oltpEndpoint = new Uri(builder.Configuration["OpenTelemetry:OltpEndpoint"] ?? throw new InvalidOperationException("OpenTelemetry OltpEndpoint is not configured"));

builder.Services.AddOpenTelemetry()
    .ConfigureResource(x => x.AddService(Telemetry.ServiceName))
    .WithTracing(tracing => tracing
        .AddSource(Telemetry.ServiceName)
        .AddOtlpExporter(options =>
        {
            options.Endpoint = oltpEndpoint;
        }))
    .WithMetrics(metrics => metrics
        .AddMeter(Telemetry.Meter.Name)
        .AddView(
            instrumentName: "order_created_handler_duration_seconds",
            new ExplicitBucketHistogramConfiguration
            {
                Boundaries = new double[] { 0.5, 0.75, 1, 1.5, 2, 2.5, 3, 3.5, 4 }
            })
        .AddOtlpExporter(options =>
        {
            options.Endpoint = oltpEndpoint;
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
