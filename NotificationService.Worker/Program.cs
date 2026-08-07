using BuildingBlocks;
using Microsoft.EntityFrameworkCore;
using NotificationService.Worker;
using NotificationService.Worker.Data;
using NotificationService.Worker.Handlers;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using RabbitMQ.Client;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddDbContext<NotificationDbContext>(options =>
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
            instrumentName: Telemetry.JourneyDurationName,
            new ExplicitBucketHistogramConfiguration
            {
                Boundaries = new double[] { 7, 12, 25, 45, 70, 90, 130, 180, 260, 360 }
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

builder.Services.AddSingleton<IMessageBusConnection, RabbitMqConnection>();

builder.Services.AddScoped<PaymentApprovedHandler>();
builder.Services.AddScoped<PaymentRejectedHandler>();

builder.Services.AddHostedService<PaymentApprovedConsumer>();
builder.Services.AddHostedService<PaymentRejectedConsumer>();

var host = builder.Build();

using var scope = host.Services.CreateScope();
var dbContext = scope.ServiceProvider.GetRequiredService<NotificationDbContext>();
await dbContext.Database.MigrateAsync();

host.Run();
