using BuildingBlocks;
using Microsoft.EntityFrameworkCore;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using OrderService.Api;
using OrderService.Api.Data;
using OrderService.Api.DTOs;
using OrderService.Api.Handlers;
using RabbitMQ.Client;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenApi();

builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.Converters.Add(new System.Text.Json.Serialization.JsonStringEnumConverter());
});

builder.Services.AddDbContext<OrderDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("DefaultConnection")));

var oltpEndpoint = new Uri(builder.Configuration["OpenTelemetry:OltpEndpoint"] ?? throw new InvalidOperationException("OpenTelemetry OltpEndpoint is not configured"));

builder.Services.AddOpenTelemetry()
    .ConfigureResource(x => x.AddService("OrderService"))
    .WithTracing(tracing => tracing
        .AddAspNetCoreInstrumentation()
        .AddHttpClientInstrumentation()
        .AddSource(Telemetry.ServiceName)
        .AddOtlpExporter(options =>
        {
            options.Endpoint = oltpEndpoint;
        }))
    .WithMetrics(metrics => metrics
        .AddMeter(Telemetry.ServiceName)
        .AddAspNetCoreInstrumentation()
        .AddHttpClientInstrumentation()
        .AddOtlpExporter((exporterOptions, readerOptions) =>
        {
            exporterOptions.Endpoint = oltpEndpoint;
            readerOptions.PeriodicExportingMetricReaderOptions.ExportIntervalMilliseconds = 5000;
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

builder.Services.AddScoped<CreateOrderHandler>();
builder.Services.AddScoped<GetOrderHandler>();
builder.Services.AddScoped<PaymentApprovedHandler>();
builder.Services.AddScoped<PaymentRejectedHandler>();

builder.Services.AddHostedService<PaymentApprovedConsumer>();
builder.Services.AddHostedService<PaymentRejectedConsumer>();

builder.Services.AddHostedService<OutboxProcessor>();

var app = builder.Build();

var scopeFactory = app.Services.GetRequiredService<IServiceScopeFactory>();

using var scope = scopeFactory.CreateScope();
var dbContext = scope.ServiceProvider.GetRequiredService<OrderDbContext>();
await dbContext.Database.MigrateAsync();

Telemetry.ConfigureGauges(scopeFactory);

app.UseHttpsRedirection();

app.MapPost("/orders", async (CreateOrderRequest request, CreateOrderHandler createOrderHandler) =>
{
    var order = await createOrderHandler.HandleAsync(request);

    return Results.Created($"/orders/{order.Id}", order);
});

app.MapGet("/orders/{id:guid}", async (Guid id, GetOrderHandler getOrderHandler) =>
{
    var order = await getOrderHandler.HandleAsync(id);

    return Results.Ok(order);
});

app.Run();
