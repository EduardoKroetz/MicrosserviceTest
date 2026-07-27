using BuildingBlocks;
using Microsoft.EntityFrameworkCore;
using NotificationService.Worker;
using NotificationService.Worker.Data;
using NotificationService.Worker.Handlers;
using RabbitMQ.Client;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddDbContext<NotificationDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("DefaultConnection")));

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
