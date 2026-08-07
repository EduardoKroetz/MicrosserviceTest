using OrderService.Api.Data;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace OrderService.Api;

public static class Telemetry
{
    public const string ServiceName = "OrderService";

    public static readonly ActivitySource ActivitySource = new(ServiceName);
    public static Meter Meter = new(ServiceName);

    public static void ConfigureGauges(IServiceScopeFactory scopeFactory)
    {
        Meter.CreateObservableGauge(
            "order_outbox_pending_messages",
            () =>
            {
                using var scope = scopeFactory.CreateScope();
                var dbContext = scope.ServiceProvider.GetRequiredService<OrderDbContext>();
                return dbContext.OutboxMessages.Count(m => m.ProcessedOnUtc == null && m.RetryCount < 5);
            });
    }
}
