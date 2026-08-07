using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace PaymentService.Worker;

public static class Telemetry
{
    public const string ServiceName = "PaymentService";

    public static readonly ActivitySource ActivitySource = new(ServiceName);
    public static readonly Meter Meter = new(ServiceName);

    public static readonly Counter<long> MessagesProcessed = Meter.CreateCounter<long>("messaging_messages_processed");
    public static readonly Counter<long> PaymentsProcessed = Meter.CreateCounter<long>("payments_processed");

    public static readonly Histogram<double> OrderCreatedHandlerDuration = Meter.CreateHistogram<double>("order_created_handler_duration_seconds");
}
