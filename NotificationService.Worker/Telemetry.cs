using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace NotificationService.Worker;

public static class Telemetry
{
    public const string ServiceName = "NotificationService";

    public static readonly ActivitySource ActivitySource = new(ServiceName);
    public static readonly Meter Meter = new(ServiceName);

    public const string JourneyDurationName = "order_journey_duration_seconds";
    public static readonly Histogram<double> JourneyDuration = Meter.CreateHistogram<double>(JourneyDurationName);
}
