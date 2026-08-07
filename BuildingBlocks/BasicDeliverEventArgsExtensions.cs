using RabbitMQ.Client.Events;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json;

namespace BuildingBlocks;

public static class BasicDeliverEventArgsExtensions
{
    public static Activity? CreateActivityFromEventArgs(this BasicDeliverEventArgs ea, string activityName, ActivitySource activitySource, ActivityKind activityKind = ActivityKind.Consumer)
    {
        var traceParent = ea.GetTraceParent();

        ActivityContext.TryParse(traceParent, traceState: null, out var activityContext);

        var activity = activitySource.StartActivity(activityName, activityKind, activityContext);

        return activity;
    }

    public static string? GetTraceParent(this BasicDeliverEventArgs ea)
    {
        if (ea.BasicProperties.Headers?.TryGetValue("traceparent", out var traceParentObj) == true && traceParentObj is byte[] traceParentBytes)
        {
            return Encoding.UTF8.GetString(traceParentBytes);
        }
        return null;
    }

    public static TEvent? DeserializeEvent<TEvent>(this BasicDeliverEventArgs ea)
    {
        var ev = JsonSerializer.Deserialize<TEvent?>(ea.Body.ToArray());

        return ev;
    }

    public static DateTime? GetJourneyStartedAtUtc(this BasicDeliverEventArgs ea)
    {
        if (ea.BasicProperties.Headers?.TryGetValue("journeyStartedAtUtc", out var journeyStartedAtObj) == true && journeyStartedAtObj is byte[] journeyStartedAtBytes)
        {
            var journeyStartedAtString = Encoding.UTF8.GetString(journeyStartedAtBytes);
            if (DateTime.TryParse(journeyStartedAtString, null, DateTimeStyles.RoundtripKind, out var journeyStartedAt))
            {
                return journeyStartedAt;
            }
        }
        return null;
    }

}
