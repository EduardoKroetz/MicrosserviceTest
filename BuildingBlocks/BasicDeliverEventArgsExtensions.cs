using RabbitMQ.Client.Events;
using System.Diagnostics;
using System.Text;
using System.Text.Json;

namespace BuildingBlocks;

public static class BasicDeliverEventArgsExtensions
{
    public static Activity? CreateActivityFromEventArgs(this BasicDeliverEventArgs ea, string activityName, ActivitySource activitySource, ActivityKind activityKind = ActivityKind.Consumer)
    {
        var traceParent = ea.BasicProperties.Headers?.TryGetValue("traceparent", out var traceParentObj) == true
            ? Encoding.UTF8.GetString((byte[])traceParentObj!)
            : null;

        ActivityContext.TryParse(traceParent, traceState: null, out var activityContext);

        var activity = activitySource.StartActivity(activityName, activityKind, activityContext);

        return activity;
    }

    public static TEvent? DeserializeEvent<TEvent>(this BasicDeliverEventArgs ea)
    {
        var ev = JsonSerializer.Deserialize<TEvent?>(ea.Body.ToArray());

        return ev;
    }
}
