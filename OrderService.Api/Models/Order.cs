namespace OrderService.Api.Models;

public class Order
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public required decimal TotalAmount { get; set; }
    public required OrderStatus Status { get; set; }
    public DateTime OrderDate { get; set; } = DateTime.UtcNow;
}

public enum OrderStatus
{
    Pending,
    Processing,
    Completed,
    Cancelled
}
