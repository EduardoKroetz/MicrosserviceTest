using System.ComponentModel.DataAnnotations;

namespace OrderService.Api.DTOs;

public class CreateOrderRequest
{
    [Required(ErrorMessage = "The total amount is required.")]
    public decimal TotalAmount { get; set; }
}
