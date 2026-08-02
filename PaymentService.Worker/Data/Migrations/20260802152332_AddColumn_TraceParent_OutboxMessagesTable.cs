using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace PaymentService.Worker.Migrations
{
    /// <inheritdoc />
    public partial class AddColumn_TraceParent_OutboxMessagesTable : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "TraceParent",
                table: "OutboxMessages",
                type: "text",
                nullable: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "TraceParent",
                table: "OutboxMessages");
        }
    }
}
