using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace OrderService.Api.Data.Migrations
{
    /// <inheritdoc />
    public partial class AddColumn_TraceId_OutboxMessagesTable : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "TraceId",
                table: "OutboxMessages",
                type: "text",
                nullable: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "TraceId",
                table: "OutboxMessages");
        }
    }
}
