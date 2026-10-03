using Grpc.Core;

namespace GrpcBidirectionalStreaming.Server.Service
{
    public class OrderStreamingService : OrderStreaming.OrderStreamingBase
    {
        #region OldLogic
        /*
        public override async Task ProcessOrders(
        IAsyncStreamReader<OrderRequest> requestStream,
        IServerStreamWriter<OrderResponse> responseStream,
        ServerCallContext context)
        {
            Console.WriteLine("Client connected.");

            await foreach (
                var request in requestStream.ReadAllAsync(
                    context.CancellationToken))
            {
                Console.WriteLine(
                    $"Received: {request.OrderId} | " +
                    $"{request.Action} | " +
                    $"{request.ProductName}");

                await responseStream.WriteAsync(
                    new OrderResponse
                    {
                        OrderId = request.OrderId,
                        Status = OrderStatus.Received,
                        Message = "Order received",
                        ServerTime = DateTime.UtcNow.ToString("O")
                    });

                await Task.Delay(100, context.CancellationToken);

                await responseStream.WriteAsync(
                    new OrderResponse
                    {
                        OrderId = request.OrderId,
                        Status = OrderStatus.Processing,
                        Message = "Order is being processed",
                        ServerTime = DateTime.UtcNow.ToString("O")
                    });

                await Task.Delay(200, context.CancellationToken);

                await responseStream.WriteAsync(
                    new OrderResponse
                    {
                        OrderId = request.OrderId,
                        Status = OrderStatus.Completed,
                        Message = "Order processing completed",
                        ServerTime = DateTime.UtcNow.ToString("O")
                    });
            }

            Console.WriteLine("Client disconnected.");
        }
        */
        #endregion OldLogic

        #region newLogic
        public override async Task ProcessOrders(
    IAsyncStreamReader<OrderRequest> requestStream,
    IServerStreamWriter<OrderResponse> responseStream,
    ServerCallContext context)
        {
            Console.WriteLine("Client connected.");

            await foreach (
                var request in requestStream.ReadAllAsync(
                    context.CancellationToken))
            {
                Console.WriteLine(
                    $"Received: {request.OrderId} | " +
                    $"Action: {request.Action}");

                switch (request.Action)
                {
                    case OrderAction.Create:

                        await ProcessCreateOrder(
                            request,
                            responseStream,
                            context.CancellationToken);

                        break;

                    case OrderAction.Cancel:

                        await ProcessCancelOrder(
                            request,
                            responseStream);

                        break;

                    case OrderAction.Status:

                        await SendResponse(
                            responseStream,
                            request.OrderId,
                            OrderStatus.Processing,
                            "Current order status requested");

                        break;

                    default:

                        await SendResponse(
                            responseStream,
                            request.OrderId,
                            OrderStatus.Failed,
                            "Unknown order action");

                        break;
                }
            }

            Console.WriteLine("Client disconnected.");
        }

        private async Task ProcessCreateOrder(
    OrderRequest request,
    IServerStreamWriter<OrderResponse> responseStream,
    CancellationToken cancellationToken)
        {
            await SendResponse(
                responseStream,
                request.OrderId,
                OrderStatus.Received,
                "Order received");

            await Task.Delay(
                1000,
                cancellationToken);

            await SendResponse(
                responseStream,
                request.OrderId,
                OrderStatus.Processing,
                "Order is being processed");

            await Task.Delay(
                2000,
                cancellationToken);

            await SendResponse(
                responseStream,
                request.OrderId,
                OrderStatus.Completed,
                "Order processing completed");
        }

        private async Task ProcessCancelOrder(
    OrderRequest request,
    IServerStreamWriter<OrderResponse> responseStream)
        {
            await SendResponse(
                responseStream,
                request.OrderId,
                OrderStatus.Cancelled,
                "Order cancelled");

            await Task.CompletedTask;
        }

        private static async Task SendResponse(
    IServerStreamWriter<OrderResponse> responseStream,
    string orderId,
    OrderStatus status,
    string message)
        {
            await responseStream.WriteAsync(
                new OrderResponse
                {
                    OrderId = orderId,
                    Status = status,
                    Message = message,
                    ServerTime = DateTime.UtcNow.ToString("O")
                });
        }
        #endregion
    }
}
