using Grpc.Core;
using Grpc.Net.Client;
using GrpcBidirectionalStreaming;

var channel = GrpcChannel.ForAddress(
    "https://localhost:7001");

var client =
    new OrderStreaming.OrderStreamingClient(channel);

using var call = client.ProcessOrders();

Console.WriteLine(
    "Connected to gRPC server.");

var sendTask = SendOrdersAsync(
    call.RequestStream);

var receiveTask = ReceiveResponsesAsync(
    call.ResponseStream);

await Task.WhenAll(
    sendTask,
    receiveTask);

Console.WriteLine(
    "Streaming completed.");


//static async Task SendOrdersAsync(
//    IClientStreamWriter<OrderRequest> requestStream)
//{
//    for (int i = 1; i <= 5; i++)
//    {
//        var orderId = $"ORD-{i:000}";

//        var request = new OrderRequest
//        {
//            OrderId = orderId,
//            Action = OrderAction.Create,
//            ProductName = $"Product-{i}",
//            Quantity = i
//        };

//        Console.WriteLine(
//            $"[REQUEST] Sending {orderId}");

//        await requestStream.WriteAsync(request);

//        await Task.Delay(50);
//    }

//    await requestStream.CompleteAsync();

//    Console.WriteLine(
//        "[REQUEST] Request stream completed.");
//}


static async Task SendOrdersAsync(
    IClientStreamWriter<OrderRequest> requestStream)
{
    await requestStream.WriteAsync(
        new OrderRequest
        {
            OrderId = "ORD-001",
            Action = OrderAction.Create,
            ProductName = "Laptop",
            Quantity = 2
        });

    await Task.Delay(500);

    await requestStream.WriteAsync(
        new OrderRequest
        {
            OrderId = "ORD-002",
            Action = OrderAction.Status,
            ProductName = "Monitor",
            Quantity = 1
        });

    await Task.Delay(500);

    await requestStream.WriteAsync(
        new OrderRequest
        {
            OrderId = "ORD-003",
            Action = OrderAction.Cancel,
            ProductName = "Keyboard",
            Quantity = 1
        });

    await requestStream.CompleteAsync();
}

static async Task ReceiveResponsesAsync(
    IAsyncStreamReader<OrderResponse> responseStream)
{
    try
    {
        await foreach (var response in responseStream.ReadAllAsync())
        {
            Console.WriteLine(
                $"[RESPONSE] " +
                $"OrderId: {response.OrderId} | " +
                $"Status: {response.Status} | " +
                $"Message: {response.Message}");
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine(
            $"[RESPONSE] Error: {ex.Message}");
    }
}