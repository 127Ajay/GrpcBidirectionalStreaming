# gRPC Bidirectional Streaming with .NET 10

A simple, practical **gRPC Bidirectional Streaming** application built with **C# and .NET 10**.

This project is designed to demonstrate how a client and server can maintain a single gRPC connection and **continuously send and receive messages independently**.

The example implements a simple **Order Processing** scenario where:

- The client sends multiple order requests.
- The server processes each request.
- The server sends multiple responses for a single request.
- The client continuously receives responses while still sending requests.
- Both request and response streams remain active independently.
- The client and server communicate using a single bidirectional streaming RPC.

---

## Table of Contents

- [Overview](#overview)
- [What is Bidirectional Streaming](#what-is-bidirectional-streaming)
- [What This Project Demonstrates](#what-this-project-demonstrates)
- [Technologies Used](#technologies-used)
- [Project Structure](#project-structure)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Creating the Project](#creating-the-project)
- [Creating the Solution](#creating-the-solution)
- [Creating the gRPC Server](#creating-the-grpc-server)
- [Creating the Client](#creating-the-client)
- [Creating the Protobuf Contract](#creating-the-protobuf-contract)
- [Configuring the Server](#configuring-the-server)
- [Configuring the Client](#configuring-the-client)
- [Implementing the gRPC Server](#implementing-the-grpc-server)
- [Implementing the Client](#implementing-the-client)
- [Running the Application](#running-the-application)
- [Understanding the Client Send Loop](#understanding-the-client-send-loop)
- [Understanding the Client Receive Loop](#understanding-the-client-receive-loop)
- [Understanding Concurrent Communication](#understanding-concurrent-communication)
- [Understanding CompleteAsync](#understanding-completeasync)
- [Understanding the Server Stream](#understanding-the-server-stream)
- [Expected Output](#expected-output)
- [Important gRPC Concepts](#important-grpc-concepts)
- [Bidirectional Streaming vs Unary RPC](#bidirectional-streaming-vs-unary-rpc)
- [Testing Different Scenarios](#testing-different-scenarios)
- [Common Issues](#common-issues)
- [Key Takeaways](#key-takeaways)

---

# Overview

gRPC supports four types of RPC communication:

1. Unary RPC
2. Server Streaming
3. Client Streaming
4. Bidirectional Streaming

This project focuses specifically on **Bidirectional Streaming**.

The application uses the following communication model:

```text
                    ONE gRPC CONNECTION

        ┌─────────────────────────────────────┐
        │                                     │
        │             gRPC Server             │
        │                                     │
        └──────────────────┬──────────────────┘
                           │
                    Bidirectional
                       Stream
                           │
        ┌──────────────────┴──────────────────┐
        │                                     │
        │             gRPC Client             │
        │                                     │
        └─────────────────────────────────────┘
```

The client has two independent operations:

```text
Client
  │
  ├── Send Requests ──────────────► Server
  │
  └── Receive Responses ◄───────── Server
```

The important point is that **sending and receiving happen concurrently**.

---

# What is Bidirectional Streaming?

In a normal unary gRPC call, the client sends one request and receives one response:

```text
Client                         Server

Request ──────────────────────►

Response ◄─────────────────────
```

With bidirectional streaming, both sides have a stream:

```text
Client                         Server

Request ──────────────────────►
Request ──────────────────────►
Request ──────────────────────►

Response ◄────────────────────
Response ◄────────────────────
Response ◄────────────────────
```

The client does not have to wait for a response before sending another request.

For example:

```text
Client                         Server

ORD-001 ──────────────────────►
ORD-002 ──────────────────────►
ORD-003 ──────────────────────►

         ◄──────────────────── ORD-001 RECEIVED
         ◄──────────────────── ORD-001 PROCESSING

ORD-004 ──────────────────────►

         ◄──────────────────── ORD-001 COMPLETED
         ◄──────────────────── ORD-002 RECEIVED
```

This makes bidirectional streaming useful for scenarios such as:

- Real-time communication
- Live order processing
- Notifications
- Chat systems
- Monitoring
- IoT communication
- Long-running operations
- Real-time status updates
- Streaming data pipelines

---

# What This Project Demonstrates

This project demonstrates:

- Creating a gRPC server using ASP.NET Core
- Creating a gRPC client using C#
- Defining a gRPC contract using Protocol Buffers
- Creating a bidirectional streaming RPC
- Sending multiple requests over one connection
- Receiving multiple responses over one connection
- Using `IAsyncStreamReader<T>`
- Using `IServerStreamWriter<T>`
- Using `IClientStreamWriter<T>`
- Using `AsyncDuplexStreamingCall<TRequest, TResponse>`
- Running send and receive operations concurrently
- Using `Task.WhenAll`
- Completing the client request stream
- Handling cancellation using `CancellationToken`
- Understanding the lifecycle of a streaming RPC

---

# Technologies Used

| Technology | Version / Usage |
|---|---|
| C# | C# |
| .NET | .NET 10 |
| ASP.NET Core | gRPC Server |
| gRPC | Bidirectional Streaming |
| Protocol Buffers | Service Contract |
| Grpc.Net.Client | gRPC Client |
| Google.Protobuf | Protobuf support |
| Grpc.Tools | Protobuf code generation |

---

# Project Structure

```text
GrpcBidirectionalStreaming/
│
├── GrpcBidirectionalStreaming.sln
│
├── GrpcBidirectionalStreaming.Server/
│   │
│   ├── Protos/
│   │   └── order.proto
│   │
│   ├── Services/
│   │   └── OrderStreamingService.cs
│   │
│   ├── Program.cs
│   └── GrpcBidirectionalStreaming.Server.csproj
│
└── GrpcBidirectionalStreaming.Client/
    │
    ├── Program.cs
    └── GrpcBidirectionalStreaming.Client.csproj
```

---

# Architecture

The application follows this communication flow:

```text
                    ┌─────────────────────────┐
                    │      gRPC Client        │
                    │                         │
                    │  SendOrdersAsync()      │
                    │          │              │
                    │          ▼              │
                    │    RequestStream        │
                    │                         │
                    │    ResponseStream       │
                    │          ▲              │
                    │          │              │
                    │ ReceiveResponsesAsync() │
                    └──────────┬──────────────┘
                               │
                         HTTP/2 + gRPC
                               │
                    ┌──────────▼──────────────┐
                    │      gRPC Server        │
                    │                         │
                    │ OrderStreamingService   │
                    │                         │
                    │  Request Stream         │
                    │        │                │
                    │        ▼                │
                    │ Order Processing        │
                    │        │                │
                    │        ▼                │
                    │  Response Stream        │
                    └─────────────────────────┘
```

The important architectural concept is that the request and response streams are independent.

---

# Prerequisites

Install the following:

- .NET 10 SDK
- Visual Studio / JetBrains Rider / VS Code
- Git

Verify the installed .NET version:

```bash
dotnet --version
```

Verify installed SDKs:

```bash
dotnet --list-sdks
```

You should see a .NET 10 SDK:

```text
10.0.xxx
```

---

# Creating the Project

## 1. Create the Root Directory

```bash
mkdir GrpcBidirectionalStreaming
cd GrpcBidirectionalStreaming
```

---

# Creating the Solution

Create the solution:

```bash
dotnet new sln -n GrpcBidirectionalStreaming
```

The structure will initially be:

```text
GrpcBidirectionalStreaming/
└── GrpcBidirectionalStreaming.sln
```

---

# Creating the gRPC Server

Create an ASP.NET Core gRPC project:

```bash
dotnet new grpc -n GrpcBidirectionalStreaming.Server
```

Add the project to the solution:

```bash
dotnet sln add GrpcBidirectionalStreaming.Server/GrpcBidirectionalStreaming.Server.csproj
```

The server project will initially contain the default gRPC example.

Delete the default files:

```text
Protos/greet.proto
Services/GreeterService.cs
```

The server structure should become:

```text
GrpcBidirectionalStreaming.Server/
│
├── Protos/
├── Services/
├── Program.cs
└── GrpcBidirectionalStreaming.Server.csproj
```

---

# Creating the Client

Create a console application:

```bash
dotnet new console -n GrpcBidirectionalStreaming.Client
```

Add it to the solution:

```bash
dotnet sln add GrpcBidirectionalStreaming.Client/GrpcBidirectionalStreaming.Client.csproj
```

The solution now contains:

```text
GrpcBidirectionalStreaming/
│
├── GrpcBidirectionalStreaming.sln
│
├── GrpcBidirectionalStreaming.Server/
│
└── GrpcBidirectionalStreaming.Client/
```

---

# Installing Client Packages

Navigate to the client project:

```bash
cd GrpcBidirectionalStreaming.Client
```

Install the required packages:

```bash
dotnet add package Grpc.Net.Client
```

```bash
dotnet add package Google.Protobuf
```

```bash
dotnet add package Grpc.Tools
```

Return to the root directory:

```bash
cd ..
```

---

# Creating the Protobuf Contract

Create:

```text
GrpcBidirectionalStreaming.Server/Protos/order.proto
```

Add the following:

```protobuf
syntax = "proto3";

option csharp_namespace = "GrpcBidirectionalStreaming";

package orders;

service OrderStreaming {

    rpc ProcessOrders(stream OrderRequest)
        returns (stream OrderResponse);
}

message OrderRequest {

    string order_id = 1;

    OrderAction action = 2;

    string product_name = 3;

    int32 quantity = 4;
}

message OrderResponse {

    string order_id = 1;

    OrderStatus status = 2;

    string message = 3;

    string server_time = 4;
}

enum OrderAction {

    UNKNOWN = 0;

    CREATE = 1;

    CANCEL = 2;

    STATUS = 3;
}

enum OrderStatus {

    RECEIVED = 0;

    PROCESSING = 1;

    COMPLETED = 2;

    CANCELLED = 3;

    FAILED = 4;
}
```

---

# Understanding the Protobuf Contract

The most important part is:

```protobuf
rpc ProcessOrders(stream OrderRequest)
    returns (stream OrderResponse);
```

The first `stream` means the client can send multiple requests.

The second `stream` means the server can send multiple responses.

Therefore:

```text
stream OrderRequest
        │
        ▼
      Client
        │
        │
        ▼
      Server
        │
        │
        ▼
stream OrderResponse
```

This is a **bidirectional streaming RPC**.

---

# Configuring the Server

Open:

```text
GrpcBidirectionalStreaming.Server/GrpcBidirectionalStreaming.Server.csproj
```

Make sure the protobuf file is configured for server-side code generation:

```xml
<ItemGroup>
  <Protobuf Include="Protos\order.proto" GrpcServices="Server" />
</ItemGroup>
```

---

# Configuring the Client

Open:

```text
GrpcBidirectionalStreaming.Client/GrpcBidirectionalStreaming.Client.csproj
```

Add:

```xml
<ItemGroup>
  <Protobuf Include="..\GrpcBidirectionalStreaming.Server\Protos\order.proto"
            GrpcServices="Client"
            Link="Protos\order.proto" />
</ItemGroup>
```

The same `.proto` contract is therefore used by both applications.

```text
                   order.proto
                       │
              ┌────────┴────────┐
              ▼                 ▼
           Server             Client
              │                 │
       Server classes      Client classes
```

---

# Build the Solution

From the solution root:

```bash
dotnet build
```

Expected result:

```text
Build succeeded.
```

Building at this point is useful because it confirms that the protobuf contract and generated gRPC classes are valid before implementing the service.

---

# Implementing the gRPC Server

Create:

```text
GrpcBidirectionalStreaming.Server/Services/OrderStreamingService.cs
```

Add:

```csharp
using Grpc.Core;

namespace GrpcBidirectionalStreaming.Server.Services;

public class OrderStreamingService : OrderStreaming.OrderStreamingBase
{
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

            await Task.Delay(
                1000,
                context.CancellationToken);

            await responseStream.WriteAsync(
                new OrderResponse
                {
                    OrderId = request.OrderId,
                    Status = OrderStatus.Processing,
                    Message = "Order is being processed",
                    ServerTime = DateTime.UtcNow.ToString("O")
                });

            await Task.Delay(
                2000,
                context.CancellationToken);

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
}
```

---

# Understanding the Server Method

The method receives three important objects:

```csharp
IAsyncStreamReader<OrderRequest> requestStream
```

This represents the incoming stream from the client.

```csharp
IServerStreamWriter<OrderResponse> responseStream
```

This represents the outgoing stream to the client.

```csharp
ServerCallContext context
```

This contains information about the current gRPC request, including cancellation.

---

## Reading Requests

Requests are read using:

```csharp
await foreach (
    var request in requestStream.ReadAllAsync(
        context.CancellationToken))
{
    // Process request
}
```

This continuously reads incoming messages.

Conceptually:

```text
Client

ORD-001 ────────┐
ORD-002 ────────┤
ORD-003 ────────┤
ORD-004 ────────┤
                │
                ▼
        requestStream
                │
                ▼
             Server
```

---

## Sending Responses

The server sends responses using:

```csharp
await responseStream.WriteAsync(response);
```

A single request can produce multiple responses:

```text
ORD-001
   │
   ├── RECEIVED
   ├── PROCESSING
   └── COMPLETED
```

This is an important property of streaming communication.

---

# Configuring Program.cs

Open:

```text
GrpcBidirectionalStreaming.Server/Program.cs
```

Replace the contents with:

```csharp
using GrpcBidirectionalStreaming.Server.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddGrpc();

var app = builder.Build();

app.MapGrpcService<OrderStreamingService>();

app.MapGet("/", () =>
    "gRPC Bidirectional Streaming Server");

app.Run();
```

The important registration is:

```csharp
builder.Services.AddGrpc();
```

and:

```csharp
app.MapGrpcService<OrderStreamingService>();
```

---

# Running the Server

Navigate to the server project:

```bash
cd GrpcBidirectionalStreaming.Server
```

Run:

```bash
dotnet run
```

You should see something similar to:

```text
Now listening on: https://localhost:7001
```

The actual port can differ depending on the generated project configuration.

Keep the server running.

---

# HTTPS Development Certificate

The default ASP.NET Core gRPC template uses HTTPS.

If the client reports a certificate-related error, run:

```bash
dotnet dev-certs https --trust
```

Then restart the server.

---

# Implementing the Client

Open:

```text
GrpcBidirectionalStreaming.Client/Program.cs
```

The first thing we need is a gRPC channel:

```csharp
using Grpc.Net.Client;
using GrpcBidirectionalStreaming;

var channel = GrpcChannel.ForAddress(
    "https://localhost:7001");
```

The URL must match the address where the gRPC server is running.

---

# Creating the gRPC Client

Create the generated gRPC client:

```csharp
var client =
    new OrderStreaming.OrderStreamingClient(channel);
```

Now create the bidirectional streaming call:

```csharp
using var call = client.ProcessOrders();
```

The important part is:

```csharp
client.ProcessOrders();
```

Because `ProcessOrders` is defined as:

```protobuf
rpc ProcessOrders(stream OrderRequest)
    returns (stream OrderResponse);
```

the generated client provides access to both streams.

```text
call
 │
 ├── RequestStream
 │
 └── ResponseStream
```

---

# Understanding RequestStream

The client sends requests through:

```csharp
call.RequestStream
```

For example:

```csharp
await call.RequestStream.WriteAsync(
    new OrderRequest
    {
        OrderId = "ORD-001",
        Action = OrderAction.Create,
        ProductName = "Laptop",
        Quantity = 2
    });
```

The request travels:

```text
Client
   │
   │ RequestStream
   ▼
 gRPC
   │
   ▼
Server
```

---

# Understanding ResponseStream

The client receives responses through:

```csharp
call.ResponseStream
```

Responses can be read using:

```csharp
await foreach (
    var response in call.ResponseStream.ReadAllAsync())
{
    Console.WriteLine(response.Message);
}
```

The response travels:

```text
Server
   │
   │ ResponseStream
   ▼
 gRPC
   │
   ▼
Client
```

---

# Implementing the Send Loop

Create the following method:

```csharp
static async Task SendOrdersAsync(
    IClientStreamWriter<OrderRequest> requestStream)
{
    for (int i = 1; i <= 5; i++)
    {
        var orderId = $"ORD-{i:000}";

        var request = new OrderRequest
        {
            OrderId = orderId,
            Action = OrderAction.Create,
            ProductName = $"Product-{i}",
            Quantity = i
        };

        Console.WriteLine(
            $"[REQUEST] Sending {orderId}");

        await requestStream.WriteAsync(request);

        await Task.Delay(500);
    }

    await requestStream.CompleteAsync();

    Console.WriteLine(
        "[REQUEST] Request stream completed.");
}
```

This method sends five orders.

The generated order IDs are:

```text
ORD-001
ORD-002
ORD-003
ORD-004
ORD-005
```

---

# Implementing the Receive Loop

Create:

```csharp
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
```

This method continuously waits for server responses.

The flow is:

```text
Wait for response
       │
       ▼
Receive response
       │
       ▼
Process response
       │
       ▼
Wait for next response
       │
       └───────────────┐
                       │
                       ▼
                  Repeat
```

---

# Running Send and Receive Concurrently

This is the most important part of the client.

Create the streaming call:

```csharp
using var call = client.ProcessOrders();
```

Start the send operation:

```csharp
var sendTask = SendOrdersAsync(
    call.RequestStream);
```

Start the receive operation:

```csharp
var receiveTask = ReceiveResponsesAsync(
    call.ResponseStream);
```

Then wait for both:

```csharp
await Task.WhenAll(
    sendTask,
    receiveTask);
```

This gives us:

```text
                 gRPC Connection
                       │
           ┌───────────┴───────────┐
           │                       │
           ▼                       ▼
      Send Task               Receive Task
           │                       │
           ▼                       ▼
    RequestStream            ResponseStream
           │                       ▲
           │                       │
           └───────────┬───────────┘
                       │
                       ▼
                     Server
```

The client can therefore send and receive at the same time.

---

# Complete Client Program.cs

The complete client implementation is:

```csharp
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


static async Task SendOrdersAsync(
    IClientStreamWriter<OrderRequest> requestStream)
{
    for (int i = 1; i <= 5; i++)
    {
        var orderId = $"ORD-{i:000}";

        var request = new OrderRequest
        {
            OrderId = orderId,
            Action = OrderAction.Create,
            ProductName = $"Product-{i}",
            Quantity = i
        };

        Console.WriteLine(
            $"[REQUEST] Sending {orderId}");

        await requestStream.WriteAsync(request);

        await Task.Delay(500);
    }

    await requestStream.CompleteAsync();

    Console.WriteLine(
        "[REQUEST] Request stream completed.");
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
```

---

# Running the Client

Open another terminal.

Navigate to:

```bash
cd GrpcBidirectionalStreaming.Client
```

Run:

```bash
dotnet run
```

The client connects to the server and starts the bidirectional stream.

---

# Expected Output

You should see output similar to:

```text
Connected to gRPC server.

[REQUEST] Sending ORD-001

[RESPONSE] OrderId: ORD-001
           Status: RECEIVED
           Message: Order received

[REQUEST] Sending ORD-002
[REQUEST] Sending ORD-003

[RESPONSE] OrderId: ORD-001
           Status: PROCESSING
           Message: Order is being processed

[REQUEST] Sending ORD-004
[REQUEST] Sending ORD-005

[REQUEST] Request stream completed.

[RESPONSE] OrderId: ORD-001
           Status: COMPLETED
           Message: Order processing completed

[RESPONSE] OrderId: ORD-002
           Status: RECEIVED
           Message: Order received

[RESPONSE] OrderId: ORD-002
           Status: PROCESSING
           Message: Order is being processed

[RESPONSE] OrderId: ORD-002
           Status: COMPLETED
           Message: Order processing completed

...
```

The exact timing and ordering of console output can vary.

---

# Understanding the Communication

The client sends:

```text
ORD-001
ORD-002
ORD-003
ORD-004
ORD-005
```

The server processes each order.

For every order, the server sends:

```text
RECEIVED
    ↓
PROCESSING
    ↓
COMPLETED
```

Therefore one request can generate multiple responses.

For example:

```text
Client                         Server

ORD-001 ──────────────────────►

         ◄──────────────────── ORD-001 RECEIVED

         ◄──────────────────── ORD-001 PROCESSING

         ◄──────────────────── ORD-001 COMPLETED
```

---

# Why Send and Receive Must Be Separate

Consider this implementation:

```csharp
await SendOrdersAsync(
    call.RequestStream);

await ReceiveResponsesAsync(
    call.ResponseStream);
```

This creates a sequential workflow:

```text
Send everything
      │
      ▼
Finish sending
      │
      ▼
Start receiving
```

That isn't the behavior we want from a continuously bidirectional stream.

Instead:

```csharp
var sendTask = SendOrdersAsync(
    call.RequestStream);

var receiveTask = ReceiveResponsesAsync(
    call.ResponseStream);

await Task.WhenAll(
    sendTask,
    receiveTask);
```

allows:

```text
Send
  │
  ├──────────────►
  │
  │      Receive
  │         ▲
  │         │
  ├────────►│
  │         │
  ├────────►│
  │         │
  │         │
```

Both operations are active at the same time.

---

# Understanding CompleteAsync()

One of the most important concepts is:

```csharp
await requestStream.CompleteAsync();
```

This means:

> The client has finished sending requests.

It does **not** mean:

> Close the entire gRPC connection immediately.

For example:

```text
Client                         Server

ORD-001 ──────────────────────►
ORD-002 ──────────────────────►
ORD-003 ──────────────────────►

CompleteAsync()
        │
        └─────────────────────►

         ◄──────────────────── Response
         ◄──────────────────── Response
         ◄──────────────────── Response
```

The server can still send responses after the client has completed its request stream.

---

# Server-Side Stream Lifecycle

The server reads requests using:

```csharp
await foreach (
    var request in requestStream.ReadAllAsync(
        context.CancellationToken))
{
    // Process request
}
```

This loop continues until:

- The client completes the request stream.
- The client disconnects.
- The RPC is cancelled.
- The server encounters an error.

After the request stream ends:

```csharp
Console.WriteLine("Client disconnected.");
```

is executed.

---

# Using CancellationToken

The server receives a `ServerCallContext`:

```csharp
ServerCallContext context
```

The context provides:

```csharp
context.CancellationToken
```

This should be passed to long-running asynchronous operations:

```csharp
await Task.Delay(
    1000,
    context.CancellationToken);
```

It allows the operation to stop when the client disconnects or the RPC is cancelled.

The request stream also uses it:

```csharp
requestStream.ReadAllAsync(
    context.CancellationToken)
```

This is important for long-lived streaming connections.

---

# Bidirectional Streaming vs Unary RPC

## Unary

```protobuf
rpc GetOrder(OrderRequest)
    returns (OrderResponse);
```

Communication:

```text
Client ───── Request ─────► Server

Client ◄──── Response ───── Server
```

One request and one response.

---

## Server Streaming

```protobuf
rpc GetOrders(OrderRequest)
    returns (stream OrderResponse);
```

Communication:

```text
Client ───── Request ─────► Server

Client ◄──── Response ──── Server
Client ◄──── Response ──── Server
Client ◄──── Response ──── Server
```

The server streams multiple responses.

---

## Client Streaming

```protobuf
rpc UploadOrders(stream OrderRequest)
    returns (OrderResponse);
```

Communication:

```text
Client ───── Request ─────► Server
Client ───── Request ─────► Server
Client ───── Request ─────► Server

Client ◄──── Response ──── Server
```

The client streams multiple requests.

---

## Bidirectional Streaming

```protobuf
rpc ProcessOrders(stream OrderRequest)
    returns (stream OrderResponse);
```

Communication:

```text
Client ───── Request ─────► Server
Client ───── Request ─────► Server
Client ◄──── Response ───── Server
Client ───── Request ─────► Server
Client ◄──── Response ───── Server
Client ◄──── Response ───── Server
Client ───── Request ─────► Server
```

Both sides can continuously communicate.

---

# Important gRPC Types Used

## `IAsyncStreamReader<T>`

Used to read messages from a stream.

Server:

```csharp
IAsyncStreamReader<OrderRequest>
```

Client:

```csharp
IAsyncStreamReader<OrderResponse>
```

Example:

```csharp
await foreach (
    var response in responseStream.ReadAllAsync())
{
    // Process response
}
```

---

# `IClientStreamWriter<T>`

Used by the client to send messages.

```csharp
IClientStreamWriter<OrderRequest>
```

Example:

```csharp
await requestStream.WriteAsync(
    request);
```

---

# `IServerStreamWriter<T>`

Used by the server to send messages.

```csharp
IServerStreamWriter<OrderResponse>
```

Example:

```csharp
await responseStream.WriteAsync(
    response);
```

---

# `AsyncDuplexStreamingCall<TRequest, TResponse>`

The client receives an object representing the entire bidirectional streaming call:

```csharp
using var call = client.ProcessOrders();
```

Conceptually:

```text
AsyncDuplexStreamingCall
│
├── RequestStream
│
└── ResponseStream
```

This object represents the active streaming RPC.

---

# Testing Different Scenarios

## Test 1 — Increase the Number of Orders

Change:

```csharp
for (int i = 1; i <= 5; i++)
```

to:

```csharp
for (int i = 1; i <= 20; i++)
```

Run the client again.

Observe that the client continues sending requests while the server is processing previous requests.

---

# Test 2 — Slow Down the Server

Change:

```csharp
await Task.Delay(
    1000,
    context.CancellationToken);
```

to:

```csharp
await Task.Delay(
    5000,
    context.CancellationToken);
```

Now the server takes longer to process each request.

The client can still send requests because the send and receive operations are independent.

---

# Test 3 — Speed Up the Client

Change:

```csharp
await Task.Delay(500);
```

to:

```csharp
await Task.Delay(100);
```

The client will send requests much faster than the server processes them.

This is useful for observing the difference between:

```text
Request production
```

and:

```text
Request processing
```

---

# Test 4 — Send Different Actions

The protobuf contract supports:

```protobuf
enum OrderAction {

    UNKNOWN = 0;

    CREATE = 1;

    CANCEL = 2;

    STATUS = 3;
}
```

You can send:

```csharp
Action = OrderAction.Create
```

or:

```csharp
Action = OrderAction.Cancel
```

or:

```csharp
Action = OrderAction.Status
```

For example:

```csharp
await requestStream.WriteAsync(
    new OrderRequest
    {
        OrderId = "ORD-100",
        Action = OrderAction.Cancel,
        ProductName = "Keyboard",
        Quantity = 1
    });
```

---

# Common Issues

## 1. Certificate Error

If you see an HTTPS certificate error:

```bash
dotnet dev-certs https --trust
```

Then restart the server.

---

## 2. Incorrect Port

If the server is running on:

```text
https://localhost:7001
```

the client must use:

```csharp
GrpcChannel.ForAddress(
    "https://localhost:7001");
```

If the server uses another port, update the client accordingly.

---

## 3. Server Not Running

If the client cannot connect, make sure the server is running first:

```bash
cd GrpcBidirectionalStreaming.Server
dotnet run
```

Then start the client:

```bash
cd GrpcBidirectionalStreaming.Client
dotnet run
```

---

## 4. Protobuf Changes Not Reflected

If you modify `order.proto`, rebuild the solution:

```bash
dotnet clean
dotnet build
```

The generated C# classes will then be regenerated.

---

# Key Takeaways

The most important concept in this project is that a bidirectional streaming RPC provides **two independent streams over a single gRPC call**.

```text
                     gRPC Call
                         │
             ┌───────────┴───────────┐
             │                       │
             ▼                       ▼
       Request Stream          Response Stream
             │                       ▲
             │                       │
             ▼                       │
           Server ◄──────────────────┘
```

The client sends using:

```csharp
await call.RequestStream.WriteAsync(request);
```

The client receives using:

```csharp
await foreach (
    var response in call.ResponseStream.ReadAllAsync())
{
}
```

The server receives using:

```csharp
await foreach (
    var request in requestStream.ReadAllAsync())
{
}
```

The server sends using:

```csharp
await responseStream.WriteAsync(response);
```

And the client keeps both operations running concurrently:

```csharp
var sendTask = SendOrdersAsync(
    call.RequestStream);

var receiveTask = ReceiveResponsesAsync(
    call.ResponseStream);

await Task.WhenAll(
    sendTask,
    receiveTask);
```

The central mental model is:

```text
                  BIDIRECTIONAL STREAM

       CLIENT                              SERVER
         │                                   │
         │                                   │
         │────── Request 1 ─────────────────►│
         │────── Request 2 ─────────────────►│
         │◄───── Response 1 ────────────────│
         │────── Request 3 ─────────────────►│
         │◄───── Response 2 ────────────────│
         │◄───── Response 3 ────────────────│
         │────── Request 4 ─────────────────►│
         │◄───── Response 4 ────────────────│
         │                                   │
```

The client does **not** need to follow a strict:

```text
Request → Response → Request → Response
```

pattern.

Instead, both directions can operate independently over the same streaming RPC.

---

# Final Project

After completing the implementation, the repository should contain:

```text
GrpcBidirectionalStreaming/
│
├── GrpcBidirectionalStreaming.sln
│
├── GrpcBidirectionalStreaming.Server/
│   │
│   ├── Protos/
│   │   └── order.proto
│   │
│   ├── Services/
│   │   └── OrderStreamingService.cs
│   │
│   ├── Program.cs
│   └── GrpcBidirectionalStreaming.Server.csproj
│
└── GrpcBidirectionalStreaming.Client/
    │
    ├── Program.cs
    └── GrpcBidirectionalStreaming.Client.csproj
```

To run the application:

### Terminal 1 — Server

```bash
cd GrpcBidirectionalStreaming.Server
dotnet run
```

### Terminal 2 — Client

```bash
cd GrpcBidirectionalStreaming.Client
dotnet run
```

The result is a simple but complete **.NET 10 gRPC Bidirectional Streaming application** that demonstrates continuous, concurrent communication between a client and server over a single gRPC connection.
