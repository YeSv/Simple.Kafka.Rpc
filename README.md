## NuGet
[Simple.Kafka.Rpc](https://www.nuget.org/packages/Simple.Kafka.Rpc/)

Version mapping between [Simple.Kafka.Rpc](https://www.nuget.org/packages/Simple.Kafka.Rpc/) and [Confluent.Kafka](https://www.nuget.org/packages/Confluent.Kafka/):
1. 1.0.0 -> 1.0.0
2. 1.5.0+ -> 1.5.0+
3. 1.6.0 -> 1.6.0+
4. 1.7.0+ -> 1.7.0+
5. 1.8.0+ -> 1.8.0+ (latest)

## Table of contents
1. [Introduction](#1-introduction)
2. [Advantages and disadvantages](#2-advantages-and-disadvantages)
3. [Examples](#3-examples)
4. [Healthcheck](#4-healthcheck) [TODO]
5. [API](#5-api) [TODO]
6. [Configuration](#6-configuration) [TODO]


# 1. Introduction

`Simple.Kafka.Rpc` allows you to use Kafka not only as commit log but also for `Request/Response` type of communication between microservices, for example. What does it mean?

Let's write an example of http call to google.com in `C#` which might look like this:

```csharp

// Disposing a client here for demonstration purposes
using var client = new HttpClient();

var response = await client.SendAsync(new HttpRequestMessage(HttpMethod.Get, "http://google.com"));
var content = await response.Content.ReadAsByteArrayAsync();

Console.WriteLine($"Content.Length is {content.Length}");

```

To make this work you need:
1. Client application that can send requests and receive responses using `HttpClient`
2. Server application that can parse requests, handle them and return responses (in our case - google servers)

For you `HttpClient` implementation is a blackbox - all you need is a working client and server.

Let's now check an API that Simple.Kafka.Rpc provides:

``` csharp

using var rpc = RpcClient.Create(b => 
{
    b.Config.Topics = new[] { "responses-topic" }; // Required
    b.Consumer.Kafka.BootstrapServers = "localhost:9092"; // Required
    b.Producer.Kafka.BootstrapServers = "localhost:9092"; // Required
});

var consumeResult = await rpc.SendAsync(new Message<byte[], byte[]>
{
    Key = Array.Empty<byte>(),
    Value = Array.Empty<byte>()
}, "requests-topic");

var content = consumeResult.Message.Value;
Console.WriteLine($"Content.Length is {content.Length}");

```

We can use `RpcClient` the same way we used `HttpClient` before.

To make this work you need:
1. Client application that can send request messages and recieve response messages using `RpcClient`
2. Server application that can parse requests and consume from Kafka topic and produce responses

For you `RpcClient` implementation is also a blackbox - all you need is a working client and server :)

As you can see there are a lots of similarities. `Simple.Kafka.Rpc`'s workflow can be demonstated using the diagram below:

![Diagram](./diagrams/diagram.jpg)

`Simple.Kafka.Rpc` creates a consumer instance in background that is subscribed to responses topics provided in configuration and producer instance that produces messages when you call `Send` or `SendAsync`. `Simple.Kafka.Rpc` also checks that Kafka cluster is available, provides a healthcheck and is able to recreate consumer and producer instances transparently if it's possible.

Note that `Server` side should be implemented by yourself - you should create a consumer and producer and produce responses to corresponding topics in a manner that you wish to - in batches or one by one using `Confluent.Kafka`'s producer and consumer classes.

The question may arise - how `RpcClient` knows which response should be mapped to which request, well, `RpcClient` adds a special header to provided `Message<byte[], byte[]>` and you should populate it when you send a response. You can check how to propagate and extract it in an [examples](#3-examples) section.

# 2. Advantages and disadvantages

Let's start with advantages first:
1. Kafka has a high throughput so once implemented correctly 95th percentile can be really low for the whole flow - sending a request and receiving responses (depends on your use case and you can write load tests/benchmarks)
2. Library provides cluster failower discovery and can recreate producer/consumer instances
3. Health check is also available so if you use it in AspNetCore app you can restart your app in kubernetes if something goes wrong or remove it from load balancer, etc
4. Serializer/deserializer agnostic - library does not force you to use JSON/Protobuf/MessagePack/Avro etc, it's up to you, it does not modify bytes in a `Key` or `Value` - it only adds a header to your message
5. You can scale your server instances by changing number of partitions and use consumers with the same `GroupId`
6. You can also freely scale client instances - `Simple.Kafka.Rpc` uses `AutoOffsetReset.Latest` and random consumer group (which can be overriden) plus it never commits (can be changed to use `AutoCommits`)
7. Library does not enforces you to use specific Kafka client configuration - everything can be overriden

Disadvantages:
1. You should implement your own server instance that consumes/produces messages 
2. You can say that it's more complicated flow in terms of implementation
3. Relates to advantage (6) - if failover occurred and consumer instance is recreated - response is lost due to `AutoOffsetReset.Latest` so you should be aware that responses may be lost
4. Depending on your server implementation - requests can be lost too, but it's highly improbable using the same  `GroupId` and commiting once in a while using either `StoreOffset()` with `AutoCommitEnabled` or calling `Commit()` manually
5. You should be ready that requests can be retried by Kafka client

# 3. Examples

___

*Note:*

There is also an example of a todo list application (API) written using aspnet core which uses `Simple.Kafka.Rpc` client under the hood, sources:
1. [Todo list api](https://github.com/YeSv/Simple.Kafka.Rpc/blob/main/examples/Simple.Kafka.Rpc.Todo.Client)
2. [Backend](https://github.com/YeSv/Simple.Kafka.Rpc/blob/main/examples/Simple.Kafka.Rpc.Todo.Server)

___

In this section we are going to implement a primitive service-to-service communication with only one request and response contract. Client can issue a `Ping` requests and server returns `Pong` responses :)

Required steps:
1. Define request and response models
2. Define which serializer to use
3. Write a client
4. Write a server
5. Running kafka instance

Before we start, jfyi you can always check the source code for this section [here](https://github.com/YeSv/Simple.Kafka.Rpc/blob/main/examples/Simple.Kafka.Rpc.Readme).


First of all, we should define contracts (models), two structs with only one field of type `DateTime` should be enough:

``` csharp

    // Ping to a server
    [DataContract]
    public readonly struct Ping
    {
        public static readonly string Topic = "ping";

        [DataMember(Order = 0)] public readonly DateTime Time;

        public Ping(DateTime time) => Time = time;

        public static Ping New => new(DateTime.UtcNow);
    }

    // Response to corresponding ping 
    [DataContract]
    public readonly struct Pong
    {
        public static readonly string Topic = "pong";

        [DataMember(Order = 0)] public readonly DateTime Time;

        public Pong(DateTime time) => Time = time;

        public static Pong New => new(DateTime.UtcNow);
    }

```

As you can see we will send requests to `Ping.Topic` topic and recieve responses from `Pong.Topic` topic. Also our contracts only have one field for simplicity.

We are going to use [MessagePack](https://www.nuget.org/packages/MessagePack/) as serializer of choice but any other can do the trick, as it was mentioned previously, library does not enforce you to use any particular serializer.

To write client/server we only need two tasks - first one that will send requests and the second one that will consume requests and produce responses. For the sake of simplicity we will use a console application to emulate server and client.

Let's initialize rpc client (see the comments to understand what each line does):

```csharp

using var rpc = RpcClient.Create(b =>
{
    b.Config.Topics = new[] { Pong.Topic }; // Required

    // Here we specify logging for some useful events, you can skip it and those logs won't be printed 
    b.Consumer.RpcHandler.OnEof = consumeResult => Console.WriteLine($"[RPC] Got an EOF. TP: {consumeResult.TopicPartition}");
    b.Consumer.RpcHandler.OnRpcMessage = (header, result) => Console.WriteLine($"[RPC] Consumer received a response. Header: {header}. TPO: {result.TopicPartitionOffset}. Value len: {result.Message.Value.Length}");
    b.Consumer.RpcHandler.OnRpcLog = log => Console.WriteLine($"[RPC] Consumer log[{log.Level}] {log.Message}");

    // Consumer overrides:
    // All settings for consumer are available here: Consumer.Kafka (ConsumerConfig)
    b.Consumer.Kafka.BootstrapServers = "localhost:9092"; // Required

    // Consumer events: (all events are available here - like OnAssigned, OnRevoked, OnStatistics, etc.
    b.Consumer.KafkaHandler.OnAssigned = (c, assigned) => Console.WriteLine($"[RPC] Consumer partitions were assigned: {string.Join(",", assigned)}");

    // Producer config: (of type ProducerConfig)
    b.Producer.Kafka.BootstrapServers = "localhost:9092"; // Required

    // Kafka events like OnError, OnStatistics, etc
    b.Producer.KafkaHandler.OnError = (p, e) => Console.WriteLine($"[RPC] Producer error occurred: {e.Reason}");

    b.Producer.RpcHandler.OnRpcLog = log => Console.WriteLine($"[RPC] Producer log[{log.Level}] {log.Message}");
});

```

Let's also write an extensions so we don't have to provide topic name and value each time we call `SendAsync`:


```csharp

public static class PingPongRpcExtensions
{
    public static async Task<Pong> PingAsync(this IKafkaRpc rpc)
    {
        var consumeResult = await rpc.SendAsync(new Message<byte[], byte[]>
        {
            Key = Array.Empty<byte>(),
            Value = MessagePackSerializer.Serialize(Ping.New)
        }, Ping.Topic);

        return MessagePackSerializer.Deserialize<Pong>(consumeResult.Message.Value);
    }
}

```

We can now send a `Ping` request using just `rpc.PingAsync()` and recieve a `Pong` response.

Now it's time to add a client `Task` that will call our server with some interval:

``` csharp

using var stop = new CancellationTokenSource();

var clientTask = Task.Run(async () =>
{
    while (!stop.IsCancellationRequested)
    {
        var pong = await rpc.PingAsync();
        Console.WriteLine("Received pong response");

        await Task.Delay(5_000);
    }
}, stop.Token).ContinueWith(t => { });

```

We just send ping request once in 5 seconds until we decide to cancel `CancellationTokenSource` later in the code above.

Server `Task` code:

``` csharp

var serverTask = Task.Run(async () =>
{
    using var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
    {
        AutoOffsetReset = AutoOffsetReset.Earliest,
        GroupId = Guid.NewGuid().ToString(),
        EnablePartitionEof = true,
        BootstrapServers = "localhost:9092",
        EnableAutoCommit = true,
        EnableAutoOffsetStore = true
    }).Build();

    using var producer = new ProducerBuilder<byte[], byte[]>(new ProducerConfig
    {
        BootstrapServers = "localhost:9092"
    }).Build();

    consumer.Subscribe(Ping.Topic);

    while (!stop.IsCancellationRequested)
    {
        var consumeResult = consumer.Consume(stop.Token);
        if (consumeResult == null || consumeResult.IsPartitionEOF) continue;

        var rpcRequest = consumeResult.Message.GetRpcRequestId(); // This extension method is available with Simple.Kafka.Rpc package (REQUIRED)
        if (rpcRequest == null) continue; // Not a Simple.Kafka.Rpc request message

        var rpcRequestIdParseResult = rpcRequest.ParseRpcRequestId(); // This extension method is available with Simple.Kafka.Rpc package (not required to use, here - just for logging)
        if (!rpcRequestIdParseResult.IsOk) continue; // Failed to parse

        Console.WriteLine($"[Server] Received request with id: {rpcRequestIdParseResult.Ok}");

        producer.Produce(Pong.Topic, new Message<byte[], byte[]>
        {
            Key = Array.Empty<byte>(),
            Value = MessagePackSerializer.Serialize(Pong.New)
        }.WithRpcRequestId(rpcRequest)); // This extension method is available in Simple.Kafka.Rpc package (required to call so client can match request to response)

        Console.WriteLine($"[Server] Successfully added response to producer's queue. Id: {rpcRequestIdParseResult.Ok}");
    }
}, stop.Token).ContinueWith(t => { });

```

The code above creates producer and consumer instances and starts consuming data from topic in the separate `Task`. Once we get something we should call `GetRpcRequestId` extension on a `Message<byte[], byte[]>` as we need to propagate it in the response `Message<byte[], byte[]>` so client can match request with a response :) Other than that - the code just consumes message and produces a response with new `Pong` message serialized using message pack (anything else can be used).

Let's add another chunk for stopping the program gracefully:

``` csharp

Console.WriteLine("Press <Enter> to stop program");

_ = Console.ReadLine();

stop.Cancel();

await Task.WhenAll(serverTask, clientTask);

```

That's it, don't forget to start local Kafka instance and create topics for `Pong`s and `Ping`s, try how it works, for example, logs that I've got:

```

Press <Enter> to stop program
[RPC] Consumer log[Info] Consumer thread started
[Server] Received request with id: 7c960eae-c909-4b4a-ac63-85c2721be25a
[RPC] Consumer partitions were assigned: pong [[0]]
[Server] Successfully added response to producer's queue. Id: 7c960eae-c909-4b4a-ac63-85c2721be25a
[Server] Received request with id: c959e180-41db-4499-99b6-08e1f719c3c1
[Server] Successfully added response to producer's queue. Id: c959e180-41db-4499-99b6-08e1f719c3c1
[RPC] Got an EOF. TP: pong [[0]]
[Server] Received request with id: 0ef741ce-6455-48b0-a339-03a5e7443683
[Server] Successfully added response to producer's queue. Id: 0ef741ce-6455-48b0-a339-03a5e7443683
[RPC] Consumer received a response. Header: 7c960eae-c909-4b4a-ac63-85c2721be25a. TPO: pong [[0]] @2. Value len: 11
[RPC] Consumer received a response. Header: c959e180-41db-4499-99b6-08e1f719c3c1. TPO: pong [[0]] @3. Value len: 11
[RPC] Consumer received a response. Header: 0ef741ce-6455-48b0-a339-03a5e7443683. TPO: pong [[0]] @4. Value len: 11
[RPC] Got an EOF. TP: pong [[0]]
Received pong response
[Server] Received request with id: 92177a6a-703c-482c-bc56-3d6dc6491e42
[Server] Successfully added response to producer's queue. Id: 92177a6a-703c-482c-bc56-3d6dc6491e42
[RPC] Consumer received a response. Header: 92177a6a-703c-482c-bc56-3d6dc6491e42. TPO: pong [[0]] @5. Value len: 11
Received pong response
[RPC] Got an EOF. TP: pong [[0]]

[RPC] Consumer log[Info] Consumer thread stopped

```

Feel free to modify it as you want, but what you have achieved is that two `microservice`-like tasks can talk to each other via Kafka like you probably do via HTTP. Awesome :D
