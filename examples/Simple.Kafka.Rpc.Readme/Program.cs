using Confluent.Kafka;
using MessagePack;
using Simple.Kafka.Rpc;
using System;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;


/*var response = await client.SendAsync(new HttpRequestMessage(HttpMethod.Get, "http://google.com"));
var content = await response.Content.ReadAsByteArrayAsync();

Console.WriteLine($"Content.Length is {content.Length}");*/


/*// The code below is just for demonstration purposes

using var rpc = RpcClient.Create(o => o.Config.Topics = new[] { "responses-topic" });

var consumeResult = await rpc.SendAsync(new Message<byte[], byte[]>
{
    Key = Array.Empty<byte>(),
    Value = Array.Empty<byte>()
}, "requests-topic");

var content = consumeResult.Message.Value;
Console.WriteLine($"Content.Length is {content.Length}");*/


/*using var rpc_health = RpcClient.Create(b => { });
var health = rpc_health.Health;
Console.WriteLine(health.IsHealthy);
Console.WriteLine(health.Reason);*/

/*var builder = new RpcBuilder();
builder.WithConfig(c =>
{
    c.Topics = new[] { Ping.Topic };
    c.RequestTimeout = TimeSpan.FromSeconds(1);
});

// Setting `Acks` property for producer and compression
builder.Producer.Kafka.Acks = Acks.All;
builder.Producer.Kafka.CompressionType = CompressionType.Lz4;

// OR

builder.Producer.WithConfig(c =>
{
    c.Acks = Acks.All;
    c.CompressionType = CompressionType.Lz4;
});

builder.Producer.KafkaHandler.OnError = (p, e) => Console.WriteLine($"Error occurred: {e.Reason}");
builder.Producer.KafkaHandler.OnStatistics = (p, statistics) => Console.WriteLine($"Producer statistics: {statistics}");
builder.Producer.KafkaHandler.OnErrorRestart = e => e.IsFatal;

// OR

builder.Producer.WithKafkaEvents(h =>
{
    h.OnError = (p, e) => Console.WriteLine($"Error occurred: {e.Reason}");
    h.OnStatistics = (p, statistics) => Console.WriteLine($"Producer statistics: {statistics}");
    h.OnErrorRestart = e => e.IsFatal;
});


builder.Producer.RpcHandler.OnRpcLog = m => Console.WriteLine($"[{m.Level}] {m.Message}");

// OR

builder.Producer.WithRpcEvents(h =>
{
    h.OnRpcLog = m => Console.WriteLine($"[{m.Level}] {m.Message}");
});


using var client = RpcClient.Create(builder =>
{
    builder.Config.RequestTimeout = TimeSpan.FromSeconds(1);

    builder.Consumer.Kafka.BootstrapServers = "localhost:9092";

    builder.Producer.Kafka.Acks = Acks.All;
    builder.Producer.Kafka.CompressionType = CompressionType.Lz4;
    builder.Producer.Kafka.BootstrapServers = "localhost:9092";

    builder.Producer.KafkaHandler.OnError = (p, e) => Console.WriteLine($"Error occurred: {e.Reason}");
    builder.Producer.KafkaHandler.OnStatistics = (p, statistics) => Console.WriteLine($"Producer statistics: {statistics}");
    builder.Producer.KafkaHandler.OnErrorRestart = e => e.IsFatal;

    builder.Consumer.KafkaHandler.OnError = (p, e) => Console.WriteLine($"Error occurred: {e.Reason}");
    builder.Consumer.KafkaHandler.OnStatistics = (p, statistics) => Console.WriteLine($"Producer statistics: {statistics}");
    builder.Consumer.KafkaHandler.OnErrorRestart = e => e.IsFatal;
});

// OR

var client = new RpcBuilder()
    .WithConfig(c => c.RequestTimeout = TimeSpan.FromSeconds(1))
    .Producer
    .WithConfig(c =>
    {
        c.Acks = Acks.All;
        c.BootstrapServers = "localhost:9092";
        c.CompressionType = CompressionType.Lz4;
    })
    .WithKafkaEvents(h =>
    {
        h.OnError = (p, e) => Console.WriteLine($"Error occurred: {e.Reason}");
        h.OnStatistics = (p, statistics) => Console.WriteLine($"Producer statistics: {statistics}");
        h.OnErrorRestart = e => e.IsFatal;
    })
    .Rpc
    .Consumer
    .WithConfig(c =>
    {
        c.BootstrapServers = "localhost:9092";
    })
    .WithKafkaEvents(h =>
    {
        h.OnError = (p, e) => Console.WriteLine($"Error occurred: {e.Reason}");
        h.OnStatistics = (p, statistics) => Console.WriteLine($"Producer statistics: {statistics}");
        h.OnErrorRestart = e => e.IsFatal;
    })
    .Rpc
    .Build();*/

using var rpc = RpcClient.Create(b =>
{
    b.Config.Topics = new[] { Pong.Topic };

    // Here we specify logging for some useful events, you can skip it and those logs won't be printed 
    b.Consumer.RpcHandler.OnEof = consumeResult => Console.WriteLine($"[RPC] Got an EOF. TP: {consumeResult.TopicPartition}");
    b.Consumer.RpcHandler.OnRpcMessage = (header, result) => Console.WriteLine($"[RPC] Consumer received a response. Header: {header}. TPO: {result.TopicPartitionOffset}. Value len: {result.Message.Value.Length}");
    b.Consumer.RpcHandler.OnRpcLog = log => Console.WriteLine($"[RPC] Consumer log[{log.Level}] {log.Message}");

    // Consumer overrides:
    // All settings for consumer are available here: Consumer.Kafka (ConsumerConfig)
    b.Consumer.Kafka.BootstrapServers = "localhost:9092";

    // Consumer events: (all events are available here - like OnAssigned, OnRevoked, OnStatistics, etc.
    b.Consumer.KafkaHandler.OnAssigned = (c, assigned) => Console.WriteLine($"[RPC] Consumer partitions were assigned: {string.Join(",", assigned)}");

    // Producer config: (of type ProducerConfig)
    b.Producer.Kafka.BootstrapServers = "localhost:9092";

    // Kafka events like OnError, OnStatistics, etc
    b.Producer.KafkaHandler.OnError = (p, e) => Console.WriteLine($"[RPC] Producer error occurred: {e.Reason}");

    b.Producer.RpcHandler.OnRpcLog = log => Console.WriteLine($"[RPC] Producer log[{log.Level}] {log.Message}");
});

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

        var rpcRequest = consumeResult.Message.GetRpcRequestId(); // This extension method comes with Simple.Kafka.Rpc package (REQUIRED)
        if (rpcRequest == null) continue; // Not an Simple.Kafka.Rpc request message

        var rpcRequestIdParseResult = rpcRequest.ParseRpcRequestId(); // This extension method comes with Simple.Kafka.Rpc package (not required to use, here - just for logging)
        if (!rpcRequestIdParseResult.IsOk) continue; // Failed to parse

        Console.WriteLine($"[Server] Received request with id: {rpcRequestIdParseResult.Ok}");

        producer.Produce(Pong.Topic, new Message<byte[], byte[]>
        {
            Key = Array.Empty<byte>(),
            Value = MessagePackSerializer.Serialize(Pong.New)
        }.WithRpcRequestId(rpcRequest)); // This extension method comes with Simple.Kafka.Rpc package (required to call so client can match request to response)

        Console.WriteLine($"[Server] Successfully added response to producer's queue. Id: {rpcRequestIdParseResult.Ok}");
    }
}, stop.Token).ContinueWith(t => { });

Console.WriteLine("Press <Enter> to stop program");

_ = Console.ReadLine();

stop.Cancel();

await Task.WhenAll(serverTask, clientTask);


[DataContract]
public readonly struct Ping
{
    public static readonly string Topic = "ping";

    [DataMember(Order = 0)] public readonly DateTime Time;

    public Ping(DateTime time) => Time = time;

    public static Ping New => new(DateTime.UtcNow);
}

[DataContract]
public readonly struct Pong
{
    public static readonly string Topic = "pong";

    [DataMember(Order = 0)] public readonly DateTime Time;

    public Pong(DateTime time) => Time = time;

    public static Pong New => new(DateTime.UtcNow);
}


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

