using Confluent.Kafka;
using MessagePack;
using Simple.Dotnet.Utilities.Results;
using System;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace Simple.Kafka.Rpc.IntegrationTests
{
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

    public sealed class PingServer : IDisposable
    {
        readonly Task _listener;
        readonly Environment _env;
        readonly IConsumer<byte[], byte[]> _consumer;
        readonly CancellationTokenSource _stop = new();

        public PingServer(Environment env, Action<ConsumerConfig> @override = null)
        {
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = Guid.NewGuid().ToString(),
                EnableAutoCommit = false,
                EnablePartitionEof = true,
                AutoOffsetReset = AutoOffsetReset.Latest
            };

            @override?.Invoke(consumerConfig);

            _env = env;
            _consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build();

            _consumer.Subscribe(Ping.Topic);

            _listener = Task.Factory.StartNew(() =>
            {
                try
                {
                    while (!_stop.Token.IsCancellationRequested)
                    {
                        try
                        {
                            var result = _consumer.Consume(_stop.Token);
                            if (result == null || result.IsPartitionEOF) continue;

                            var requestId = result.Message.GetRpcRequestId();
                            if (requestId == null) continue;

                            var parseResult = requestId.ParseRpcRequestId();
                            if (!parseResult.IsOk)
                            {
                                _env.Output!.WriteLine($"Failed to parse RPC RequestId. TPO: {result.TopicPartitionOffset}. Error: {parseResult.Error!}");
                                continue;
                            }

                            _env.Kafka!.Producer.Produce(Pong.Topic, new Message<byte[], byte[]>
                            {
                                Key = Array.Empty<byte>(),
                                Value = MessagePackSerializer.Serialize(Pong.New)
                            }.WithRpcRequestId(requestId));

                            _env.Output!.WriteLine($"Sent a response for: {parseResult.Ok}. TPO: {result.TopicPartitionOffset}");
                        }
                        catch (ConsumeException ex) when (!ex.Error.IsFatal)
                        {
                            _env.Output!.WriteLine($"Consumer non-fatal error occurred: {ex}");
                        }
                    }
                }
                catch (OperationCanceledException) { }
                catch (Exception ex)
                {
                    _env.Output!.WriteLine($"Consumer unhandled error occurred: {ex}");
                }
                
            }, _stop.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        public void Dispose()
        {
            _stop.Cancel();
            _listener?.Wait();
            _consumer?.Close();
            _consumer?.Dispose();
        }
    }

    public static class PingPongRpcExtensions
    {
        public static async Task<Result<Pong, Exception>> Ping(this IKafkaRpc rpc, CancellationToken token = default)
        {
            try
            {
                var sendResult = await rpc.Send(new Message<byte[], byte[]>
                {
                    Key = Array.Empty<byte>(),
                    Value = MessagePackSerializer.Serialize(IntegrationTests.Ping.New)
                }, IntegrationTests.Ping.Topic, token);

                if (!sendResult.IsOk) return Result.Error<Pong, Exception>(sendResult.Error!);

                return Result.Ok<Pong, Exception>(MessagePackSerializer.Deserialize<Pong>(sendResult.Ok!.Value));
            }
            catch (Exception ex)
            {
                return Result.Error<Pong, Exception>(ex);
            }
        }
    }
}
