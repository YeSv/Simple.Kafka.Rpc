using Confluent.Kafka;
using System;
using System.Threading.Tasks;

using Simple.Dotnet.Utilities.Arc;
using System.Threading.Tasks.Dataflow;
using System.Threading;

using ProducerRent = Simple.Dotnet.Utilities.Arc.Arc<Confluent.Kafka.IProducer<byte[], byte[]>>.ArcRent<Confluent.Kafka.IProducer<byte[], byte[]>>;
using ConsumerRent = Simple.Dotnet.Utilities.Arc.Arc<Confluent.Kafka.IConsumer<byte[], byte[]>>.ArcRent<Confluent.Kafka.IConsumer<byte[], byte[]>>;
using System.Runtime.CompilerServices;

namespace Simple.Kafka.Rpc
{
    // Manages producer instances
    internal sealed class ProducerOwner : IDisposable
    {
        ProducerRent _rent;

        readonly RpcConfig _config;
        readonly RpcProducerBuilder _builder;
        readonly ActionBlock<object?> _refresher;
        readonly ActionBlock<HealthResult> _health;

        volatile Arc<IProducer<byte[], byte[]>> _producer;

        public ProducerOwner(RpcProducerBuilder builder, RpcConfig config)
        {
            _config = config;
            _builder = builder.WithKafkaEvents(e =>
            {
                var old = e.OnError;
                e.OnError = error =>
                {
                    if (error.IsFatal)
                    {
                        _health.Post(Rpc.Health.FatalErrorRecreatingProducer);
                        _refresher.Post(null);
                    }
                    old?.Invoke(error);
                };
            });

            _health = new(h => Health = h, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1 });

            _refresher = new(_ =>
            {
                try
                {
                    var oldRent = _rent;

                    _builder.RpcHandler.OnRpcLog?.Invoke(RecreatingMessage);

                    _producer = new(_builder.Build());
                    _rent = _producer.Rent();

                    oldRent.Dispose();

                    _health.Post(Rpc.Health.Healthy);
                    _builder.RpcHandler.OnRpcLog?.Invoke(RecreatedMessage);
                }
                catch (Exception ex)
                {
                    _refresher.Post(null);
                    _health.Post(Rpc.Health.FailedToRecreateProducer);
                    _builder.RpcHandler.OnRpcLog?.Invoke(new(nameof(RpcClient), SyslogLevel.Critical, string.Empty, $"Failed to recreate producer instance. Exception occurred: {ex}"));

                    Thread.Sleep(_config.ProducerRecreationPause);
                }
            }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1 });

            _producer = new(_builder.Build());
            _rent = _producer.Rent();
        }

        public HealthResult Health { get; private set; } = HealthResult.Healthy; 

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ProducerRent Rent() => _producer.Rent();

        public void Dispose()
        {
            _refresher?.Complete();
            _refresher?.Completion.Wait();

            _health?.Complete();
            _health?.Completion.Wait();

            _rent.Dispose();
        }

        static readonly LogMessage RecreatingMessage = new(nameof(RpcClient), SyslogLevel.Info, string.Empty, "Fatal error occurred, recreating producer instance");
        static readonly LogMessage RecreatedMessage = new(nameof(RpcClient), SyslogLevel.Info, string.Empty, "Fatal error occurred, successfully recreated producer instance");
    }

    // Manages consumer instances
    internal sealed class ConsumerOwner : IDisposable
    {
        Task _task; // Background consumer thread
        ConsumerRent _rent;
        CancellationTokenSource _source; // Required to stop the thread

        readonly RpcConfig _config;
        readonly RpcConsumerBuilder _builder;
        readonly ActionBlock<object?> _refresher; // Required to refresh consumer instances
        readonly ActionBlock<HealthResult> _health; // Updates current health

        volatile Arc<IConsumer<byte[], byte[]>> _consumer;

        public ConsumerOwner(RpcConsumerBuilder builder, RpcConfig config)
        {
            _config = config;
            _builder = builder.WithKafkaEvents(e =>
            {
                var old = e.OnError;
                e.OnError = error =>
                {
                    if (error.IsFatal)
                    {
                        _refresher?.Post(null);
                        _health?.Post(Rpc.Health.FatalErrorRecreatingConsumer);
                    }
                    old?.Invoke(error);
                };
            });

            _health = new (h => Health = h, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1 });

            _refresher = new(_ =>
            {
                try
                {
                    var oldRent = _rent;

                    _builder.RpcHandler.OnRpcLog?.Invoke(RecreatingMessage);

                    _source?.Cancel();
                    _task?.Wait();

                    _source = new();
                    _consumer = new(_builder.Build(), c => c.Close());
                    _rent = _consumer.Rent();

                    oldRent.Dispose();

                    _task = Task.Factory.StartNew(Handler, _source.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);

                    _health.Post(HealthResult.Healthy);
                    _builder.RpcHandler.OnRpcLog?.Invoke(RecreatedMessage);
                }
                catch (Exception ex)
                {
                    _refresher.Post(null);
                    _health.Post(Rpc.Health.FailedToRecreateConsumer);
                    _builder.RpcHandler.OnRpcLog?.Invoke(new(nameof(RpcClient), SyslogLevel.Critical, string.Empty, $"Failed to recreate consumer instance. Exception occurred: {ex}"));

                    Thread.Sleep(_config.ConsumerRecreationPause);
                }
            }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1 });

            _source = new();
            _consumer = new(_builder.Build(), c => c.Close());

            _task = Task.Factory.StartNew(Handler, _source.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        public HealthResult Health { get; private set; } = Rpc.Health.Healthy;

        public void Dispose()
        {
            _refresher?.Complete();
            _refresher?.Completion.Wait(); 
            
            _source?.Cancel();
            _task?.Wait();
            
            _health?.Complete();
            _health?.Completion.Wait();

            _rent.Dispose();
        }

        void Handler()
        {
            var token = _source.Token;
            using var consumer = _consumer.Rent();

            _builder.RpcHandler.OnRpcLog?.Invoke(ConsumerThreadStarted);
            try
            {
                while (!token.IsCancellationRequested)
                {
                    try
                    {
                        var result = consumer.Value!.Consume(token);
                        if (result.IsPartitionEOF)
                        {
                            _builder.RpcHandler.OnEof?.Invoke(result);
                            continue;
                        }

                        var requestId = result.Message.GetRpcRequestId();
                        if (requestId == null) continue;

                        var parseResult = requestId!.ParseRpcRequestId();
                        if (!parseResult.IsOk) _builder.RpcHandler.OnRpcLog?.Invoke(new(nameof(RpcClient), SyslogLevel.Info, string.Empty, $"Failed to parse {RpcHeaders.RpcRequestID} header. TPO: {result.TopicPartitionOffset}. Exception: {parseResult.Error!}"));

                        _builder.RpcHandler.OnRpcMessage?.Invoke(parseResult.Ok, result);
                    }
                    catch (ConsumeException ex)
                    {
                        _builder.KafkaHandler.OnError?.Invoke(ex.Error);
                    }
                    catch (Exception ex)
                    {
                        _builder.RpcHandler.OnRpcLog?.Invoke(new(nameof(RpcClient), SyslogLevel.Info, string.Empty, $"Unhandled exception occured in consumer thread: {ex}"));
                        if (_config.StopConsumerOnUnhandledException)
                        {
                            _health.Post(Rpc.Health.ConsumerStoppedDueToUnhandledException);
                            _builder.RpcHandler.OnRpcLog?.Invoke(ExceptionConsumerWontBeRecreated);
                            return;
                        }
                    }
                }
            }
            catch (OperationCanceledException) { }
            _builder.RpcHandler.OnRpcLog?.Invoke(ConsumerThreadStopped);
        }

        static readonly LogMessage ConsumerThreadStopped = new(nameof(RpcClient), SyslogLevel.Info, string.Empty, "Consumer thread stopped");
        static readonly LogMessage ConsumerThreadStarted = new (nameof(RpcClient), SyslogLevel.Info, string.Empty, "Consumer thread started");
        static readonly LogMessage RecreatingMessage = new(nameof(RpcClient), SyslogLevel.Info, string.Empty, "Fatal error occurred, recreating consumer instance");
        static readonly LogMessage RecreatedMessage = new(nameof(RpcClient), SyslogLevel.Info, string.Empty, "Fatal error occurred, successfully recreated consumer instance");
        static readonly LogMessage ExceptionConsumerWontBeRecreated = new(nameof(RpcClient), SyslogLevel.Critical, string.Empty, $"Consumer won't be recreated because {nameof(RpcConfig.StopConsumerOnUnhandledException)} is true");
    }
}
