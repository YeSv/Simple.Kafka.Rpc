using Confluent.Kafka;
using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

using Simple.Dotnet.Utilities.Arc;
using System.Threading.Tasks.Dataflow;
using System.Threading;

using ProducerRent = Simple.Dotnet.Utilities.Arc.Arc<Confluent.Kafka.IProducer<byte[], byte[]>>.ArcRent;
using ConsumerRent = Simple.Dotnet.Utilities.Arc.Arc<Confluent.Kafka.IConsumer<byte[], byte[]>>.ArcRent;

namespace Simple.Kafka.Rpc
{
    // Manages producer instances
    internal sealed class ProducerOwner : IDisposable
    {
        ProducerRent _rent;

        readonly RpcConfig _config;
        readonly RpcProducerBuilder _builder;
        readonly ActionBlock<string> _refresher;
        readonly ActionBlock<(string Id, HealthResult Health)> _health;

        volatile Arc<IProducer<byte[], byte[]>> _producer;

        // TODO: Completely rewrite state updates
        public ProducerOwner(RpcProducerBuilder builder, RpcConfig config)
        {
            _config = config;
            _builder = builder.WithKafkaEvents(e =>
            {
                e.OnErrorRestart ??= error => error.IsFatal;

                var old = e.OnError;
                e.OnError = (p, error) =>
                {
                    if (e.OnErrorRestart!(error))
                    {
                        _health.Post((p.Name, Rpc.Health.FatalErrorRecreatingProducer));
                        _refresher.Post(p.Name);
                    }
                    old?.Invoke(p, error);
                };
            });

            _health = new(h => 
            {
                using var rent = _producer.Rent();
                if (rent.Value!.Name != h.Id) return; // Only updates for the same producer allowed

                Health = h.Health;
            }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1 });

            _refresher = new(id =>
            {
                try
                {
                    var oldRent = _rent;
                    if (oldRent.Value!.Name != id) return; // Consumer was changed, ignore

                    _builder.RpcHandler.OnRpcLog?.Invoke(RecreatingMessage);

                    _producer = new(_builder.Build());
                    _rent = _producer.Rent();

                    oldRent.Dispose();

                    _health.Post((_rent.Value!.Name, Rpc.Health.Healthy));
                    _builder.RpcHandler.OnRpcLog?.Invoke(RecreatedMessage);
                }
                catch (Exception ex)
                {
                    _refresher.Post(id);
                    _health.Post((id, Rpc.Health.FailedToRecreateProducer));
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
        readonly ActionBlock<string> _refresher; // Required to refresh consumer instances
        readonly ActionBlock<(string Id, HealthResult Health)> _health; // Updates current health

        volatile Arc<IConsumer<byte[], byte[]>> _consumer;

        // TODO: Completely rewrite state updates
        public ConsumerOwner(RpcConsumerBuilder builder, RpcConfig config)
        {
            _config = config;
            _builder = builder.WithKafkaEvents(e =>
            {
                e.OnErrorRestart ??= error => error.IsFatal;

                var oldError = e.OnError;
                e.OnError = (c, error) =>
                {
                    if (e.OnErrorRestart(error))
                    {
                        _refresher?.Post(c.Name);
                        _health?.Post((c.Name, Rpc.Health.FatalErrorRecreatingConsumer));
                    }
                    oldError?.Invoke(c, error);
                };

                var oldAssigned = e.OnAssigned;
                e.OnAssigned = (c, p) =>
                {
                    if (p.Count == 0) _health?.Post((c.Name, Rpc.Health.ConsumerAssignedToZeroPartitions));
                    else if (Health.Reason == Rpc.Health.ConsumerAssignedToZeroPartitions.Reason) _health?.Post((c.Name, Rpc.Health.Healthy));
                    
                    oldAssigned?.Invoke(c, p);
                };
            });

            _health = new (h => 
            {
                using var rent = _consumer?.Rent();
                if (rent?.Value!.Name != h.Id) return; // Only health updates for current consumer are allowed

                Health = h.Health;
            }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1 });

            _refresher = new(id =>
            {
                try
                {
                    var oldRent = _rent;

                    using var rent = _consumer.Rent();
                    if (rent.Value!.Name != id) return; // Consumer was changed already

                    _builder.RpcHandler.OnRpcLog?.Invoke(RecreatingMessage);

                    _source?.Cancel();
                    _task?.Wait();

                    _source = new();
                    _consumer = new(_builder.Build(), c => c.Close());
                    _rent = _consumer.Rent();

                    oldRent.Dispose();

                    _task = Task.Factory.StartNew(Handler, _source.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);

                    _health.Post((_rent.Value!.Name, HealthResult.Healthy));
                    _builder.RpcHandler.OnRpcLog?.Invoke(RecreatedMessage);
                }
                catch (Exception ex)
                {
                    _refresher.Post(id);
                    _health.Post((id, Rpc.Health.FailedToRecreateConsumer));
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
                consumer.Value!.Subscribe(_config.Topics);

                while (!token.IsCancellationRequested)
                {
                    try
                    {
                        var result = consumer.Value!.Consume(token);

                        if (result == null) continue;
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
                        _builder.KafkaHandler.OnError?.Invoke(consumer.Value!, ex.Error);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        _builder.RpcHandler.OnRpcLog?.Invoke(new(nameof(RpcClient), SyslogLevel.Info, string.Empty, $"Unhandled exception occured in consumer thread: {ex}"));
                        if (_config.StopConsumerOnUnhandledException)
                        {
                            _health.Post((consumer.Value!.Name, Rpc.Health.ConsumerStoppedDueToUnhandledException));
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
