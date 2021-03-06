using Confluent.Kafka;
using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

using Simple.Dotnet.Utilities.Arc;
using System.Threading.Tasks.Dataflow;
using System.Threading;

using KafkaEnumerable;

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
        readonly ActionBlock<IStateChangeCommand> _stateChanger;

        volatile HealthResult _health;
        volatile Arc<IProducer<byte[], byte[]>> _producer;

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
                        _stateChanger?.Post(new ChangeHealthCommand(p.Name, Rpc.Health.ProducerFatalError));
                        _stateChanger?.Post(new RecreateCommand(p.Name));
                    }
                    old?.Invoke(p, error);
                };

                var oldStatistics = e.OnStatistics;
                e.OnStatistics = _config.EnableBrokerAvailabilityHealthCheck ? (c, s) =>
                {
                    var parsed = RpcKafkaStatistics.Parse(s);
                    if (!parsed.IsOk) _builder?.RpcHandler.OnRpcLog?.Invoke(new LogMessage(nameof(RpcClient), SyslogLevel.Error, string.Empty, $"Failed to parse kafka statistics. Exception: {parsed.Error}"));
                    else
                    {
                        var (unavailableBrokers, allBrokers) = parsed.Ok!.GetBrokerStats();
                        _stateChanger?.Post(new ChangeBrokersNumberHealthCommand(c.Name, unavailableBrokers, allBrokers));
                    }

                    oldStatistics?.Invoke(c, s);
                } : oldStatistics;
            }).WithConfig(c =>
            {
                if (_config.RequestTimeout.HasValue && _config.RequestTimeout.Value != default) c.MessageTimeoutMs = (int)_config.RequestTimeout.Value.TotalMilliseconds;
                if (_config.EnableBrokerAvailabilityHealthCheck && !c.StatisticsIntervalMs.HasValue) c.StatisticsIntervalMs = (int)_config.ProducerRecreationPause.TotalMilliseconds;
            });

            _health = Rpc.Health.Healthy;

            _producer = new(_builder.Build());
            _rent = _producer.Rent();

            _stateChanger = new(cmd =>
            {
                switch (cmd)
                {
                    case ChangeHealthCommand c: SetHealth(c.Id, c.Change); break;
                    case RecreateCommand r: Recreate(r.Id); break;
                    case ChangeBrokersNumberHealthCommand b: BrokersNumChanged(b.UnavailableBrokers, b.TotalBrokers); break;
                }
            }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1 });
        }

        public HealthResult Health => _health;

        internal void Recreate(string senderId)
        {
            try
            {
                var oldRent = _rent;
                if (oldRent.Value!.Name != senderId) return; // Consumer was changed, ignore

                _builder.RpcHandler.OnRpcLog?.Invoke(RecreatingMessage);

                _producer = new(_builder.Build());
                _rent = _producer.Rent();

                oldRent.Dispose();

                _builder.RpcHandler.OnRpcLog?.Invoke(RecreatedMessage);
                _health = Rpc.Health.Healthy;
            }
            catch (Exception ex)
            {
                _builder.RpcHandler.OnRpcLog?.Invoke(new(nameof(RpcClient), SyslogLevel.Critical, string.Empty, $"Failed to recreate producer instance. Exception occurred: {ex}"));
                _stateChanger.Post(new ChangeHealthCommand(senderId, Rpc.Health.FailedToRecreateProducer));
                Task.Delay(_config.ProducerRecreationPause).ContinueWith(t => _stateChanger?.Post(new RecreateCommand(senderId)));
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetHealth(string senderId, HealthResult health)
        {
            if (_rent.Value!.Name != senderId) return; // Do nothing 
            _health = health;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void BrokersNumChanged(int unavailable, int total)
        {
            if (unavailable == total) _health = Rpc.Health.AllBrokersUnavailable;
            if (unavailable < total && _health == Rpc.Health.AllBrokersUnavailable) _health = Rpc.Health.Healthy;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ProducerRent Rent() => _producer.Rent();

        public void Dispose()
        {
            _stateChanger?.Complete();
            _stateChanger?.Completion.Wait();
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
        readonly ActionBlock<IStateChangeCommand> _stateChanger;

        volatile HealthResult _health;
        volatile Arc<IConsumer<byte[], byte[]>> _consumer;

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
                        _stateChanger?.Post(new ChangeHealthCommand(c.Name, Rpc.Health.ConsumerFatalError));
                        _stateChanger?.Post(new RecreateCommand(c.Name));
                    }
                    oldError?.Invoke(c, error);
                };

                var oldAssigned = e.OnAssigned;
                e.OnAssigned = (c, p) =>
                {
                    if (_config.UnhealthyIfNoPartitionsAssigned) _stateChanger?.Post(new ChangePartitionsNumberHealthCommand(c.Name, p.Count));
                    oldAssigned?.Invoke(c, p);
                };

                var oldStatistics = e.OnStatistics;
                e.OnStatistics = _config.EnableBrokerAvailabilityHealthCheck ? (c, s) =>
                {
                    var parsed = RpcKafkaStatistics.Parse(s);
                    if (!parsed.IsOk) _builder?.RpcHandler.OnRpcLog?.Invoke(new LogMessage(nameof(RpcClient), SyslogLevel.Error, string.Empty, $"Failed to parse kafka statistics. Exception: {parsed.Error}"));
                    else
                    {
                        var (unavailableBrokers, allBrokers) = parsed.Ok!.GetBrokerStats();
                        _stateChanger?.Post(new ChangeBrokersNumberHealthCommand(c.Name, unavailableBrokers, allBrokers));
                    }

                    oldStatistics?.Invoke(c, s);
                } : oldStatistics;
            }).WithConfig(c => 
            {
                c.EnablePartitionEof = true;
                if (_config.EnableBrokerAvailabilityHealthCheck && !c.StatisticsIntervalMs.HasValue) c.StatisticsIntervalMs = (int)_config.ConsumerRecreationPause.TotalMilliseconds;
            });

            _source = new();
            _consumer = new(_builder.Build(), c => c.Close());
            _rent = _consumer.Rent();

            _stateChanger = new(cmd =>
            {
                if (_rent.Value?.Name != cmd.Id) return;

                switch (cmd)
                {
                    case RecreateCommand r: Recreate(r.Id); break;
                    case ChangeHealthCommand c: _health = c.Change; break;
                    case ChangePartitionsNumberHealthCommand p: PartitionsNumChanged(p.Assigned); break;
                    case ChangeBrokersNumberHealthCommand b: BrokersNumChanged(b.UnavailableBrokers, b.TotalBrokers); break;
                }
            }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1 });

            if (Warmup()) _health = Rpc.Health.Healthy;

            _task = Task.Factory.StartNew(Handler, _source.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        public HealthResult Health => _health;

        internal void Recreate(string senderId)
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

                if (Warmup()) _health = Rpc.Health.Healthy;

                _task = Task.Factory.StartNew(Handler, _source.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);

                _builder.RpcHandler.OnRpcLog?.Invoke(RecreatedMessage);
            }
            catch (Exception ex)
            {
                _builder.RpcHandler.OnRpcLog?.Invoke(new(nameof(RpcClient), SyslogLevel.Critical, string.Empty, $"Failed to recreate consumer instance. Exception occurred: {ex}"));
                _stateChanger.Post(new ChangeHealthCommand(senderId, Rpc.Health.FailedToRecreateConsumer));
                Task.Delay(_config.ConsumerRecreationPause).ContinueWith(t => _stateChanger?.Post(new RecreateCommand(senderId)));
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void PartitionsNumChanged(int assigned)
        {
            if (assigned == 0 && _config.UnhealthyIfNoPartitionsAssigned) _health = Rpc.Health.ConsumerAssignedToZeroPartitions;
            if (assigned != 0 && _health == Rpc.Health.ConsumerAssignedToZeroPartitions) _health = Rpc.Health.Healthy;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void BrokersNumChanged(int unavailable, int total)
        {
            if (unavailable == total) _health = Rpc.Health.AllBrokersUnavailable;
            if (unavailable < total && _health == Rpc.Health.AllBrokersUnavailable) _health = Rpc.Health.Healthy;
        }

        public void Dispose()
        {
            _stateChanger?.Complete();
            _stateChanger?.Completion.Wait(); 
            
            _source?.Cancel();
            _task?.Wait();

            _rent.Dispose();
        }

        void Handler()
        {
            var token = _source.Token;
            using var consumer = _consumer.Rent();

            _builder.RpcHandler.OnRpcLog?.Invoke(ConsumerThreadStarted);
            try
            {
                var stream = KafkaEnumerables.Single(consumer.Value!, returnNulls: false, cancellationToken: token);
                foreach (var message in stream)
                {
                    try
                    {
                        if (message.IsError)
                        {
                            _builder.KafkaHandler.OnError?.Invoke(consumer.Value!, message.Error!.Error);
                            continue;
                        }

                        var result = message.ConsumeResult!;
                        if (result.IsPartitionEOF)
                        {
                            _builder.RpcHandler.OnEof?.Invoke(result);
                            continue;
                        }

                        var requestId = result.Message.GetRpcRequestId();
                        if (requestId == null) continue;

                        var parseResult = requestId.ParseRpcRequestId();
                        if (!parseResult.IsOk) _builder.RpcHandler.OnRpcLog?.Invoke(new(nameof(RpcClient), SyslogLevel.Info, string.Empty, $"Failed to parse {RpcHeaders.RpcRequestID} header. TPO: {result.TopicPartitionOffset}. Exception: {parseResult.Error!}"));
                        else _builder.RpcHandler.OnRpcMessage?.Invoke(parseResult.Ok, result);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        _builder.RpcHandler.OnRpcLog?.Invoke(new(nameof(RpcClient), SyslogLevel.Info, string.Empty, $"Unhandled exception occurred in consumer thread: {ex}"));
                        if (_config.StopConsumerOnUnhandledException)
                        {
                            _stateChanger?.Post(new ChangeHealthCommand(consumer.Value!.Name, Rpc.Health.ConsumerStoppedDueToUnhandledException));
                            _builder.RpcHandler.OnRpcLog?.Invoke(ExceptionConsumerWontBeRecreated);
                            return;
                        }
                    }
                }
            }
            catch (OperationCanceledException) { }
            _builder.RpcHandler.OnRpcLog?.Invoke(ConsumerThreadStopped);
        }

        bool Warmup()
        {
            using var rent = _consumer.Rent();
            rent.Value!.Subscribe(_config.Topics ?? Array.Empty<string>());

            if (!_config.UnhealthyIfNoPartitionsAssigned) return true;
            
            using var warmupTimeout = new CancellationTokenSource(_config.ConsumerWarmupDuration);
            var token = warmupTimeout.Token;

            try
            {
                while (true)
                {
                    var consumeResult = rent.Value!.Consume(token);
                    if (consumeResult is { IsPartitionEOF: true }) return true;
                }
            }
            catch (OperationCanceledException)
            {
                _stateChanger.Post(new ChangeHealthCommand(rent.Value!.Name, Rpc.Health.ConsumerAssignedToZeroPartitions));
                return false;
            }
        }

        static readonly LogMessage ConsumerThreadStopped = new(nameof(RpcClient), SyslogLevel.Info, string.Empty, "Consumer thread stopped");
        static readonly LogMessage ConsumerThreadStarted = new (nameof(RpcClient), SyslogLevel.Info, string.Empty, "Consumer thread started");
        static readonly LogMessage RecreatingMessage = new(nameof(RpcClient), SyslogLevel.Info, string.Empty, "Fatal error occurred, recreating consumer instance");
        static readonly LogMessage RecreatedMessage = new(nameof(RpcClient), SyslogLevel.Info, string.Empty, "Fatal error occurred, successfully recreated consumer instance");
        static readonly LogMessage ExceptionConsumerWontBeRecreated = new(nameof(RpcClient), SyslogLevel.Critical, string.Empty, $"Consumer won't be recreated because {nameof(RpcConfig.StopConsumerOnUnhandledException)} is true");
    }

    internal interface IStateChangeCommand 
    {
        public string Id { get; }
    }

    internal sealed class RecreateCommand : IStateChangeCommand
    {
        public string Id { get; }

        public RecreateCommand(string id) => Id = id;
    }

    internal sealed class ChangeHealthCommand : IStateChangeCommand
    {
        public string Id { get; }
        public HealthResult Change { get; }

        public ChangeHealthCommand(string id, HealthResult change) => (Id, Change) = (id, change);
    }

    internal sealed class ChangePartitionsNumberHealthCommand : IStateChangeCommand
    {
        public string Id { get; }
        public int Assigned { get; }

        public ChangePartitionsNumberHealthCommand(string id, int assigned) => (Id, Assigned) = (id, assigned);
    }

    internal sealed class ChangeBrokersNumberHealthCommand : IStateChangeCommand
    {
        public string Id { get; }
        public int TotalBrokers { get; }
        public int UnavailableBrokers { get; }

        public ChangeBrokersNumberHealthCommand(string id, int unavailableBrokers, int totalBrokers) => (Id, TotalBrokers, UnavailableBrokers) = (id, totalBrokers, unavailableBrokers);
    }
}
