using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Simple.Dotnet.Utilities.Results;
using Simple.Dotnet.Utilities.Tasks;

namespace Simple.Kafka.Rpc
{
    public interface IKafkaRpc : IDisposable
    {
        HealthResult Health { get; }

        Task<UniResult<ConsumeResult<byte[], byte[]>, RpcException>> Send(byte[] key, byte[] value, string topic, CancellationToken token = default);
        Task<UniResult<ConsumeResult<byte[], byte[]>, RpcException>> Send(Message<byte[], byte[]> message, string topic, CancellationToken token = default);
    }

    // More idiomatic dotnet implementation
    public static class KafkaRpcExtensions
    {
        public static Task<ConsumeResult<byte[], byte[]>> SendAsync(this IKafkaRpc rpc, byte[] key, byte[] value, string topic, CancellationToken token = default) =>
            SendAsync(rpc, new Message<byte[], byte[]>
            {
                Key = key,
                Value = value
            }, topic, token);

        public static async Task<ConsumeResult<byte[], byte[]>> SendAsync(this IKafkaRpc rpc, Message<byte[], byte[]> message, string topic, CancellationToken token = default)
        {
            var sendResult = await rpc.Send(message, topic, token);
            return sendResult.IsOk switch
            {
                true => sendResult.Ok!,
                false => throw sendResult.Error!
            };
        }
    }

    public sealed class RpcClient : IKafkaRpc
    {
        readonly RpcConfig _config;
        readonly ProducerOwner _producer;
        readonly ConsumerOwner _consumer;
        readonly TaskBufferPool _taskBuffers;
        readonly Responses<ConsumeResult<byte[], byte[]>> _responses;

        internal RpcClient(RpcBuilder builder)
        {
            _config = builder.Config;
            builder.Consumer.WithRpcEvents(e =>
            {
                var old = e.OnRpcMessage;
                e.OnRpcMessage = (s, r) =>
                {
                    _responses?.Complete(s, r);
                    old?.Invoke(s, r);
                };
            });

            _responses = new ();
            _producer = new (builder.Producer, builder.Config);
            _consumer = new (builder.Consumer, builder.Config);
            _taskBuffers = new(2, Environment.ProcessorCount * 4);
        }

        public HealthResult Health => new (
            _producer.Health.IsHealthy && _consumer.Health.IsHealthy,
            (_producer.Health.IsHealthy, _consumer.Health.IsHealthy) switch
            {
                (true, true) => null,
                (true, false) => $"[Producer] Healthy{Environment.NewLine}[Consumer]: {_consumer.Health.Reason}",
                (false, true) => $"[Producer]: {_producer.Health.Reason}{Environment.NewLine}[Consumer]: Healthy",
                _ => $"[Producer]: {_producer.Health.Reason}{Environment.NewLine}[Consumer]: {_consumer.Health.Reason}"
            });

        public Task<UniResult<ConsumeResult<byte[], byte[]>, RpcException>> Send(byte[] key, byte[] value, string topic, CancellationToken token = default) =>
            Send(new Message<byte[], byte[]>
            {
                Key = key,
                Value = value
            }, topic, token);

        public async Task<UniResult<ConsumeResult<byte[], byte[]>, RpcException>> Send(Message<byte[], byte[]> message, string topic, CancellationToken token = default)
        {
            var subscription = Guid.NewGuid();
            var timeout = !token.CanBeCanceled && _config.RequestTimeout.HasValue ? new CancellationTokenSource(_config.RequestTimeout.Value) : null;
            try
            {
                message.WithRpcRequestId(subscription.ToByteArray());

                var subscriptionTask = _responses.Subscribe(subscription);

                using var producerRent = _producer.Rent();
                var produceResult = await producerRent.Value!.ProduceAsync(topic, message).ConfigureAwait(false);

                using var taskBufferRent = _taskBuffers.Get();
                taskBufferRent.Value.Append(subscriptionTask);
                taskBufferRent.Value.Append(Task.Delay(Timeout.InfiniteTimeSpan, timeout?.Token ?? token));

                var resultTask = await Task.WhenAny((Task[])taskBufferRent.Value).ConfigureAwait(false);
                if (resultTask != subscriptionTask)
                {
                    _responses.Unsubscribe(subscription);
                    return UniResult.Error<ConsumeResult<byte[], byte[]>, RpcException>(RpcException.Timeout(subscription));
                }

                return UniResult.Ok<ConsumeResult<byte[], byte[]>, RpcException>(subscriptionTask.Result);
            }
            catch (Exception ex)
            {
                _responses.Unsubscribe(subscription);
                return UniResult.Error<ConsumeResult<byte[], byte[]>, RpcException>(ex switch 
                {
                    ProduceException<byte[], byte[]> e => RpcException.Kafka(subscription, ex),
                    OperationCanceledException e => RpcException.Timeout(subscription),
                    _ => RpcException.Unhandled(subscription, ex)
                });
            }
            finally
            {
                timeout?.Dispose();
            }
        }

        public void Dispose()
        {
            _producer?.Dispose();
            _consumer?.Dispose();
            _responses?.Dispose();
        }

        public static RpcClient Create(Action<RpcBuilder> @override)
        {
            var builder = new RpcBuilder();
            @override(builder);
            return builder.Build();
        }
    }
}
