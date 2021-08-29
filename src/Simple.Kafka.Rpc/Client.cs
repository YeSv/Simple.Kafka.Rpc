using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Simple.Dotnet.Utilities.Results;
using Simple.Dotnet.Utilities.Tasks;

namespace Simple.Kafka.Rpc
{
    public interface IKafkaRpc
    {
        HealthResult Health { get; }

        public Task<UniResult<ConsumeResult<byte[], byte[]>, Exception>> Send(byte[] key, byte[] value, string topic, CancellationToken token = default);
        public Task<UniResult<ConsumeResult<byte[], byte[]>, Exception>> Send(Message<byte[], byte[]> message, string topic, CancellationToken token = default);
    }

    public sealed class RpcClient : IKafkaRpc, IDisposable
    {
        static readonly OperationCanceledException TimeoutException = new ("Timeout occurred during RPC send operation");

        readonly ProducerOwner _producer;
        readonly ConsumerOwner _consumer;
        readonly TaskBufferPool _taskBuffers;
        readonly Observable<ConsumeResult<byte[], byte[]>> _observable;

        internal RpcClient(RpcBuilder builder)
        {
            builder.Consumer.WithRpcEvents(e =>
            {
                var old = e.OnRpcMessage;
                e.OnRpcMessage = (s, r) =>
                {
                    _observable?.Complete(s, r);
                    old?.Invoke(s, r);
                };
            });

            _observable = new ();
            _producer = new (builder.Producer, builder.Config);
            _consumer = new (builder.Consumer, builder.Config);
            _taskBuffers = new(2, Environment.ProcessorCount * 4);
        }

        public HealthResult Health => new (
            _producer.Health.IsHealthy && _consumer.Health.IsHealthy,
            (_producer.Health.IsHealthy, _consumer.Health.IsHealthy) switch
            {
                (true, true) => null,
                (true, false) => _consumer.Health.Reason,
                (false, true) => _producer.Health.Reason,
                _ => $"{_producer.Health.Reason}{Environment.NewLine}{_consumer.Health.Reason}"
            });

        public Task<UniResult<ConsumeResult<byte[], byte[]>, Exception>> Send(byte[] key, byte[] value, string topic, CancellationToken token = default) =>
            Send(new Message<byte[], byte[]>
            {
                Key = key,
                Value = value
            }, topic, token);

        public async Task<UniResult<ConsumeResult<byte[], byte[]>, Exception>> Send(Message<byte[], byte[]> message, string topic, CancellationToken token = default)
        {
            var subscription = Guid.NewGuid();
            try
            {
                message.WithRpcRequestId(subscription.ToByteArray());

                var subscriptionTask = _observable.Subscribe(subscription);

                using var producerRent = _producer.Rent();
                var produceResult = await producerRent.Value!.ProduceAsync(topic, message).ConfigureAwait(false);

                using var taskBufferRent = _taskBuffers.Get();
                taskBufferRent.Value.Append(subscriptionTask);
                taskBufferRent.Value.Append(Task.Delay(Timeout.InfiniteTimeSpan, token));

                var resultTask = await Task.WhenAny((Task[])taskBufferRent.Value).ConfigureAwait(false);
                if (resultTask != subscriptionTask)
                {
                    _observable.Unsubscribe(subscription);
                    return UniResult.Error<ConsumeResult<byte[], byte[]>, Exception>(TimeoutException);
                }

                return UniResult.Ok<ConsumeResult<byte[], byte[]>, Exception>(subscriptionTask.Result);
            }
            catch (Exception ex)
            {
                _observable.Unsubscribe(subscription);
                return UniResult.Error<ConsumeResult<byte[], byte[]>, Exception>(ex);
            }
        }

        public void Dispose()
        {
            _producer?.Dispose();
            _consumer?.Dispose();
            _observable?.Dispose();
        }
    }
}
