using Confluent.Kafka;
using System;
using System.Collections.Generic;

namespace Simple.Kafka.Rpc
{
    public sealed class KafkaConsumerEventHandler
    {
        public Func<Error, bool>? OnErrorRestart;
        public Action<IConsumer<byte[], byte[]>, Error>? OnError;
        public Action<IConsumer<byte[], byte[]>, LogMessage>? OnLog;
        public Action<IConsumer<byte[], byte[]>, string>? OnStatistics;
        public Action<IConsumer<byte[], byte[]>, CommittedOffsets>? OnCommitted;
        public Action<IConsumer<byte[], byte[]>, List<TopicPartition>>? OnAssigned;
        public Action<IConsumer<byte[], byte[]>, List<TopicPartitionOffset>>? OnRevoked;
    }

    public sealed class KafkaProducerEventHandler
    {
        public Func<Error, bool>? OnErrorRestart;
        public Action<IProducer<byte[], byte[]>, Error>? OnError;
        public Action<IProducer<byte[], byte[]>, LogMessage>? OnLog;
        public Action<IProducer<byte[], byte[]>, string>? OnStatistics;
    }

    public sealed class RpcConsumerEventHandler
    {
        public Action<LogMessage>? OnRpcLog;
        public Action<ConsumeResult<byte[], byte[]>>? OnEof;
        public Action<Guid, ConsumeResult<byte[], byte[]>>? OnRpcMessage;
    }

    public sealed class RpcProducerEventHandler
    {
        public Action<LogMessage>? OnRpcLog;
    }
}
