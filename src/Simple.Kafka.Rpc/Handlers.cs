using Confluent.Kafka;
using System;
using System.Collections.Generic;

namespace Simple.Kafka.Rpc
{
    public sealed class KafkaEventHandler
    {
        // Consumer:
        public Action<CommittedOffsets>? OnCommitted;
        public Action<List<TopicPartition>>? OnAssigned;
        public Action<List<TopicPartitionOffset>>? OnRevoked;

        // Shared:
        public Action<Error>? OnError;
        public Action<LogMessage>? OnLog;
        public Action<string>? OnStatistics;
    }

    public sealed class RpcEventHandler
    {
        // Consumer:
        public Action<ConsumeResult<byte[], byte[]>>? OnEof;
        public Action<Guid, ConsumeResult<byte[], byte[]>>? OnRpcMessage;

        // Shared:
        public Action<LogMessage>? OnRpcLog;
    }
}
