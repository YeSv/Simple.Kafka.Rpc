using Confluent.Kafka;
using System;

namespace Simple.Kafka.Rpc
{
    public sealed class RpcConsumerBuilder
    {
        internal RpcConsumerBuilder(RpcBuilder builder)
        {
            Rpc = builder;
            Kafka = new ConsumerConfig
            {
                EnableAutoCommit = false,
                EnablePartitionEof = true,
                EnableAutoOffsetStore = true,
                AutoOffsetReset = AutoOffsetReset.Latest,
                GroupId = $"RpcClient_{Guid.NewGuid()}"
            };
            RpcHandler = new RpcEventHandler();
            KafkaHandler = new KafkaEventHandler();
        }

        public RpcBuilder Rpc { get; }
        public ConsumerConfig Kafka { get; }
        public RpcEventHandler RpcHandler { get; }
        public KafkaEventHandler KafkaHandler { get; }

        public RpcConsumerBuilder WithConfig(Action<ConsumerConfig> @override)
        {
            @override(Kafka);
            return this;
        }

        public RpcConsumerBuilder WithKafkaEvents(Action<KafkaEventHandler> @override)
        {
            @override(KafkaHandler);
            return this;
        }

        public RpcConsumerBuilder WithRpcEvents(Action<RpcEventHandler> @override)
        {
            @override(RpcHandler);
            return this;
        }

        public IConsumer<byte[], byte[]> Build()
        {
            var builder = new ConsumerBuilder<byte[], byte[]>(Kafka);

            if (KafkaHandler.OnAssigned != null) builder.SetPartitionsAssignedHandler((c, p) => KafkaHandler.OnAssigned(p));
            if (KafkaHandler.OnRevoked != null) builder.SetPartitionsRevokedHandler((c, p) => KafkaHandler.OnRevoked(p));
            if (KafkaHandler.OnCommitted != null) builder.SetOffsetsCommittedHandler((c, o) => KafkaHandler.OnCommitted(o));
            if (KafkaHandler.OnError != null) builder.SetErrorHandler((c, e) => KafkaHandler.OnError(e));
            if (KafkaHandler.OnLog != null) builder.SetLogHandler((c, l) => KafkaHandler.OnLog(l));
            if (KafkaHandler.OnStatistics != null) builder.SetStatisticsHandler((c, s) => KafkaHandler.OnStatistics(s));

            return builder.Build();
        }
    }

    public sealed class RpcProducerBuilder 
    {
        internal RpcProducerBuilder(RpcBuilder builder)
        {
            Rpc = builder;
            RpcHandler = new();
            KafkaHandler = new();

            Kafka = new ProducerConfig
            {
                EnableIdempotence = true
            };
        }

        public RpcBuilder Rpc { get; }
        public ProducerConfig Kafka { get; }
        public RpcEventHandler RpcHandler { get; }
        public KafkaEventHandler KafkaHandler { get; }

        public RpcProducerBuilder WithConfig(Action<ProducerConfig> @override)
        {
            @override(Kafka);
            return this;
        }

        public RpcProducerBuilder WithKafkaEvents(Action<KafkaEventHandler> @override)
        {
            @override(KafkaHandler);
            return this;
        }

        public RpcProducerBuilder WithRpcEvents(Action<RpcEventHandler> @override)
        {
            @override(RpcHandler);
            return this;
        }

        public IProducer<byte[], byte[]> Build()
        {
            var builder = new ProducerBuilder<byte[], byte[]>(Kafka);
            
            if (KafkaHandler.OnLog != null) builder.SetLogHandler((p, l) => KafkaHandler.OnLog(l));
            if (KafkaHandler.OnError != null) builder.SetErrorHandler((p, e) => KafkaHandler.OnError(e));
            if (KafkaHandler.OnStatistics != null) builder.SetStatisticsHandler((p, s) => KafkaHandler.OnStatistics(s));

            return builder.Build();
        }
    }

    public sealed class RpcBuilder
    {
        public RpcBuilder()
        {
            Config = new RpcConfig();
            Consumer = new RpcConsumerBuilder(this);
            Producer = new RpcProducerBuilder(this);
        }

        public RpcConfig Config { get; }
        public RpcConsumerBuilder Consumer { get; }
        public RpcProducerBuilder Producer { get; }

        public RpcBuilder WithConfig(Action<RpcConfig> @override)
        {
            @override(Config);
            return this;
        }

        public RpcClient Build() => new (this);
    }
}
