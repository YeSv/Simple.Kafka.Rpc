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
            RpcHandler = new ();
            KafkaHandler = new ();
        }

        public RpcBuilder Rpc { get; }
        public ConsumerConfig Kafka { get; }
        public RpcConsumerEventHandler RpcHandler { get; }
        public KafkaConsumerEventHandler KafkaHandler { get; }

        public RpcConsumerBuilder WithConfig(Action<ConsumerConfig> @override)
        {
            @override(Kafka);
            return this;
        }

        public RpcConsumerBuilder WithKafkaEvents(Action<KafkaConsumerEventHandler> @override)
        {
            @override(KafkaHandler);
            return this;
        }

        public RpcConsumerBuilder WithRpcEvents(Action<RpcConsumerEventHandler> @override)
        {
            @override(RpcHandler);
            return this;
        }

        public IConsumer<byte[], byte[]> Build()
        {
            var builder = new ConsumerBuilder<byte[], byte[]>(Kafka);

            if (KafkaHandler.OnAssigned != null) builder.SetPartitionsAssignedHandler(KafkaHandler.OnAssigned);
            if (KafkaHandler.OnRevoked != null) builder.SetPartitionsRevokedHandler(KafkaHandler.OnRevoked);
            if (KafkaHandler.OnCommitted != null) builder.SetOffsetsCommittedHandler(KafkaHandler.OnCommitted);
            if (KafkaHandler.OnError != null) builder.SetErrorHandler(KafkaHandler.OnError);
            if (KafkaHandler.OnLog != null) builder.SetLogHandler(KafkaHandler.OnLog);
            if (KafkaHandler.OnStatistics != null) builder.SetStatisticsHandler(KafkaHandler.OnStatistics);

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
        public RpcProducerEventHandler RpcHandler { get; }
        public KafkaProducerEventHandler KafkaHandler { get; }

        public RpcProducerBuilder WithConfig(Action<ProducerConfig> @override)
        {
            @override(Kafka);
            return this;
        }

        public RpcProducerBuilder WithKafkaEvents(Action<KafkaProducerEventHandler> @override)
        {
            @override(KafkaHandler);
            return this;
        }

        public RpcProducerBuilder WithRpcEvents(Action<RpcProducerEventHandler> @override)
        {
            @override(RpcHandler);
            return this;
        }

        public IProducer<byte[], byte[]> Build()
        {
            var builder = new ProducerBuilder<byte[], byte[]>(Kafka);
            
            if (KafkaHandler.OnLog != null) builder.SetLogHandler(KafkaHandler.OnLog);
            if (KafkaHandler.OnError != null) builder.SetErrorHandler(KafkaHandler.OnError);
            if (KafkaHandler.OnStatistics != null) builder.SetStatisticsHandler(KafkaHandler.OnStatistics);

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
