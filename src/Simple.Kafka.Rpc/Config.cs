using System;

namespace Simple.Kafka.Rpc
{
    public sealed class RpcConfig
    {
        public string[]? Topics { get; set; } = Array.Empty<string>();
        public bool StopConsumerOnUnhandledException { get; set; }
        public bool UnhealthyIfNoPartitionsAssigned { get; set; }
        public bool EnableBrokerAvailabilityHealthCheck { get; set; }
        public TimeSpan? RequestTimeout { get; set; }
        public TimeSpan ConsumerWarmupDuration { get; set; } = TimeSpan.FromSeconds(10);
        public TimeSpan ConsumerRecreationPause { get; set; } = TimeSpan.FromSeconds(10);
        public TimeSpan ProducerRecreationPause { get; set; } = TimeSpan.FromSeconds(10);
    }
}
