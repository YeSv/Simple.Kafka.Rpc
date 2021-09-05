using System;

namespace Simple.Kafka.Rpc
{
    public sealed class RpcConfig
    {
        public string[] Topics { get; set; } = Array.Empty<string>();
        public bool StopConsumerOnUnhandledException { get; set; }
        public TimeSpan? RequestTimeout { get; set; }
        public TimeSpan ConsumerHealthCheckInterval { get; set; } = TimeSpan.FromSeconds(10);
        public TimeSpan ProducerHealthCheckInterval { get; set; } = TimeSpan.FromSeconds(10);
    }
}
