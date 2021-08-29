using System;

namespace Simple.Kafka.Rpc
{
    public sealed class RpcConfig
    {
        public string[] Topics { get; set; } = Array.Empty<string>();
        public bool StopConsumerOnUnhandledException { get; set; } = false;
        public TimeSpan ConsumerRecreationPause { get; set; } = TimeSpan.FromSeconds(10);
        public TimeSpan ProducerRecreationPause { get; set; } = TimeSpan.FromSeconds(10);
    }
}
