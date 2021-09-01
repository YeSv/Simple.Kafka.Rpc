using Confluent.Kafka;
using Docker.DotNet;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using TestEnvironment.Docker;
using Xunit.Abstractions;

namespace Simple.Kafka.Rpc.IntegrationTests
{
    public sealed class Environment : IDisposable
    {
        public Environment(ITestOutputHelper output) 
        {
            Output = output;
            Kafka = new (new DockerEnvironmentBuilder().DockerClient, output);
        }

        public ITestOutputHelper Output { get; }

        public KafkaContainer Kafka { get; }

        public void Dispose() => Kafka.Dispose();
    }

    public sealed class KafkaContainer : Container
    {
        public KafkaContainer(DockerClient client, ITestOutputHelper output) : base(
            client,
            "kafka-rpc",
            "johnnypark/kafka-zookeeper",
            environmentVariables: new Dictionary<string, string>
            {
                ["ADVERTISED_HOST"] = "localhost"
            },
            ports: new Dictionary<ushort, ushort>
            {
                [9092] = 9092,
                [2181] = 2181
            },
            containerWaiter: new KafkaWaiter(output))
        {
            Producer = new ProducerBuilder<byte[], byte[]>(new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                MessageTimeoutMs = 5000,
                MessageSendMaxRetries = int.MaxValue
            }).Build();
        }

        public IProducer<byte[], byte[]> Producer { get; }

        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (disposing) Producer?.Dispose();
        }

        public sealed class KafkaWaiter : IContainerWaiter<KafkaContainer>
        {
            readonly ITestOutputHelper _output;

            public KafkaWaiter(ITestOutputHelper output) => _output = output;

            public async Task<bool> Wait(KafkaContainer container, CancellationToken cancellationToken)
            {
                try
                {
                    await container.Producer.ProduceAsync("waiter-healthcheck", new Message<byte[], byte[]>
                    {
                        Key = Array.Empty<byte>(),
                        Value = Array.Empty<byte>()
                    });
                    return true;
                }
                catch (ProduceException<byte[], byte[]> ex)
                {
                    _output.WriteLine($"Produce exception occurred. Exception: {ex}");
                    return false;
                }
            }

            public Task<bool> Wait(Container container, CancellationToken cancellationToken) => Wait((KafkaContainer)container, cancellationToken);
        }
    }
}
