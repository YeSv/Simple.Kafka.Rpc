using Confluent.Kafka;
using Docker.DotNet;
using Microsoft.Extensions.Logging;
using Simple.Dotnet.Utilities.Tasks;
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
        public static Dictionary<string, string> EmptyEnvVariables = new();

        public ITestOutputHelper? Output { get; private set; }

        public KafkaContainer? Kafka { get; private set; }

        public DockerClient? Client { get; private set; }

        public Environment Start(ITestOutputHelper output)
        {
            Output = output;

            Client = new DockerEnvironmentBuilder().DockerClient;
            Client.DefaultTimeout = TimeSpan.FromSeconds(60);

            Kafka = new KafkaContainer(Client, Output);

            Kafka.Run(new Dictionary<string, string>()).GetAwaiter().GetResult();

            return this;
        }

        public void Dispose() 
        { 
            Kafka?.Dispose();
            Client?.Dispose();
        }
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
            containerWaiter: new KafkaWaiter(output),
            logger: new ContainerLogger(output))
        {
            Producer = new ProducerBuilder<byte[], byte[]>(new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                MessageTimeoutMs = 60_000
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
            static readonly TaskBufferPool<DeliveryResult<byte[], byte[]>> Pool = new (10);

            static readonly Message<byte[], byte[]> Empty = new Message<byte[], byte[]>
            {
                Key = Array.Empty<byte>(),
                Value = Array.Empty<byte>()
            };

            readonly ITestOutputHelper _output;

            public KafkaWaiter(ITestOutputHelper output) => _output = output;

            public async Task<bool> Wait(KafkaContainer container, CancellationToken cancellationToken)
            {
                try
                {
                    using var rent = Pool.Get();

                    rent.Value.Append(container.Producer.ProduceAsync(Rpc.RequestsTopic, Empty));
                    rent.Value.Append(container.Producer.ProduceAsync(Rpc.ResponsesTopic, Empty));
                    rent.Value.Append(container.Producer.ProduceAsync(Ping.Topic, Empty));
                    rent.Value.Append(container.Producer.ProduceAsync(Pong.Topic, Empty));
                    

                    await Task.WhenAll((Task[])rent.Value);

                    for (var i = 0; i < rent.Value.Written; i++) await rent.Value.WrittenSpan[i];
                    
                    return true;
                }
                catch (ProduceException<byte[], byte[]> ex)
                {
                    _output.WriteLine($"[Waiter] Produce exception occurred. Exception: {ex}");
                    return false;
                }
                catch (Exception ex)
                {
                    _output.WriteLine($"[Waiter] Unhandled exception occurred. Exception: {ex}");
                    return false;
                }
            }

            public Task<bool> Wait(Container container, CancellationToken cancellationToken) => Wait((KafkaContainer)container, cancellationToken);
        }
    }

    public sealed class ContainerLogger : ILogger
    {
        readonly ITestOutputHelper _output;

        public ContainerLogger(ITestOutputHelper output) => _output = output;

        public IDisposable BeginScope<TState>(TState state) => throw new NotImplementedException();

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter) =>
            _output.WriteLine($"[{logLevel}, {eventId}]: {formatter(state, exception)}");
    }
}
