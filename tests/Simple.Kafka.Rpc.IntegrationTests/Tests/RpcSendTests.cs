using Confluent.Kafka;
using FluentAssertions;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Simple.Kafka.Rpc.IntegrationTests.Tests
{
    public sealed class RpcSendTests : IClassFixture<Environment>
    {
        readonly Environment _env;
        readonly ITestOutputHelper _output;
        readonly TimeSpan _timeout = TimeSpan.FromSeconds(30);

        public RpcSendTests(Environment env, ITestOutputHelper output)
        {
            _output = output;
            _env = env.Start(output);
        }

        [Fact]
        public async Task Rpc_Send_Should_Receive_Response()
        {
            using var server = new PingServer(_env);
            using var rpc = Rpc.Create(_env);

            var pongResult = await rpc.Ping();

            pongResult.IsOk.Should().BeTrue();
            pongResult.Ok.Time.Should().NotBe(default);
        }

        [Fact]
        public async Task Rpc_Send_Should_Respect_Timeout()
        {
            using var server = new PingServer(_env);
            using var rpc = Rpc.Create(_env);
            using var timeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(1));

            await Task.Delay(TimeSpan.FromSeconds(1));
            
            var pongResult = await rpc.Ping(timeout.Token);

            pongResult.IsOk.Should().BeFalse();
            pongResult.Error.Should().BeOfType<RpcException>();
            ((RpcException)pongResult.Error).Type.Should().Be(ErrorType.Timeout);
            ((RpcException)pongResult.Error).RequestId.Should().NotBe(Guid.Empty);
        }

        [Fact]
        public async Task Rpc_Send_Should_Send_Multiple()
        {
            using var server = new PingServer(_env);
            using var rpc = Rpc.Create(_env);
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));

            var tasks = Enumerable.Range(0, 100).Select(_ => rpc.Ping(timeout.Token)).ToArray();

            await Task.WhenAll(tasks);

            foreach (var task in tasks)
            {
                task.IsCompletedSuccessfully.Should().BeTrue();

                task.Result.IsOk.Should().BeTrue();
                task.Result.Ok.Time.Should().NotBe(default);

            }
        }

        [Fact]
        public async Task Rpc_Send_Should_Respect_Producer_Timeout()
        {
            await _env.Kafka!.Stop();
            try
            {
                using var rpc = Rpc.Create(_env, c => c.Producer.Kafka.MessageTimeoutMs = 1_000);

                var result = await rpc.Ping();

                result.IsOk.Should().BeFalse();
                result.Error!.Should().BeOfType<RpcException>();
                ((RpcException)result.Error!).Type.Should().Be(ErrorType.Kafka);
                ((RpcException)result.Error!).InnerException.Should().BeOfType<ProduceException<byte[], byte[]>>();
                ((ProduceException<byte[], byte[]>)((RpcException)result.Error).InnerException).Error.Code.Should().Be(ErrorCode.Local_MsgTimedOut);
            }
            finally
            {
                await _env.Kafka.Run(Environment.EmptyEnvVariables);
            }
        }

        [Fact]
        public async Task Rpc_Send_Should_Still_Work_After_Transient_Error()
        {
            try
            {
                using var rpc = Rpc.Create(_env, c => c.Producer.Kafka.MessageTimeoutMs = 10_000);

                await _env.Kafka.Stop();

                var failedResult = await rpc.Ping();

                failedResult.IsOk.Should().BeFalse();
                failedResult.Error!.Should().BeOfType<RpcException>();
                ((RpcException)failedResult.Error!).Type.Should().Be(ErrorType.Kafka);


                await _env.Kafka.Run(Environment.EmptyEnvVariables);
                using var timeout = new CancellationTokenSource(_timeout);

                using var server = new PingServer(_env);
                var health = await rpc.WaitForHealth(h => h.IsHealthy, timeout.Token);
                health.IsHealthy.Should().BeTrue();

                var okResult = await rpc.Ping();
                okResult.IsOk.Should().BeTrue();
                okResult.Ok.Time.Should().NotBe(default(DateTime));
            }
            finally
            {
                await _env.Kafka.Run(Environment.EmptyEnvVariables);
            }
        }
    }
}
