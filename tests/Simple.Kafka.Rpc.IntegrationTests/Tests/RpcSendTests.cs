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
    }
}
