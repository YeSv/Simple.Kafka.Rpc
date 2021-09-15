using FluentAssertions;
using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Simple.Kafka.Rpc.IntegrationTests.Tests
{
    public sealed class RpcUnhandledExceptionsTest : IClassFixture<Environment>
    {
        readonly Environment _env;
        readonly TimeSpan _timeout = TimeSpan.FromSeconds(5);

        public RpcUnhandledExceptionsTest(Environment env, ITestOutputHelper output) => _env = env.Start(output);

        [Fact]
        public async Task Rpc_Should_Be_Healthy_If_Unhandled_Exception_Occurred_In_Consumer_Setting_Disabled()
        {
            using var server = new PingServer(_env);
            using var rpc = Rpc.Create(_env, c =>
            {
                c.Config.StopConsumerOnUnhandledException = false;
                c.Consumer.RpcHandler.OnRpcMessage = (id, r) => throw new Exception("Test");
            });
            using var cts = new CancellationTokenSource(_timeout);

            var pongResult = await rpc.Ping(cts.Token);

            rpc.Health.IsHealthy.Should().BeTrue();
        }

        [Fact]
        public async Task Rpc_Should_Be_Unhealthy_If_Unhandled_Exception_Occurred_In_Consumer()
        {
            using var server = new PingServer(_env);
            using var rpc = Rpc.Create(_env, c =>
            {
                c.Config.StopConsumerOnUnhandledException = true;
                c.Consumer.RpcHandler.OnRpcMessage = (id, r) => throw new Exception("Test");
            });
            using var cts = new CancellationTokenSource(_timeout);

            var pongResult = await rpc.Ping(cts.Token);

            rpc.Health.IsHealthy.Should().BeFalse();
            rpc.Health.Reason.Should().Contain(Health.ConsumerStoppedDueToUnhandledException.Reason);
        }
    }
}
