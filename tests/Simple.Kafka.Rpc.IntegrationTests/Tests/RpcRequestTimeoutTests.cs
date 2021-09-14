using Confluent.Kafka;
using FluentAssertions;
using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Simple.Kafka.Rpc.IntegrationTests.Tests
{
    public sealed class RpcRequestTimeoutTests : IClassFixture<Environment>
    {
        readonly Environment _env;

        public RpcRequestTimeoutTests(Environment env, ITestOutputHelper output) => _env = env.Start(output);

        [Theory]
        [InlineData(1_000)]
        [InlineData(5_000)]
        [InlineData(10_000)]
        public async Task Rpc_Should_Respect_RequestTimeout_Setting_MessageTimeoutMs(int timeoutMs)
        {
            await _env.Kafka.Stop();
            try
            {

                using var server = new PingServer(_env);
                using var rpc = Rpc.Create(_env, c => c.Config.RequestTimeout = TimeSpan.FromMilliseconds(timeoutMs));
                using var cts = new CancellationTokenSource(timeoutMs * 2);

                var response = await rpc.Ping();

                cts.IsCancellationRequested.Should().BeFalse();

                response.IsOk.Should().BeFalse();
                response.Error.Should().BeOfType<RpcException>();
                ((RpcException)response.Error!).Type.Should().Be(ErrorType.Kafka);
                ((ProduceException<byte[], byte[]>)((RpcException)response.Error!).InnerException).Error.Code.Should().Be(ErrorCode.Local_MsgTimedOut);
            }
            finally
            {
                await _env.Kafka.Run(Environment.EmptyEnvVariables);
            }
        }

        [Theory]
        [InlineData(1_000)]
        [InlineData(5_000)]
        [InlineData(10_000)]
        public async Task Rpc_Should_Respect_RequestTimeout_Setting_RpcTimeout(int timeoutMs)
        {
            using var rpc = Rpc.Create(_env, c => c.Config.RequestTimeout = TimeSpan.FromMilliseconds(timeoutMs));
            using var cts = new CancellationTokenSource(timeoutMs * 2);

            var response = await rpc.Ping();

            cts.IsCancellationRequested.Should().BeFalse();
            response.IsOk.Should().BeFalse();
            response.Error.Should().BeOfType<RpcException>();
            ((RpcException)response.Error!).Type.Should().Be(ErrorType.Timeout);
        }
    }
}
