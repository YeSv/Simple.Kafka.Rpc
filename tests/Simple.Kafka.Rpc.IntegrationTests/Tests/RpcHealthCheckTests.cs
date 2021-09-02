using FluentAssertions;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Simple.Kafka.Rpc.IntegrationTests.Tests
{
    public class RpcHealthCheckTests : IClassFixture<Environment>
    {
        static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

        readonly Environment _env;

        public RpcHealthCheckTests(Environment env)
        {
            _env = env;
            _env.Kafka.Run(new Dictionary<string, string>()).GetAwaiter().GetResult();
        }

        [Fact]
        public async Task Rpc_Health_Check_Should_Be_Healthy_If_Kafka_Is_Alive()
        {
            using var timeout = new CancellationTokenSource(Timeout);
            using var rpc = Rpc.Create(_env);

            var health = await rpc.WaitForHealth(h => h.IsHealthy, timeout.Token);

            health.IsHealthy.Should().BeTrue();
            health.Reason.Should().BeNull();
        }

        [Fact]
        public async Task Rpc_Health_Check_Should_Be_Unhealthy_If_Kafka_Is_Down()
        {
            using var timeout = new CancellationTokenSource(Timeout);
            using var rpc = Rpc.Create(_env);

            try
            {
                await _env.Kafka.Stop(timeout.Token);

                var health = await rpc.WaitForHealth(h => h.IsHealthy, timeout.Token);

                health.IsHealthy.Should().BeFalse();
                health.Reason.Should().NotBeNullOrWhiteSpace();
            }
            finally
            {
                await _env.Kafka.Run(new Dictionary<string, string>());
            }
        }
    }
}
