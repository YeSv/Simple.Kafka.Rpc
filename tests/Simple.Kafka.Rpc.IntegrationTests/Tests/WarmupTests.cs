using FluentAssertions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Simple.Kafka.Rpc.IntegrationTests.Tests
{
    public sealed class WarmupTests : IClassFixture<Environment>
    {
        readonly Environment _env;
        readonly TimeSpan _timeout = TimeSpan.FromSeconds(30);

        public WarmupTests(Environment env, ITestOutputHelper output) => _env = env.Start(output);

        [Fact]
        public void Rpc_Should_Start_With_Warmup()
        {
            using var rpc = Rpc.Create(_env);

            rpc.Health.IsHealthy.Should().BeTrue();
        }

        [Fact]
        public void Rpc_Should_Override_IsEof()
        {
            using var rpc = Rpc.Create(_env, c => c.Consumer.Kafka.EnablePartitionEof = false);
            rpc.Health.IsHealthy.Should().BeTrue();
        }

        [Fact]
        public async Task Rpc_Should_Be_Unhealthy_After_Warmup_Fail()
        {
            await _env.Kafka!.Stop();
            try
            {
                using var rpc = Rpc.Create(_env);
                using var timeout = new CancellationTokenSource(_timeout);

                var health = await rpc.WaitForHealth(h => !h.IsHealthy, timeout.Token);

                health.IsHealthy.Should().BeFalse();
                health.Reason.Should().Contain(Health.ConsumerAssignedToZeroPartitions.Reason);
            }
            finally
            {
                await _env.Kafka!.Run(Environment.EmptyEnvVariables);
            }
        }

        [Fact]
        public async Task Rpc_Should_Be_Healthy_If_Warmup_Is_Disabled()
        {
            await _env.Kafka!.Stop();
            try
            {
                using var rpc = Rpc.Create(_env, c => c.Config.UnhealthyIfNoPartitionsAssigned = false);
                using var timeout = new CancellationTokenSource(_timeout);

                var health = await rpc.WaitForHealth(h => h.IsHealthy, timeout.Token);

                health.IsHealthy.Should().BeTrue();
            }
            finally
            {
                await _env.Kafka!.Run(Environment.EmptyEnvVariables);
            }
        }
    }
}
