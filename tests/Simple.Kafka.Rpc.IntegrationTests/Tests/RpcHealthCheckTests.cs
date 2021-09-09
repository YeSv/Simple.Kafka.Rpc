using FluentAssertions;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Simple.Kafka.Rpc.IntegrationTests.Tests
{
    public class RpcHealthCheckTests : IClassFixture<Environment>
    {
        static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

        readonly Environment _env;
        readonly ITestOutputHelper _output;

        public RpcHealthCheckTests(Environment env, ITestOutputHelper output)
        {
            _output = output;
            _env = env.Start(output);
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
            try
            {
                await _env.Kafka!.Stop(timeout.Token);

                using var rpc = Rpc.Create(_env);

                var health = await rpc.WaitForHealth(h => !h.IsHealthy, timeout.Token);

                health.IsHealthy.Should().BeFalse();
                health.Reason.Should().Be(Health.ConsumerAssignedToZeroPartitions.Reason);
            }
            finally
            {
                await _env.Kafka!.Run(Environment.EmptyEnvVariables);
            }
        }

        [Fact]
        public async Task Rpc_Health_Check_Should_Be_Unhealthy_If_Kafka_Is_Down_After_Start()
        {
            using var timeout = new CancellationTokenSource(Timeout * 2);
            using var rpc = Rpc.Create(_env);

            try
            {
                var health = await rpc.WaitForHealth(h => h.IsHealthy, timeout.Token);
                health.IsHealthy.Should().BeTrue();
                health.Reason.Should().BeNull();


                await _env.Kafka!.Stop(timeout.Token);

                health = await rpc.WaitForHealth(h => !h.IsHealthy, timeout.Token);

                health.IsHealthy.Should().BeFalse();
                health.Reason.Should().NotBeNullOrWhiteSpace();
            }
            finally
            {
                await _env.Kafka!.Run(Environment.EmptyEnvVariables);
            }
        }

        [Fact]
        public async Task Rpc_Health_Check_Should_Recover()
        {
            using var timeout = new CancellationTokenSource(Timeout * 2);
            using var rpc = Rpc.Create(_env);

            try
            {
                var health = await rpc.WaitForHealth(h => h.IsHealthy, timeout.Token);
                health.IsHealthy.Should().BeTrue();
                health.Reason.Should().BeNull();

                await _env.Kafka!.Stop(timeout.Token);

                health = await rpc.WaitForHealth(h => !h.IsHealthy, timeout.Token);
                health.IsHealthy.Should().BeFalse();
                health.Reason.Should().NotBeNullOrWhiteSpace();


                await _env.Kafka!.Run(Environment.EmptyEnvVariables);
                
                health = await rpc.WaitForHealth(h => h.IsHealthy, timeout.Token);
                health.IsHealthy.Should().BeTrue();
                health.Reason.Should().BeNull();
            }
            finally
            {
                await _env.Kafka!.Run(Environment.EmptyEnvVariables);
            }
        }
    }
}
