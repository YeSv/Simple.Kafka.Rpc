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
    public sealed class RpcNoPartitionsAssignedTests : IClassFixture<Environment>
    {
        readonly Environment _env;
        readonly TimeSpan _timeout = TimeSpan.FromSeconds(30);

        public RpcNoPartitionsAssignedTests(Environment env, ITestOutputHelper output) => _env = env.Start(output);

        [Fact]
        public async Task Rpc_Should_Be_Healthy_If_NoPartitionAssigned_Setting_Disabled()
        {
            var groupId = Guid.NewGuid().ToString();

            using var rpc1 = Rpc.Create(_env, b =>
            {
                b.Consumer.Kafka.GroupId = groupId;
                b.Config.Topics = new[] { Pong.Topic };
                b.Config.UnhealthyIfNoPartitionsAssigned = false;
            });
            using var rpc2 = Rpc.Create(_env, b =>
            {
                b.Consumer.Kafka.GroupId = groupId;
                b.Config.Topics = new[] { Pong.Topic };
                b.Config.UnhealthyIfNoPartitionsAssigned = false;
            });
            using var cts = new CancellationTokenSource(_timeout);

            var delayTask = Task.Delay(Timeout.Infinite, cts.Token).ContinueWith(e => { });
            var rpc1Health = rpc1.WaitForHealth(h => !h.IsHealthy, cts.Token);
            var rpc2Health = rpc2.WaitForHealth(h => !h.IsHealthy, cts.Token);

            await Task.WhenAll(delayTask, rpc1Health, rpc2Health);

            delayTask.IsCompleted.Should().BeTrue();
            rpc1Health.Result.IsHealthy.Should().BeTrue();
            rpc2Health.Result.IsHealthy.Should().BeTrue();
        }

        [Fact]
        public async Task Rpc_Should_Be_Healthy_If_NoPartitionAssigned_Setting_Enabled()
        {
            var groupId = Guid.NewGuid().ToString();

            using var rpc1 = Rpc.Create(_env, b =>
            {
                b.Consumer.Kafka.GroupId = groupId;
                b.Config.Topics = new[] { Pong.Topic };
                b.Config.UnhealthyIfNoPartitionsAssigned = true;
            });

            using var rpc2 = Rpc.Create(_env, b =>
            {
                b.Consumer.Kafka.GroupId = groupId;
                b.Config.Topics = new[] { Pong.Topic };
                b.Config.UnhealthyIfNoPartitionsAssigned = true;
            }); 
            using var cts = new CancellationTokenSource(_timeout);

            var delayTask = Task.Delay(Timeout.Infinite, cts.Token).ContinueWith(e => { });
            var rpc1Health = rpc1.WaitForHealth(h => h.IsHealthy, cts.Token);
            var rpc2Health = rpc2.WaitForHealth(h => h.IsHealthy, cts.Token);

            await Task.WhenAll(delayTask, rpc1Health, rpc2Health);

            delayTask.IsCompleted.Should().BeTrue();

            var taskToCheck = rpc1Health.Result.IsHealthy ? rpc2Health : rpc1Health;
            taskToCheck.Result.IsHealthy.Should().BeFalse();
            taskToCheck.Result.Reason.Should().Contain(Health.ConsumerAssignedToZeroPartitions.Reason);
        }
    }
}
