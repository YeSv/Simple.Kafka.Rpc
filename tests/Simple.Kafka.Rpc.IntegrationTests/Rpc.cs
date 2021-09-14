using System;
using System.Threading;
using System.Threading.Tasks;

namespace Simple.Kafka.Rpc.IntegrationTests
{
    public static class Rpc
    {
        public static readonly string RequestsTopic = "requests";
        public static readonly string ResponsesTopic = "responses";

        public static IKafkaRpc Create(Environment env, Action<RpcBuilder> @override = null)
        {
            var builder = new RpcBuilder()
                .WithConfig(c =>
                {
                    c.RequestTimeout = TimeSpan.FromSeconds(30);
                    c.Topics = new[] { ResponsesTopic, Pong.Topic };
                    c.UnhealthyIfNoPartitionsAssigned = true;
                    c.EnableBrokerAvailabilityHealthCheck = true;
                })
                .Consumer
                .WithConfig(c => c.BootstrapServers = "localhost:9092")
                .WithRpcEvents(e =>
                {
                    // Logs for different events from rpc library
                    e.OnEof = c => env.Output.WriteLine($"[RPC] Got an eof: {c.TopicPartitionOffset}");
                    e.OnRpcLog = c => env.Output.WriteLine($"[RPC] Consumer RpcLog: {c.Message} [{c.Level}]");
                    e.OnRpcMessage = (id, r) => env.Output.WriteLine($"[RPC] Consumer received respose for: {id}");
                })
                .WithKafkaEvents(e =>
                {
                    e.OnErrorRestart = e => e.IsFatal; // If fatal - recreate consumer
                    e.OnAssigned = (c, e) => env.Output.WriteLine($"[RPC] Assigned: {string.Join(",", e)}");
                    e.OnRevoked = (c, r) => env.Output.WriteLine($"[RPC] Revoked: {string.Join(",", r)}");
                    e.OnCommitted = (c, t) => env.Output.WriteLine($"[RPC] Committed: {string.Join(",", t.Offsets)}");
                    e.OnStatistics = (c, s) => env.Output.WriteLine($"[RPC] Consumer statistics: {s}");
                    e.OnError = (c, e) => env.Output.WriteLine($"[RPC] Consumer error occurred: {e.Reason}");
                    e.OnLog = (c, e) => env.Output.WriteLine($"[RPC] Consumer log: {e.Message} [{e.Level}]");
                })
                .Rpc
                .Producer
                .WithConfig(c => c.BootstrapServers = "localhost:9092")
                .WithRpcEvents(e =>
                {
                    // Logs for different events from rpc library
                    e.OnRpcLog = c => env.Output.WriteLine($"[RPC] Producer RpcLog: {c.Message} [{c.Level}]");
                })
                .WithKafkaEvents(e =>
                {
                    e.OnErrorRestart = e => e.IsFatal; // If fatal - recreate consumer
                    e.OnError = (c, e) => env.Output.WriteLine($"[RPC] Producer error occurred: {e}");
                    e.OnStatistics = (c, s) => env.Output.WriteLine($"[RPC] Producer statistics: {s}");
                    e.OnLog = (c, e) => env.Output.WriteLine($"[RPC] Producer log: {e.Message} [{e.Level}]");
                })
                .Rpc;

            @override?.Invoke(builder);
            return builder.Build();
        }


        public static async Task<HealthResult> WaitForHealth(this IKafkaRpc rpc, Predicate<HealthResult> predicate, CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                if (predicate(rpc.Health)) return rpc.Health;

                await Task.Delay(TimeSpan.FromSeconds(5));
            }

            return rpc.Health;
        }
    }
}
