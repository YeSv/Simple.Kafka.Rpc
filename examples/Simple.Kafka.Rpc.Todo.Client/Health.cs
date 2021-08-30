using Microsoft.Extensions.Diagnostics.HealthChecks;
using System.Threading;
using System.Threading.Tasks;

namespace Simple.Kafka.Rpc.Todo.Client
{
    public sealed class RpcHealthCheck : IHealthCheck
    {
        readonly IKafkaRpc _rpc;

        public RpcHealthCheck(IKafkaRpc rpc) => _rpc = rpc;

        public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
        {
            var health = _rpc.Health;
            return Task.FromResult(health.IsHealthy ? HealthCheckResult.Healthy() : HealthCheckResult.Unhealthy(health.Reason));
        }
    }
}
