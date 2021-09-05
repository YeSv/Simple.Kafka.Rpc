namespace Simple.Kafka.Rpc
{
    public sealed class HealthResult
    {
        public bool IsHealthy { get; }
        public string? Reason { get; }

        public HealthResult(bool healthy, string? reason) => (IsHealthy, Reason) = (healthy, reason);

        public static HealthResult Healthy => new (true, null);
        public static HealthResult Unhealthy(string reason) => new (false, reason);
    }

    internal readonly struct HealthChange
    {
        public readonly string Id;
        public readonly HealthResult Result;

        public HealthChange(string id, HealthResult result) => (Id, Result) = (id, result);
    }


    public static class Health
    {
        public static readonly HealthResult Healthy = HealthResult.Healthy;

        // Consumer:
        public static readonly HealthResult FailedToRecreateConsumer = HealthResult.Unhealthy("Failed to recreate consumer instance");
        public static readonly HealthResult ConsumerAssignedToZeroPartitions = HealthResult.Unhealthy("Consumer is not assigned to any topic or partition");
        public static readonly HealthResult ConsumerFatalError = HealthResult.Unhealthy("Received fatal consumer error");
        public static readonly HealthResult ConsumerStoppedDueToUnhandledException = HealthResult.Unhealthy($"Unhandled exception occurred in consumer thread. Consumer won't be recreated because {nameof(RpcConfig.StopConsumerOnUnhandledException)} is true");

        // Producer:
        public static readonly HealthResult FailedToRecreateProducer = HealthResult.Unhealthy("Failed to recreate producer instance");
        public static readonly HealthResult ProducerFatalError = HealthResult.Unhealthy("Received fatal producer error");
    }
}
