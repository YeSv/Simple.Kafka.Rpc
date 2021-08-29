namespace Simple.Kafka.Rpc
{
    public readonly struct HealthResult
    {
        public readonly bool IsHealthy;
        public readonly string? Reason;

        public HealthResult(bool healthy, string? reason) => (IsHealthy, Reason) = (healthy, reason);

        public static HealthResult Healthy => new HealthResult(true, null);
        public static HealthResult Unhealthy(string reason) => new HealthResult(false, reason);
    }


    internal static class Health
    {
        public static readonly HealthResult Healthy = HealthResult.Healthy;

        // Consumer:
        public static readonly HealthResult FailedToRecreateConsumer = HealthResult.Unhealthy("Failed to recreate consumer instance");
        public static readonly HealthResult FatalErrorRecreatingConsumer = HealthResult.Unhealthy("Received fatal consumer error. Recreating consumer instance");
        public static readonly HealthResult ConsumerStoppedDueToUnhandledException = HealthResult.Unhealthy($"Unhandled exception occurred in consumer thread. Consumer won't be recreated because {nameof(RpcConfig.StopConsumerOnUnhandledException)} is true");

        // Producer:
        public static readonly HealthResult FailedToRecreateProducer = HealthResult.Unhealthy("Failed to recreate producer instance");
        public static readonly HealthResult FatalErrorRecreatingProducer = HealthResult.Unhealthy("Received fatal consumer error. Recreating consumer instance");
    }
}
