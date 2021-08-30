using System;

namespace Simple.Kafka.Rpc
{
    public enum ErrorType : byte
    {
        Kafka,
        Timeout,
        UnhandledException
    }

    public sealed class RpcException : Exception
    {
        public ErrorType Type { get; }
        public Guid RequestId { get; }

        public RpcException(ErrorType type, Guid requestId, string message) : base(message)
        {
            Type = type;
            RequestId = requestId;
        }

        public RpcException(ErrorType type, Guid requestId, string message, Exception ex) : base(message, ex)
        {
            Type = type;
            RequestId = requestId;
        }

        internal static RpcException Kafka(Guid requestId, Exception ex) => new(ErrorType.Kafka, requestId, "Kafka exception occurred", ex);
        internal static RpcException Timeout(Guid requestId) => new (ErrorType.Timeout, requestId, "Timeout occurred during RPC operation");
        internal static RpcException Unhandled(Guid requestId, Exception ex) => new (ErrorType.UnhandledException, requestId, "Unhandled exception occurred during RPC operation", ex);
    }
}
