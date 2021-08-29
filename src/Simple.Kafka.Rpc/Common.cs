using Confluent.Kafka;
using Simple.Dotnet.Utilities.Results;
using System;

namespace Simple.Kafka.Rpc
{
    internal static class RpcHeaders
    {
        public static readonly string RpcRequestID = "X-RPC-REQUESTID";
    }

    public static class RpcExtensions
    {
        public static Message<byte[], byte[]> WithRpcRequestId(this Message<byte[], byte[]> message, byte[]? requestId = null)
        {
            message.Headers.Add(new (RpcHeaders.RpcRequestID, requestId ?? Guid.NewGuid().ToByteArray()));
            return message;
        }

        public static byte[]? GetRpcRequestId(this Message<byte[], byte[]> message)
        {
            if (message.Headers.Count == 0) return null;
            
            for (var i = 0; i < message.Headers.Count; i++)
            {
                if (message.Headers[i].Key == RpcHeaders.RpcRequestID) return message.Headers[i].GetValueBytes();
            }

            return null;
        }

        public static Result<Guid, Exception> ParseRpcRequestId(this byte[] guid)
        {
            try
            {
                return Result.Ok<Guid, Exception>(new Guid(guid));
            }
            catch (Exception ex)
            {
                return Result.Error<Guid, Exception>(ex);
            }
        }
    }
}
