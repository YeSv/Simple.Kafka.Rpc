using Confluent.Kafka;
using Simple.Dotnet.Utilities.Results;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;

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
            message.Headers ??= new ();
            message.Headers.Add(new (RpcHeaders.RpcRequestID, requestId ?? Guid.NewGuid().ToByteArray()));
            return message;
        }

        public static byte[]? GetRpcRequestId(this Message<byte[], byte[]> message)
        {
            if (message.Headers is null or { Count: 0 }) return null;
            
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

    // See https://docs.confluent.io/5.5.0/clients/librdkafka/md_STATISTICS.html
    // Only brokers are used to check the health of a cluster from the librdkafka point of view
    public sealed class RpcKafkaStatistics
    {
        [JsonPropertyName("brokers")]
        public Dictionary<string, BrokerStatistics> Brokers { get; set; } = new ();

        public sealed class BrokerStatistics
        {
            [JsonPropertyName("name")]
            public string Name { get; set; }


            [JsonPropertyName("state")]
            public string State { get; set; }
        }

        public (int UnavailableBrokers, int AllBrokers) GetBrokerStats()
        {
            var query = Brokers.Where(b => !string.Equals(b.Key, "GroupCoordinator", StringComparison.OrdinalIgnoreCase));
            return (query.Count(b => !string.Equals(b.Value.State, "UP", StringComparison.OrdinalIgnoreCase)), query.Count());
        }

        public static UniResult<RpcKafkaStatistics, Exception> Parse(string json)
        {
            try
            {
                return UniResult.Ok<RpcKafkaStatistics, Exception>(JsonSerializer.Deserialize<RpcKafkaStatistics>(json));
            }
            catch (Exception ex)
            {
                return UniResult.Error<RpcKafkaStatistics, Exception>(ex);
            }
        }
    }
}
