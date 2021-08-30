using Simple.Kafka.Rpc.Todo.Common;
using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Simple.Kafka.Rpc.Todo.Client
{
    public static class RpcExtensions
    {
        public static async Task<GetTodosResponse> GetTodos(this IKafkaRpc rpc, GetTodosRequest request)
        {
            var response = await rpc.SendAsync(
                Array.Empty<byte>(), // key
                JsonSerializer.SerializeToUtf8Bytes(request), // value
                "get-todos-requests"); // topic

            return JsonSerializer.Deserialize<GetTodosResponse>(response.Message.Value);
        }

        public static async Task<AddTodoResponse> AddTodo(this IKafkaRpc rpc, AddTodoRequest request)
        {
            var response = await rpc.SendAsync(
                Array.Empty<byte>(), // key
                JsonSerializer.SerializeToUtf8Bytes(request), // value
                "add-todo-requests"); // topic

            return JsonSerializer.Deserialize<AddTodoResponse>(response.Message.Value);

        }

        public static async Task<RemoveTodoResponse> RemoveTodo(this IKafkaRpc rpc, RemoveTodoRequest request)
        {
            var response = await rpc.SendAsync(
                Array.Empty<byte>(), // key
                JsonSerializer.SerializeToUtf8Bytes(request), // value
                "remove-todo-requests"); // topic

            return JsonSerializer.Deserialize<RemoveTodoResponse>(response.Message.Value);
        }
    }
}
