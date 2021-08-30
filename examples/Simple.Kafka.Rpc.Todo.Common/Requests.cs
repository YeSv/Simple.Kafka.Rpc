using System;

namespace Simple.Kafka.Rpc.Todo.Common
{
    public sealed class AddTodoRequest
    {
        public Guid UserId { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
    }

    public sealed class AddTodoResponse
    {
        public Guid TodoId { get; set; }
    }

    public sealed class GetTodosRequest
    {
        public Guid UserId { get; set; }
    }

    public sealed class GetTodosResponse
    {
        public TodoItem[] Todos { get; set; }
    }

    public sealed class RemoveTodoRequest
    {
        public Guid UserId { get; set; }
        public Guid TodoId { get; set; }
    }

    public sealed class RemoveTodoResponse
    {
        public Guid TodoId { get; set; }
    }
}
