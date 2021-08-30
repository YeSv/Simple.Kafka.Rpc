using System;

namespace Simple.Kafka.Rpc.Todo.Common
{
    public sealed class TodoItem
    {
        public Guid Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
    }
}
