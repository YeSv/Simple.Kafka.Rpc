using Simple.Kafka.Rpc.Todo.Common;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Simple.Kafka.Rpc.Todo.Server
{
    public sealed class Storage
    {
        readonly Dictionary<Guid, Dictionary<Guid, TodoItem>> _todos = new ();

        public GetTodosResponse GetTodos(GetTodosRequest request)
        {
            if (!_todos.TryGetValue(request.UserId, out var todos)) return new GetTodosResponse { Todos = Array.Empty<TodoItem>() };
            return new GetTodosResponse { Todos = todos.Values.ToArray() };
        }

        public RemoveTodoResponse RemoveTodo(RemoveTodoRequest request)
        {
            if (!_todos.TryGetValue(request.UserId, out var todos)) return new RemoveTodoResponse { TodoId = Guid.Empty };
            todos.Remove(request.TodoId);

            return new RemoveTodoResponse { TodoId = request.TodoId };
        }

        public AddTodoResponse AddTodo(AddTodoRequest request)
        {
           if (!_todos.ContainsKey(request.UserId)) _todos[request.UserId] = new();

            var todo = new TodoItem { Description = request.Description, Id = Guid.NewGuid(), Name = request.Name };
            _todos[request.UserId][todo.Id] = todo;

            return new AddTodoResponse { TodoId = todo.Id };
        }
    }
}
