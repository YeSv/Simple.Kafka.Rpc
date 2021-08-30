using Microsoft.AspNetCore.Mvc;
using Simple.Kafka.Rpc.Todo.Common;
using System;
using System.Threading.Tasks;

namespace Simple.Kafka.Rpc.Todo.Client.Controllers
{
    [Route("todos")]
    [ApiController]
    public class TodoController : ControllerBase
    {
        readonly IKafkaRpc _rpc;

        public TodoController(IKafkaRpc rpc) => _rpc = rpc;

        [HttpPost("add")]
        [ProducesResponseType(typeof(AddTodoResponse), 200)]
        public async Task<ActionResult> Add([FromBody] AddTodoRequest request) => 
            Ok(await _rpc.AddTodo(request));

        [HttpPost("remove")]
        [ProducesResponseType(typeof(RemoveTodoResponse), 200)]
        public async Task<ActionResult> Remove([FromBody] RemoveTodoRequest request) =>
            Ok(await _rpc.RemoveTodo(request));

        [HttpGet]
        [ProducesResponseType(typeof(GetTodosResponse), 200)]
        public async Task<ActionResult> Get([FromQuery] Guid userId) =>
            Ok(await _rpc.GetTodos(new GetTodosRequest { UserId = userId }));
    }
}
