using Confluent.Kafka;
using Simple.Kafka.Rpc.Todo.Common;
using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Simple.Kafka.Rpc.Todo.Server
{
    class Program
    {
        static void Main(string[] args)
        {
            // Create consumer that is used to receive requests
            var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
            {
                EnableAutoCommit = true,
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                GroupId = "Simple.Kafka.Rpc.Todo.Server.Group"
            }).Build();

            // Create producer that is used to produce responses
            var producer = new ProducerBuilder<byte[], byte[]>(new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                LingerMs = 10,
            }).Build();

            consumer.Subscribe(new[] { "get-todos-requests", "add-todo-requests", "remove-todo-requests" });

            var storage = new Storage();
            var stop = new CancellationTokenSource();

            // Consumer thread to consume messages from topics
            var consumerThread = Task.Factory.StartNew(() =>
            {
                try
                {
                    var token = stop.Token;
                    while (!token.IsCancellationRequested)
                    {
                        var result = consumer.Consume(token);
                        if (result == null || result.IsPartitionEOF) continue;

                        // Get request from Rpc message
                        var rpcRequestId = result.Message.GetRpcRequestId();
                        if (rpcRequestId == null) continue; // Not an rpc request

                        Console.WriteLine($"Received request with id: {rpcRequestId.ParseRpcRequestId()}");

                        // Handle a response and produce to specified topic which client is subscribed to
                        // NOTE: this example uses multiple topics but this can be one with requests and another with responses, it's up to you
                        var response = Array.Empty<byte>();
                        switch (result.Topic)
                        {
                            case "get-todos-requests":
                                var getTodosResponse = storage.GetTodos(JsonSerializer.Deserialize<GetTodosRequest>(result.Message.Value));
                                response = JsonSerializer.SerializeToUtf8Bytes(getTodosResponse);
                                break;
                            case "add-todo-requests":
                                var addTodoResponse = storage.AddTodo(JsonSerializer.Deserialize<AddTodoRequest>(result.Message.Value));
                                response = JsonSerializer.SerializeToUtf8Bytes(addTodoResponse);
                                break;
                            case "remove-todo-requests":
                                var removeTodoResponse = storage.RemoveTodo(JsonSerializer.Deserialize<RemoveTodoRequest>(result.Message.Value));
                                response = JsonSerializer.SerializeToUtf8Bytes(removeTodoResponse);
                                break;
                        }

                        producer.Produce(result.Topic switch
                        {
                            "get-todos-requests" => "get-todos-responses",
                            "add-todo-requests" => "add-todo-responses",
                            "remove-todo-requests" => "remove-todo-responses"
                        },
                        new Message<byte[], byte[]>
                        {
                            Key = Array.Empty<byte>(),
                            Value = response
                        }.WithRpcRequestId(rpcRequestId)); // Add rpcRequestId into a message

                        Console.WriteLine($"Added a response in producer queue for request: {rpcRequestId.ParseRpcRequestId()}");
                    }
                }
                catch (OperationCanceledException) { }
                catch (Exception ex)
                {
                    Console.WriteLine($"Unhandled exception occurred: {ex}");
                }
            }, stop.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);

            Console.WriteLine("Press enter to stop");
            Console.ReadLine();

            stop.Cancel();
        }
    }
}
