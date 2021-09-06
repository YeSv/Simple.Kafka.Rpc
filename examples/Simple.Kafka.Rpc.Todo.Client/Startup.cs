using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Simple.Kafka.Rpc.Todo.Client
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        public void ConfigureServices(IServiceCollection services)
        {

            services.AddControllers();

            services.AddHealthChecks().AddCheck<RpcHealthCheck>("rpc");

            services.AddSwaggerGen();
            services.AddSingleton<IKafkaRpc, RpcClient>(p =>
                new RpcBuilder()
                    .WithConfig(c =>
                    {
                        c.RequestTimeout = TimeSpan.FromSeconds(5); // Timeout for an rpc operation (Passed cancellation token overrides this config)
                        c.StopConsumerOnUnhandledException = false; // If unhandled exception occures in consumer thread - continue...if true -> everything is stopped
                        c.UnhealthyIfNoPartitionsAssigned = true; // If there are no assignments -> unhealthy
                        c.Topics = new[] { "get-todos-responses", "add-todo-responses", "remove-todo-responses" }; // Topics to subscribe for responses
                        c.ConsumerRecreationPause = TimeSpan.FromSeconds(15); // If error occurs on recreation of consumer a dedicated thread will be paused to retry later
                        c.ProducerRecreationPause = TimeSpan.FromSeconds(15); // If error occurs on recreation of producer a dedicated thread will be paused to retry later
                    })
                    .Consumer
                    .WithConfig(c => c.BootstrapServers = "localhost:9092")
                    .WithRpcEvents(e =>
                    {
                        // Logs for different events from rpc library
                        e.OnEof = c => Console.WriteLine($"Got an eof: {c.TopicPartitionOffset}");
                        e.OnRpcLog = c => Console.WriteLine($"Consumer RpcLog: {c.Message} [{c.Level}]");
                        e.OnRpcMessage = (id, r) => Console.WriteLine($"received respose for: {id}");
                    })
                    .WithKafkaEvents(e =>
                    {
                        e.OnErrorRestart = e => e.IsFatal; // If fatal - recreate consumer
                        e.OnAssigned = (c, e) => Console.WriteLine($"Assigned: {string.Join(",", e)}");
                        e.OnRevoked = (c, r) => Console.WriteLine($"Revoked: {string.Join(",", r)}");
                        e.OnCommitted = (c, t) => Console.WriteLine($"Committed: {string.Join(",", t.Offsets)}");
                        e.OnStatistics = (c, s) => Console.WriteLine($"Consumer tatistics: {s}");
                        e.OnError = (c, e) => Console.WriteLine($"Consumer error occurred: {e.Reason}");
                        e.OnLog = (c, e) => Console.WriteLine($"Consumer log: {e.Message} [{e.Level}]");
                    })
                    .Rpc
                    .Producer
                    .WithConfig(c => c.BootstrapServers = "localhost:9092")
                    .WithRpcEvents(e =>
                    {
                        // Logs for different events from rpc library
                        e.OnRpcLog = c => Console.WriteLine($"Producer RpcLog: {c.Message} [{c.Level}]");
                    })
                    .WithKafkaEvents(e =>
                    {
                        e.OnErrorRestart = e => e.IsFatal; // If fatal - recreate consumer
                        e.OnError = (c, e) => Console.WriteLine($"Producer error occurred: {e}");
                        e.OnStatistics = (c, s) => Console.WriteLine($"Producer statistics: {s}");
                        e.OnLog = (c, e) => Console.WriteLine($"Producer log: {e.Message} [{e.Level}]");
                    })
                    .Rpc
                    .Build());
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            app.UseHealthChecks("/healthcheck");

            app.UseSwagger();

            app.UseSwaggerUI(c =>
            {
                c.RoutePrefix = string.Empty;
                c.SwaggerEndpoint("/swagger/v1/swagger.json", "TodosApi");
            });

            app.UseRouting();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();
            });
        }
    }
}
