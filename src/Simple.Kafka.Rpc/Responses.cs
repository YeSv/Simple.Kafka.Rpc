using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Simple.Kafka.Rpc
{
    internal enum CommandType : byte
    {
        Add,
        Remove,
        Complete
    }

    internal readonly struct Command<T>
    {
        public readonly CommandType Type;
        public readonly Guid Subscription;
        public readonly T? Data;
        public readonly TaskCompletionSource<T>? Tcs;

        public Command(CommandType type, Guid subscription, T? data, TaskCompletionSource<T>? tcs)
        {
            Tcs = tcs;
            Type = type;
            Data = data;
            Subscription = subscription;
        }
    }

    internal sealed class Responses<T> : IDisposable
    {
        readonly ActionBlock<Command<T>> _handler;
        readonly Dictionary<Guid, TaskCompletionSource<T>> _subscriptions;

        public Responses()
        {
            _subscriptions = new();
            _handler = new(c =>
           {
               switch (c.Type) 
               {
                   case CommandType.Add: _subscriptions[c.Subscription] = c.Tcs!; break;
                   case CommandType.Remove: _subscriptions.Remove(c.Subscription); break;
                   case CommandType.Complete:
                       if (!_subscriptions.TryGetValue(c.Subscription, out var tcs)) break;
                       tcs.TrySetResult(c.Data!);
                       _subscriptions.Remove(c.Subscription);
                       break;
               }
           }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1 }); 
        }

        public Task<T> Subscribe(Guid subscription)
        {
            var tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
            _handler.Post(Commands.Add(subscription, tcs));
            return tcs.Task;
        }

        public void Unsubscribe(Guid subscription) => _handler.Post(Commands.Remove(subscription));

        public void Complete(Guid subscription, T data) => _handler.Post(Commands.Complete(subscription, data));

        public void Dispose()
        {
            _handler?.Complete();
            _handler?.Completion.Wait();
            _subscriptions?.Clear();
        }

        internal static class Commands
        {
            public static Command<T> Add(Guid subscription, TaskCompletionSource<T> tcs) => new(CommandType.Add, subscription, default, tcs);
            public static Command<T> Complete(Guid subscription, T data) => new(CommandType.Complete, subscription, data, default);
            public static Command<T> Remove(Guid subscription) => new(CommandType.Remove, subscription, default, default);
        }
    }
}
