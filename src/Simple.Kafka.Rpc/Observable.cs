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

    internal static class ObservableCommands
    {
        public static Command<T> Add<T>(Guid subscription, TaskCompletionSource<T> tcs) => new (CommandType.Add, subscription, default, tcs);
        public static Command<T> Complete<T>(Guid subscription, T data) => new (CommandType.Complete, subscription, data, default);
        public static Command<T> Remove<T>(Guid subscription) => new (CommandType.Remove, subscription, default, default);
    }

    internal sealed class Observable<T> : IDisposable
    {
        readonly ActionBlock<Command<T>> _handler;
        readonly Dictionary<Guid, TaskCompletionSource<T>> _subscriptions;

        public Observable()
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
            _handler.Post(ObservableCommands.Add(subscription, tcs));
            return tcs.Task;
        }

        public void Unsubscribe(Guid subscription) => _handler.Post(ObservableCommands.Remove<T>(subscription));

        public void Complete(Guid subscription, T data) => _handler.Post(ObservableCommands.Complete(subscription, data));

        public void Dispose()
        {
            _handler?.Complete();
            _handler?.Completion.Wait();
            _subscriptions?.Clear();
        }
    }
}
