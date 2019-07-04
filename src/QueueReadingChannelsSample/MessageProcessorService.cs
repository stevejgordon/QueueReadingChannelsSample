using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using QueueReadingChannelsSample.Configuration;
using QueueReadingChannelsSample.Logging;

namespace QueueReadingChannelsSample
{
    public sealed class MessageProcessorService : BackgroundService
    {
        private readonly ILogger<MessageProcessorService> _logger;
        private readonly BoundedMessageChannel _boundedMessageChannel;

        private readonly int _maxTaskInstances;

        public MessageProcessorService(
            ILogger<MessageProcessorService> logger,
            IOptions<MessageProcessingConfig> queueReadingConfig,
            BoundedMessageChannel boundedMessageChannel)
        {
            _logger = logger;
            _maxTaskInstances = queueReadingConfig.Value.MaxConcurrentProcessors;
            _boundedMessageChannel = boundedMessageChannel;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            stoppingToken.Register(() => Log.ProcessorStopping(_logger));

            Log.StartedProcessing(_logger);

            var tasks = Enumerable.Range(1, _maxTaskInstances).Select(ProcessMessages);

            Log.StartedTaskInstances(_logger, _maxTaskInstances);

            await Task.WhenAll(tasks);

            Log.StoppedProcessing(_logger);

            async Task ProcessMessages(int instance)
            {
                var count = 0;

                try
                {
                    // We're not passing cancellation into async method so that we try to drain the channel on shutdown
                    // Generally this should complete before the configured ShutdownTimeout causes an ungaceful shutdown.
                    await foreach (var message in _boundedMessageChannel.ReadAllAsync())
                    {
                        try
                        {
                            // Imagine processing the message here!

                            await Task.Delay(500); // simulate processing work which we won't cancel

                            count++;
                        }
                        catch (Exception ex)
                        {
                            // If errors occur, we will probably send this to a poison queue, allow the message 
                            // to be deleted and continue processing other messages.
                            _logger.ExceptionOccurred(ex);

                            // Note: Assumes no roll back is needed due to partial success for various processing tasks.
                        }

                        // delete the message from the main queue

                        Log.ProcessedMessage(_logger, message.MessageId, instance);
                    }
                }
                catch (Exception ex)
                {
                    // An exception may be thrown if the channel writer completes with an exception
                    _logger.ExceptionOccurred(ex);
                }
                finally
                {
                    Log.StoppedProcessing(_logger, count, instance);
                }
            }
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            var sw = Stopwatch.StartNew();

            await base.StopAsync(cancellationToken);

            Log.MillisecondsToStopProcessing(_logger, sw.ElapsedMilliseconds);
        }

        internal static class EventIds
        {
            public static readonly EventId StartedProcessing = new EventId(100, "StartedProcessing");
            public static readonly EventId ProcessorStopping = new EventId(101, "ProcessorStopping");
            public static readonly EventId StoppedProcessing = new EventId(102, "StoppedProcessing");
            public static readonly EventId StartedTaskInstances = new EventId(103, "StartedTaskInstances");
            public static readonly EventId ProcessedMessage = new EventId(110, "ProcessedMessage");
            public static readonly EventId StopProcessingTimer = new EventId(120, "StopProcessingTimer");
        }

        private static class Log
        {
            private static readonly Action<ILogger, string, int, Exception> _receivedMessagesForInstance = LoggerMessage.Define<string, int>(
                LogLevel.Debug,
                EventIds.ProcessedMessage,
                "Read and processed message with ID '{MessageId}' from the channel in instance {Instance}.");

            private static readonly Action<ILogger, int, int, Exception> _stoppedProcessing = LoggerMessage.Define<int, int>(
                LogLevel.Debug,
                EventIds.StoppedProcessing,
                "Read a total of {TotalMessages} messages in instance {InstanceId}.");

            private static readonly Action<ILogger, int, Exception> _startedInstances = LoggerMessage.Define<int>(
                LogLevel.Debug,
                EventIds.StartedTaskInstances,
                "Starting {InstanceCount} message processing task instances.");

            private static readonly Action<ILogger, long, Exception> _millisecondsToStopProcessing = LoggerMessage.Define<long>(
                LogLevel.Debug,
                EventIds.StopProcessingTimer,
                "Stopped message processor after {Milliseconds} ms.");

            public static void StartedProcessing(ILogger logger)
            {
                if (logger.IsEnabled(LogLevel.Debug))
                {
                    logger.Log(LogLevel.Trace, EventIds.StartedProcessing, "Started message processing service.");
                }
            }

            public static void ProcessorStopping(ILogger logger)
            {
                if (logger.IsEnabled(LogLevel.Information))
                {
                    logger.Log(LogLevel.Information, EventIds.ProcessorStopping, "Message processing stopping due to app termination!");
                }
            }

            public static void StoppedProcessing(ILogger logger)
            {
                if (logger.IsEnabled(LogLevel.Debug))
                {
                    logger.Log(LogLevel.Trace, EventIds.StoppedProcessing, "Stopped message processing service.");
                }
            }

            public static void StoppedProcessing(ILogger logger, int msgCount, int instanceId)
            {
                _stoppedProcessing(logger, msgCount, instanceId, null);
            }

            public static void ProcessedMessage(ILogger logger, string messageId, int instanceId)
            {
                _receivedMessagesForInstance(logger, messageId, instanceId, null);
            }

            public static void StartedTaskInstances(ILogger logger, int instanceCount)
            {
                _startedInstances(logger, instanceCount, null);
            }

            public static void MillisecondsToStopProcessing(ILogger logger, long milliseconds)
            {
                _millisecondsToStopProcessing(logger, milliseconds, null);
            }
        }
    }
}
