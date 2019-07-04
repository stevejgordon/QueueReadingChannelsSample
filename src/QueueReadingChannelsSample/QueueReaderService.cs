using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using QueueReadingChannelsSample.Configuration;
using QueueReadingChannelsSample.Logging;
using QueueReadingChannelsSample.Sqs;

namespace QueueReadingChannelsSample
{
    public sealed class QueueReaderService : BackgroundService
    {
        private readonly ILogger<QueueReaderService> _logger;
        private readonly IPollingSqsReader _pollingSqsReader;
        private readonly BoundedMessageChannel _boundedMessageChannel;
        private readonly IHostApplicationLifetime _hostApplicationLifetime;

        private readonly int _maxTaskInstances;

        public QueueReaderService(
            ILogger<QueueReaderService> logger,
            IOptions<QueueReaderConfig> queueReadingConfig,
            IPollingSqsReader pollingSqsReader,
            BoundedMessageChannel boundedMessageChannel,
            IHostApplicationLifetime hostApplicationLifetime)
        {
            _logger = logger;
            _maxTaskInstances = queueReadingConfig.Value.MaxConcurrentReaders;
            _pollingSqsReader = pollingSqsReader;
            _boundedMessageChannel = boundedMessageChannel;
            _hostApplicationLifetime = hostApplicationLifetime;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            stoppingToken.Register(() => Log.ReaderStopping(_logger));

            Log.StartedReading(_logger);

            try
            {
                var tasks = Enumerable.Range(1, _maxTaskInstances).Select(PollForAndWriteMessages);

                Log.StartedTaskInstances(_logger, _maxTaskInstances);

                await Task.WhenAll(tasks);
            }
            catch (Exception ex)
            {
                // We shouldn't get here as the task instance while loop should catch all exceptions.
                // We'll include it just in case anything escapes due to future changes.

                _logger.ExceptionOccurred(ex);

                // Complete writer and include the exception which will also throw in the reader.
                _boundedMessageChannel.CompleteWriter(ex);
            }
            finally
            {
                // Ensure we always complete the writer at shutdown or due to exceptions.
                // This ensures consumers know when there is nothing left to read and never will be.
                _boundedMessageChannel.TryCompleteWriter(); // May have completed in the exception handling.

                Log.StoppedReading(_logger);
            }

            async Task PollForAndWriteMessages(int instance)
            {
                var count = 0;

                try
                {
                    while (!stoppingToken.IsCancellationRequested)
                    {
                        try
                        {
                            var messages = await _pollingSqsReader.PollForMessagesAsync(stoppingToken);

                            Log.ReceivedMessages(_logger, messages.Length, instance);

                            // Once we have some messages we won't pass cancellation so we add them to the channel 
                            // and have time to process them even after shutdown if requested.
                            await _boundedMessageChannel.WriteMessagesAsync(messages);

                            count += messages.Length;
                        }
                        catch (OperationCanceledException)
                        {
                            // Log an swallow as the while loop will end gracefully when cancellation has been requested
                            _logger.OperationCancelledExceptionOccurred();
                        }
                        catch (AmazonSQSException ex) when (ex.Message.Contains(
                            "The security token included in the request is expired"))
                        {
                            Log.SqsAuthException(_logger);
                            _hostApplicationLifetime.StopApplication(); // we can't authenticate so stop entirely
                        }
                        catch (Exception ex)
                        {
                            // Log and swallows all other exceptions so the read loop continues.
                            _logger.ExceptionOccurred(ex);
                        }
                    }
                }
                finally
                {
                    Log.StoppedReading(_logger, instance, count);
                }
            }
        }

        internal static class EventIds
        {
            public static readonly EventId StartedReading = new EventId(100, "StartedReading");
            public static readonly EventId ReaderStopping = new EventId(101, "ReaderStopping");
            public static readonly EventId StoppedReading = new EventId(102, "StoppedReading");
            public static readonly EventId StartedTaskInstances = new EventId(103, "StartedTaskInstances");
            public static readonly EventId ReceivedMessages = new EventId(110, "ReceivedMessages");
            public static readonly EventId CriticalSqsException = new EventId(120, "CriticalSqsException");
        }

        private static class Log
        {
            private static readonly Action<ILogger, int, int, Exception> _receivedMessagesForInstance = LoggerMessage.Define<int, int>(
                LogLevel.Debug,
                EventIds.ReceivedMessages,
                "Received {MessageCount} messages from the queue in instance {InstanceId}.");

            private static readonly Action<ILogger, int, int, Exception> _stoppedReading = LoggerMessage.Define<int, int>(
                LogLevel.Debug,
                EventIds.StoppedReading,
                "Stopped queue reading service for instance {InstanceId}. Received {MessageCount} messages.");

            private static readonly Action<ILogger, int, Exception> _startedInstances = LoggerMessage.Define<int>(
                LogLevel.Debug,
                EventIds.StartedTaskInstances,
                "Starting {InstanceCount} queue reading task instances.");

            public static void StartedReading(ILogger logger)
            {
                if (logger.IsEnabled(LogLevel.Debug))
                {
                    logger.Log(LogLevel.Trace, EventIds.StartedReading, "Started queue reading service.");
                }
            }

            public static void ReaderStopping(ILogger logger)
            {
                if (logger.IsEnabled(LogLevel.Information))
                {
                    logger.Log(LogLevel.Information, EventIds.ReaderStopping, "Queue reader stopping due to app termination!");
                }
            }

            public static void StoppedReading(ILogger logger)
            {
                if (logger.IsEnabled(LogLevel.Debug))
                {
                    logger.Log(LogLevel.Trace, EventIds.StoppedReading, "Stopped queue reading service.");
                }
            }

            public static void StoppedReading(ILogger logger, int instanceId,  int msgCount)
            {
                _stoppedReading(logger, instanceId, msgCount, null);
            }

            public static void ReceivedMessages(ILogger logger, int messageCount, int instanceId)
            {
                _receivedMessagesForInstance(logger, messageCount, instanceId, null);
            }

            public static void StartedTaskInstances(ILogger logger, int instanceCount)
            {
                _startedInstances(logger, instanceCount, null);
            }

            public static void SqsAuthException(ILogger logger)
            {
                if (logger.IsEnabled(LogLevel.Critical))
                {
                    logger.Log(LogLevel.Critical, EventIds.CriticalSqsException, "Unable to authenticate with AWS SQS. Stopping application!");
                }
            }
        }
    }
}
