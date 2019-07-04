using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using QueueReadingChannelsSample.Logging;
using QueueReadingChannelsSample.Sqs;

namespace QueueReadingChannelsSample
{
    /// <summary>
    /// A <see cref="BackgroundService"/> which continually polls for messages whilst the application
    /// is running. Received messages are written to a <see cref="System.Threading.Channels.Channel{T}"/>.
    /// </summary>
    public class QueueReaderService : BackgroundService
    {
        private readonly ILogger<QueueReaderService> _logger;
        private readonly IPollingSqsReader _pollingSqsReader;
        private readonly BoundedMessageChannel _boundedMessageChannel;
        private readonly IHostApplicationLifetime _hostApplicationLifetime;

        public QueueReaderService(
            ILogger<QueueReaderService> logger, 
            IPollingSqsReader pollingSqsReader,
            BoundedMessageChannel boundedMessageChannel,
            IHostApplicationLifetime hostApplicationLifetime)
        {
            _logger = logger;
            _pollingSqsReader = pollingSqsReader;
            _boundedMessageChannel = boundedMessageChannel;
            _hostApplicationLifetime = hostApplicationLifetime;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            stoppingToken.Register(() =>
            {
                Log.ReaderStopping(_logger);
            });

            Log.StartedReading(_logger);

            try
            {
                while (!stoppingToken.IsCancellationRequested) // keep reading while the app is running.
                {
                    try
                    {
                        var messages = await _pollingSqsReader.PollForMessagesAsync(stoppingToken);

                        Log.ReceivedMessages(_logger, messages.Length);

                        // Once we have some messages we won't pass cancellation so we add them to the channel 
                        // and have time to process them even after shutdown if requested.
                        await _boundedMessageChannel.WriteMessagesAsync(messages);
                    }
                    catch (OperationCanceledException)
                    {
                        // Log an swallow as the while loop will end gracefully when cancellation has been requested
                        _logger.OperationCancelledExceptionOccurred();
                    }
                    catch (AmazonSqsException ex) when (ex.Message.Contains(
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
            catch (Exception ex)
            {
                // We shouldn't get here as the task instance while loop should catch all exceptions.
                // We'll include it just in case anything escapes due to future changes.

                _logger.ExceptionOccurred(ex);

                // Complete writer and include the exception which will also throw in the reader.
                _boundedMessageChannel.CompleteWriter(ex);
            }
            finally // Ensure we always complete the writer.
            {
                // Ensure we always complete the writer at shutdown or due to exceptions.
                // This ensures consumers know when there is nothing left to read and never will be.
                _boundedMessageChannel.TryCompleteWriter(); // May have completed in the exception handling.

                Log.StoppedReading(_logger);
            }
        }

        internal static class EventIds
        {
            public static readonly EventId StartedReading = new EventId(100, "StartedReading");
            public static readonly EventId ReaderStopping = new EventId(101, "ReaderStopping");
            public static readonly EventId StoppedReading = new EventId(102, "StoppedReading");
            public static readonly EventId ReceivedMessages = new EventId(110, "ReceivedMessages");
            public static readonly EventId CriticalSqsException = new EventId(120, "CriticalSqsException");
        }

        private static class Log
        {
            private static readonly Action<ILogger, int, Exception> _receivedMessages = LoggerMessage.Define<int>(
                LogLevel.Trace,
                EventIds.ReceivedMessages,
                "Received {MessageCount} messages from the queue.");

            public static void StartedReading(ILogger logger)
            {
                if (logger.IsEnabled(LogLevel.Trace))
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
                if (logger.IsEnabled(LogLevel.Trace))
                {
                    logger.Log(LogLevel.Trace, EventIds.StoppedReading, "Stopped queue reading service.");
                }
            }

            public static void ReceivedMessages(ILogger logger, int messageCount)
            {
                _receivedMessages(logger, messageCount, null);
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
