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

        public QueueReaderService(
            ILogger<QueueReaderService> logger, 
            IPollingSqsReader pollingSqsReader,
            BoundedMessageChannel boundedMessageChannel)
        {
            _logger = logger;
            _pollingSqsReader = pollingSqsReader;
            _boundedMessageChannel = boundedMessageChannel;
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
                    catch (Exception ex)
                    {
                        // This example logs and swallows all exceptions so the read loop continues in the 
                        // event of an exception. We may want to conditionally re-throw and break from the loop,
                        // or potentially, trigger application shutdown entirely for some critical exceptions.

                        _logger.ExceptionOccurred(ex);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.ExceptionOccurred(ex);
                _boundedMessageChannel.CompleteWriter(ex);
            }
            finally // Ensure we always complete the writer.
            {
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
        }
    }
}
