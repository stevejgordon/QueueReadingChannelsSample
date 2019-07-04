using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using QueueReadingChannelsSample.Sqs;

namespace QueueReadingChannelsSample
{
    public class QueueReaderService : BackgroundService
    {
        const int MaxTaskInstances = 5;
        private readonly ILogger<QueueReaderService> _logger;
        private readonly IPollingSqsReader _pollingSqsReader;
        private readonly BoundedMessageChannel _boundedMessageChannel;

        public QueueReaderService(ILogger<QueueReaderService> logger, IPollingSqsReader pollingSqsReader, BoundedMessageChannel boundedMessageChannel)
        {
            _logger = logger;
            _pollingSqsReader = pollingSqsReader;
            _boundedMessageChannel = boundedMessageChannel;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            stoppingToken.Register(() => _logger.LogWarning("Queue reader stopping!"));

            _logger.LogInformation("Starting queue reading service.");

            try
            {
                var tasks = Enumerable.Range(1, MaxTaskInstances).Select(x => PollForAndWriteMessages(x));
                await Task.WhenAll(tasks);
            }
            catch (OperationCanceledException)
            {
                // swallow as nothing needs to know if the operation was cancelled in this background service;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "An exception occurred during queue reading");
            }
            finally // ensure we always complete the writer even if exception occurs.
            {
                _boundedMessageChannel.CompleteWriter();
                _logger.LogInformation("Completed the queue reading service.");
            }

            async Task PollForAndWriteMessages(int instance)
            {
                var count = 0;

                try
                {
                    while (!stoppingToken.IsCancellationRequested)
                    {
                        // safe to cancel at this point if the polling reader has not yet received messages
                        var messages = await _pollingSqsReader.PollForMessagesAsync(stoppingToken);

                        _logger.LogInformation("Read {MessageCount} messages from the queue in task instance {Instance}.", messages.Length, instance);

                        // once we have some messages we won't pass cancellation so we add them to the channel and have time to process them during shutdown
                        await _boundedMessageChannel.WriteMessagesAsync(messages);

                        count += messages.Length;
                    }
                }
                finally
                {
                    _logger.LogInformation("Finished writing in instance {Instance}. Wrote {TotalMessages} msgs.", count, instance);
                }
            }
        }
    }
}
