using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using QueueReadingChannelsSample.Sqs;

namespace QueueReadingChannelsSample
{
    public class QueueReaderService : BackgroundService
    {
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
            stoppingToken.Register(() =>
            {
                _logger.LogWarning("Queue reader stopping!");
            });

            _logger.LogInformation("Starting queue reading service.");

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    var messages = await _pollingSqsReader.PollForMessagesAsync(stoppingToken); // may propogate a TaskCanceledException

                    _logger.LogInformation("Read {MessageCount} messages from the queue.", messages.Length);

                    // once we have some messages we won't pass cancellation so we add them to the channel 
                    // and have time to process them even after shutdown
                    await _boundedMessageChannel.WriteMessagesAsync(messages);
                }
            }
            finally // ensure we always complete the writer even if exception such as TaskCanceledException occurs.
            {
                _boundedMessageChannel.CompleteWriter();

                _logger.LogInformation("Completed the queue reading service.");
            }
        }
    }
}
