using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace QueueReadingChannelsSample
{
    public class MessageProcessorService : BackgroundService
    {
        private readonly ILogger<MessageProcessorService> _logger;
        private readonly BoundedMessageChannel _boundedMessageChannel;

        public MessageProcessorService(ILogger<MessageProcessorService> logger, BoundedMessageChannel boundedMessageChannel)
        {
            _logger = logger;
            _boundedMessageChannel = boundedMessageChannel;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            stoppingToken.Register(() => _logger.LogWarning("Message processor stopping!"));
            
            _logger.LogInformation("Start message processing from the channel");

            // not passing cancellation into async method so that we try to drain the channel on shutdown
            await foreach (var message in _boundedMessageChannel.ReadAllAsync()) 
            {
                // process the message here
                
                await Task.Delay(500); // simulate processing work

                // delete the message

                _logger.LogInformation("Read and processed message with ID '{MessageId}' from the channel", message.MessageId);

            }

            _logger.LogInformation("Finished reading all messages from the channel");
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            var sw = Stopwatch.StartNew();

            await base.StopAsync(cancellationToken);

            _logger.LogInformation("Stopped message processor after {Milliseconds} ms", sw.ElapsedMilliseconds);
        }
    }
}
