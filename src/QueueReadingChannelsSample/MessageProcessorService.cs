using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using QueueReadingChannelsSample.Configuration;

namespace QueueReadingChannelsSample
{
    public class MessageProcessorService : BackgroundService
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
            stoppingToken.Register(() => _logger.LogWarning("Message processor stopping!"));

            _logger.LogInformation("Start message processing from the channel.");

            _logger.LogInformation("Starting {InstanceCount} message processing tasks.", _maxTaskInstances);

            var tasks = Enumerable.Range(1, _maxTaskInstances).Select(ProcessMessages);
            await Task.WhenAll(tasks);

            _logger.LogInformation("Finished reading all messages from the channel.");

            async Task ProcessMessages(int instance)
            {
                var count = 0;

                // not passing cancellation into async method so that we try to drain the channel on shutdown
                await foreach (var message in _boundedMessageChannel.ReadAllAsync())
                {
                    try
                    {
                        // process the message here

                        await Task.Delay(500); // simulate processing work which we won't cancel

                        count++;
                    }
                    catch
                    {
                        // if errors occur, we will probably send this to a poison queue
                    }

                    // delete the message from the main queue

                    _logger.LogInformation("Read and processed message with ID '{MessageId}' from the channel in instance {Instance}.", message.MessageId, instance);
                }

                _logger.LogInformation("Finished reading in instance {Instance}.", instance);

                _logger.LogInformation("Read a total of {TotalMessages} messages in instance {Instance}.", count, instance);
            }
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            var sw = Stopwatch.StartNew();

            await base.StopAsync(cancellationToken);

            _logger.LogInformation("Stopped message processor after {Milliseconds} ms.", sw.ElapsedMilliseconds);
        }
    }
}
