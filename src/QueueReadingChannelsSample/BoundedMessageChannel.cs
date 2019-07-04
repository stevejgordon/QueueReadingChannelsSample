using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using QueueReadingChannelsSample.Configuration;
using QueueReadingChannelsSample.Sqs;

namespace QueueReadingChannelsSample
{
    public class BoundedMessageChannel
    {
        private readonly Channel<Message> _channel;
        private readonly ILogger<BoundedMessageChannel> _logger;

        public BoundedMessageChannel(ILogger<BoundedMessageChannel> logger, IOptions<MessageChannelConfig> messageChannelConfig)
        {
            var options = new BoundedChannelOptions(messageChannelConfig.Value.MaxBoundedCapacity);

            _channel = Channel.CreateBounded<Message>(options);

            _logger = logger;
        }

        public IAsyncEnumerable<Message> ReadAllAsync(CancellationToken ct = default) => _channel.Reader.ReadAllAsync(ct);
        
        public async Task WriteMessagesAsync(Message[] messages, CancellationToken ct = default)
        {
            var index = 0;

            while (index < messages.Length && await _channel.Writer.WaitToWriteAsync(ct) && !ct.IsCancellationRequested)
            {
                while (index < messages.Length && _channel.Writer.TryWrite(messages[index]))
                {
                    _logger.LogInformation("Message with ID '{MessageId} was written to the channel.", messages[index].MessageId);

                    index++;
                }
            }            
        }

        public void CompleteWriter()
        {
            _channel.Writer.Complete();
        }
    }
}
