using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using QueueReadingChannelsSample.Sqs;

namespace QueueReadingChannelsSample
{
    public class BoundedMessageChannel
    {
        private const int MaxMessagesInChannel = 250;

        private readonly Channel<Message> _channel;
        private readonly ILogger<BoundedMessageChannel> _logger;

        public BoundedMessageChannel(ILogger<BoundedMessageChannel> logger)
        {
            var options = new BoundedChannelOptions(MaxMessagesInChannel)
            {
                SingleReader = true,
                SingleWriter = true
            };

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
                    Log.ChannelMessageWritten(_logger, messages[index].MessageId);

                    index++;
                }
            }            
        }

        public void CompleteWriter(Exception ex = null) =>_channel.Writer.Complete(ex);

        public bool TryCompleteWriter(Exception ex = null) => _channel.Writer.TryComplete(ex);

        internal static class EventIds
        {
            public static readonly EventId ChannelMessageWritten = new EventId(100, "ChannelMessageWritten");
        }

        private static class Log
        {
            private static readonly Action<ILogger, string, Exception> _channelMessageWritten = LoggerMessage.Define<string>(
                LogLevel.Debug,
                EventIds.ChannelMessageWritten,
                "Message with ID '{MessageId} was written to the channel.");

            public static void ChannelMessageWritten(ILogger logger, string messageId)
            {
                _channelMessageWritten(logger, messageId, null);
            }
        }
    }
}
