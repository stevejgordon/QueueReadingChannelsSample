using System;
using Microsoft.Extensions.Logging;

namespace QueueReadingChannelsSample.Logging
{
    public static class LoggerExtensions
    {
        public static class EventIds
        {
            public static readonly EventId ExceptionCaught = new EventId(001, "ExceptionCaught");
        }

        public static void ExceptionOccurred(this ILogger logger, Exception ex)
        {
            if (logger.IsEnabled(LogLevel.Error))
            {
                logger.Log(LogLevel.Error, EventIds.ExceptionCaught, null, ex, "An exception occurred while receiving messages.");
            }
        }
    }
}
