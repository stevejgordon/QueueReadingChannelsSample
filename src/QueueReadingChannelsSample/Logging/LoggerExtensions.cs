using System;
using Microsoft.Extensions.Logging;

namespace QueueReadingChannelsSample.Logging
{
    public static class LoggerExtensions
    {
        public static class EventIds
        {
            public static readonly EventId ExceptionCaught = new EventId(001, "ExceptionCaught");
            public static readonly EventId OperationCancelledExceptionCaught = new EventId(002, "OperationCancelledExceptionCaught");
        }

        public static void ExceptionOccurred(this ILogger logger, Exception ex)
        {
            if (logger.IsEnabled(LogLevel.Error))
            {
                logger.Log(LogLevel.Error, EventIds.ExceptionCaught, ex, "An exception occurred and was caught.");
            }
        }
        
        public static void OperationCancelledExceptionOccurred(this ILogger logger)
        {
            if (logger.IsEnabled(LogLevel.Information))
            {
                logger.Log(LogLevel.Information, EventIds.OperationCancelledExceptionCaught, "A task/operation cancelled exception was caught.");
            }
        }
    }
}
