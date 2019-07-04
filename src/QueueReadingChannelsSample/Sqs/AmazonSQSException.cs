using System;

namespace QueueReadingChannelsSample.Sqs
{
    public class AmazonSqsException : Exception
    {
        public AmazonSqsException() : base("The security token included in the request is expired")
        {
        }
    }
}
