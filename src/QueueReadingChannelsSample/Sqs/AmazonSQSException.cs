using System;

namespace QueueReadingChannelsSample.Sqs
{
    public class AmazonSQSException : Exception
    {
        public AmazonSQSException() : base("The security token included in the request is expired")
        {
        }
    }
}
