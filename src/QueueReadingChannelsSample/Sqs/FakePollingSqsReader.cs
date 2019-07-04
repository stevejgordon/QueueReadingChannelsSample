using System;
using System.Threading;
using System.Threading.Tasks;

namespace QueueReadingChannelsSample.Sqs
{
    public class FakePollingSqsReader : IPollingSqsReader
    {
        private static int _seed = Environment.TickCount;
        private static readonly ThreadLocal<Random> Random = new ThreadLocal<Random>(() => new Random(Interlocked.Increment(ref _seed)));

        public async Task<Message[]> PollForMessagesAsync(CancellationToken ct = default)
        {
            if (Random.Value.Next(1, 20) == 1) // simulate 5% chance of an exception (which is high but let's us see the effect).
            {
                await Task.Delay(50, ct); // Simulate slight delay for exception.

                throw new AmazonSqsException();
            }

            await Task.Delay(Random.Value.Next(1000, 5000), ct); // Simulate waiting for some messages

            var count = Random.Value.Next(1, 10); // Simulate variable amount of messages received from the queue

            var messages = new Message[count];

            for (var i = 0; i < count; i++)
            {
                messages[i] = new Message { MessageId = Guid.NewGuid().ToString(), Body = "{\"test\":\"data\"}" };
            }

            return messages;
        }
    }
}
