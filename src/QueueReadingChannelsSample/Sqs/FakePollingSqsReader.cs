using System;
using System.Threading;
using System.Threading.Tasks;

namespace QueueReadingChannelsSample.Sqs
{
    public class FakePollingSqsReader : IPollingSqsReader
    {
        private static int seed = Environment.TickCount;
        private static readonly ThreadLocal<Random> random = new ThreadLocal<Random>(() => new Random(Interlocked.Increment(ref seed)));

        public async Task<Message[]> PollForMessagesAsync(CancellationToken ct = default)
        {
            await Task.Delay(random.Value.Next(1000, 5000), ct); // simulate waiting for some messages

            var count = random.Value.Next(1, 10); // simulate variable amount of messages available on the queue

            var messages = new Message[count];

            for (int i = 0; i < count; i++)
            {
                messages[i] = new Message { MessageId = Guid.NewGuid().ToString(), Body = "{\"test\":\"data\"}" };
            }

            return messages;
        }
    }
}
