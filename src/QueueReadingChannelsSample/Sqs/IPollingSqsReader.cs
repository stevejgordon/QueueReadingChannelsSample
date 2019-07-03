using System.Threading;
using System.Threading.Tasks;

namespace QueueReadingChannelsSample.Sqs
{
    public interface IPollingSqsReader
    {
        Task<Message[]> PollForMessagesAsync(CancellationToken ct = default);
    }
}
