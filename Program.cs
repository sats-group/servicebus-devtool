using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Azure.Identity;
using Azure.Messaging.ServiceBus;
using CommandLine;

return await Parser.Default.ParseArguments<PeekOptions, object>(args)
    .MapResult(
        (PeekOptions opts) => HandlePeek(opts),
        _ => Task.FromResult(1));

static async Task<int> HandlePeek(PeekOptions options)
{
    var cts = new CancellationTokenSource();
    Console.CancelKeyPress += async (_, e) =>
    {
        await using var stdErr = Console.OpenStandardError();
        await using var stdErrWriter = new StreamWriter(stdErr);
        await stdErrWriter.WriteLineAsync("Stopping... (ctrl-c)");
        await stdErrWriter.FlushAsync();
        cts.Cancel();
        e.Cancel = true;
    };

    try
    {
        var azureCliCredentials = new AzureCliCredential();

        ServiceBusReceiver receiver;
        var split = options.Source.Split("/");
        if (split.Length == 3)
        {
            var serviceBusClient = new ServiceBusClient(split[0], azureCliCredentials);
            receiver = serviceBusClient.CreateReceiver(
                split[1],
                split[2],
                new ServiceBusReceiverOptions()
                {
                    ReceiveMode = options.Delete == true ? ServiceBusReceiveMode.ReceiveAndDelete : ServiceBusReceiveMode.PeekLock,
                });
        }
        else if (split.Length == 4)
        {
            if (split[3] != "$deadletterqueue")
            {
                return 1;
            }

            var serviceBusClient = new ServiceBusClient(split[0], azureCliCredentials);
            receiver = serviceBusClient.CreateReceiver(
                split[1],
                $"{split[2]}/{split[3]}",
                new ServiceBusReceiverOptions()
                {
                    ReceiveMode = options.Delete == true ? ServiceBusReceiveMode.ReceiveAndDelete : ServiceBusReceiveMode.PeekLock,
                });
        }
        else
        {
            return 1;
        }

        int batch = 100;
        int count = 0;
        long? sequenceNumber = null;

        while (count < options.NumberOfMessages)
        {
            var maxMessages = Math.Min(
                batch + (sequenceNumber is null ? 0 : 1),
                options.NumberOfMessages - count);
            var msgs =
                options.Delete == true
                    ? await receiver.ReceiveMessagesAsync(
                        maxMessages: maxMessages,
                        maxWaitTime: TimeSpan.FromSeconds(1),
                        cancellationToken: cts.Token)
                    : await receiver.PeekMessagesAsync(
                        fromSequenceNumber: sequenceNumber,
                        maxMessages: maxMessages,
                        cancellationToken: cts.Token);

            if (msgs.Count == 0)
            {
                return 0;
            }

            await using var stdOut = Console.OpenStandardOutput();
            await using var stdOutWriter = new StreamWriter(stdOut);

            foreach (var receivedMessage in msgs)
            {
                await stdOutWriter.WriteLineAsync($"{receivedMessage.EnqueuedTime:O},{receivedMessage.Body}");
                await stdOutWriter.FlushAsync();
                count++;
                sequenceNumber = receivedMessage.SequenceNumber + 1;
            }
        }

        return 0;
    }
    catch (Exception e)
    {
        await using var stdErr = Console.OpenStandardError();
        await using var stdErrWriter = new StreamWriter(stdErr);
        await stdErrWriter.WriteLineAsync(e.Message);
        await stdErrWriter.WriteLineAsync(e.StackTrace);
        return 1;
    }
}


[Verb("peek", HelpText = "Peek messages from a queue or topic subscription.")]
class PeekOptions
{
    public PeekOptions(
        string source,
        int? numberOfMessages,
        bool? delete)
    {
        Source = source;
        Delete = delete;
        NumberOfMessages = numberOfMessages != 0 ? numberOfMessages ?? 32 : 32;
    }

    [Value(0, MetaName = "src", HelpText = "The name of the source queue from which the messages will be peeked from.", Required = true)]
    public string Source { get; }

    [Option("messages", HelpText = "The number of messages to peek.", Required = false)]
    public int NumberOfMessages { get; }

    [Option("delete", HelpText = "If set, the messages are removed from the queue.", Required = false)]
    public bool? Delete { get; }
}
