using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Identity;
using Azure.Messaging.ServiceBus;
using CommandLine;

return await Parser.Default.ParseArguments<PeekOptions, AddOptions>(args)
    .MapResult(
        (PeekOptions opts) => HandlePeek(opts),
        (AddOptions opts) => HandleAdd(opts),
        _ => Task.FromResult(1));

static async Task<int> HandleAdd(AddOptions options)
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
        var credential = new ChainedTokenCredential(
            new AzureCliCredential(),
            new InteractiveBrowserCredential(),
            new VisualStudioCredential(),
            new AzurePowerShellCredential());

        var split = options.Destination.Split("/", count: 2);
        if (split.Length != 2)
        {
            return 1;
        }

        var serviceBusClient = new ServiceBusClient(split[0], credential);
        var sender = serviceBusClient.CreateSender(split[1]);


        var messages = new Queue<ServiceBusMessage>();

        await using var stdIn = Console.OpenStandardInput();
        using var reader = new StreamReader(stdIn, Encoding.UTF8);
        while (true)
        {
            var line = await reader.ReadLineAsync(cts.Token);
            if (string.IsNullOrEmpty(line))
            {
                break;
            }
            var visibilityTimeout =
                options.VisibilityTimeout is {} visibilityTimeoutOptions
                    ? (TimeSpan?)TimeSpan.FromSeconds(messages.Count * visibilityTimeoutOptions)
                    : null;

            var msg = new ServiceBusMessage(BinaryData.FromString(line));
            if (visibilityTimeout is not null)
            {
                msg.ScheduledEnqueueTime = DateTimeOffset.UtcNow.Add(visibilityTimeout.Value);
            }

            messages.Enqueue(msg);
        }

        var messageCount = messages.Count;

        // Copied from: https://learn.microsoft.com/en-us/dotnet/api/overview/azure/messaging.servicebus-readme?view=azure-dotnet#sending-a-batch-of-messages
        while (messages.Count > 0)
        {
            // start a new batch
            using var messageBatch = await sender.CreateMessageBatchAsync(cts.Token);

            // add the first message to the batch
            if (messageBatch.TryAddMessage(messages.Peek()))
            {
                // dequeue the message from the .NET queue once the message is added to the batch
                messages.Dequeue();
            }
            else
            {
                // if the first message can't fit, then it is too large for the batch
                throw new Exception($"Message {messageCount - messages.Count} is too large and cannot be sent.");
            }

            // add as many messages as possible to the current batch
            while (messages.Count > 0 && messageBatch.TryAddMessage(messages.Peek()))
            {
                // dequeue the message from the .NET queue as it has been added to the batch
                messages.Dequeue();
            }

            // now, send the batch
            if (options.DryRun)
            {
                Console.WriteLine($"Would've sent {messageBatch.Count} messages to {sender.FullyQualifiedNamespace}/{sender.EntityPath}.");
            }
            else
            {
                await sender.SendMessagesAsync(messageBatch, cts.Token);
            }

            // if there are any remaining messages in the .NET queue, the while loop repeats
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

    // dotnet run -- add --dry-run 'sats-test-eun-servicebus-standard.servicebus.windows.net/activities'

    try
    {
        var credential = new ChainedTokenCredential(
            new AzureCliCredential(),
            new InteractiveBrowserCredential(),
            new VisualStudioCredential(),
            new AzurePowerShellCredential());

        ServiceBusReceiver receiver;
        var split = options.Source.Split("/");
        if (split.Length == 3)
        {
            var serviceBusClient = new ServiceBusClient(split[0], credential);
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

            var serviceBusClient = new ServiceBusClient(split[0], credential);
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

[Verb("add", HelpText = "Adds messages from stdin to a service bus queue/topic.")]
class AddOptions
{
    [Value(0, MetaName = "destination", HelpText = "The name of the queue/topic to which the messages will be added.", Required = true)]
    public required string Destination { get; init; }

    [Option('t', "visibility-timeout-factor", HelpText = "This parameter decided the visibilityTimeout that is set to each messages passed from stdin, visibilityTimeout = visibility-timeout-factor * msgCount (in seconds).", Required = false)]
    public double? VisibilityTimeout { get; init; }

    [Option("dry-run", HelpText = "This parameter decides if the message will be added to the queue or just printed to stdout (default: false).", Required = false)]
    public bool DryRun { get; init; }
}

[Verb("peek", HelpText = "Peek messages from a queue or topic subscription.")]
class PeekOptions
{
    [Value(0, MetaName = "src", HelpText = "The name of the source queue from which the messages will be peeked from.", Required = true)]
    public required string Source { get; init; }

    [Option("messages", HelpText = "The number of messages to peek.", Required = false)]
    public int NumberOfMessages { get; init; } = 32;

    [Option("delete", HelpText = "If set, the messages are removed from the queue.", Required = false)]
    public bool? Delete { get; init; }
}
