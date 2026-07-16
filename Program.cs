using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.Configuration;

// ChipSbManager — finds messages on a Service Bus queue containing a search string,
// previews the match count (non-destructive peek) or deletes matches (with JSONL backup).
//
// Delete strategy: the queue has MaxDeliveryCount = 1, so abandoning a received message
// would cause it to dead-letter on its next delivery. Instead, every message is received
// exactly once and completed: matches are backed up and completed (deleted); non-matches
// are re-sent as a fresh clone (delivery count 0) before the original is completed.
// Clones carry a run-ID property; since the queue is FIFO, receiving a message tagged
// with the current run's ID means every original has been processed, so the pass stops.

const string RunIdProperty = "ChipSbManager.RequeueRunId";
const int BatchSize = 100;

var config = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: true)
    .Build();

var connectionString = config["ServiceBus:ConnectionString"];
var queueName = config["ServiceBus:QueueName"];

var useWebSockets = bool.TryParse(config["ServiceBus:UseWebSockets"], out var ws) && ws;

if (string.IsNullOrWhiteSpace(connectionString) || string.IsNullOrWhiteSpace(queueName)
    || connectionString.Contains("<your-"))
{
    Console.WriteLine("Missing configuration.");
    Console.WriteLine("Copy appsettings.example.json to appsettings.json (next to the .csproj)");
    Console.WriteLine("and fill in ServiceBus:ConnectionString and ServiceBus:QueueName.");
    return 1;
}

var clientOptions = new ServiceBusClientOptions
{
    // AMQP-over-TCP needs outbound port 5671; on locked-down networks (e.g. ExpressRoute
    // with no internet egress) only 443 is open, so tunnel AMQP through WebSockets.
    TransportType = useWebSockets ? ServiceBusTransportType.AmqpWebSockets
                                  : ServiceBusTransportType.AmqpTcp,
    RetryOptions = new ServiceBusRetryOptions { TryTimeout = TimeSpan.FromSeconds(20), MaxRetries = 2 },
};

await using var client = new ServiceBusClient(connectionString, clientOptions);

Console.WriteLine("=== ChipSbManager — Service Bus queue cleaner ===");
Console.WriteLine($"Queue: {queueName} | transport: {clientOptions.TransportType}");
await ShowQueueCountsAsync();
Console.WriteLine();

var searchText = PromptForSearchText();
var comparison = PromptForCaseSensitivity();

while (true)
{
    Console.WriteLine();
    Console.WriteLine($"Search string: \"{searchText}\" ({(comparison == StringComparison.Ordinal ? "case-sensitive" : "case-insensitive")})");
    Console.WriteLine("  [1] Preview — count matching messages (non-destructive)");
    Console.WriteLine("  [2] Delete  — back up & delete matches, re-queue everything else");
    Console.WriteLine("  [3] Change search string");
    Console.WriteLine("  [Q] Quit");
    Console.Write("> ");

    switch (Console.ReadLine()?.Trim().ToLowerInvariant())
    {
        case "1":
            await RunLoggedAsync("preview", PreviewAsync);
            break;
        case "2":
            await RunLoggedAsync("delete", DeleteAsync);
            break;
        case "3":
            searchText = PromptForSearchText();
            comparison = PromptForCaseSensitivity();
            break;
        case "q":
            return 0;
    }
}

bool IsMatch(ServiceBusReceivedMessage message)
{
    string body;
    try
    {
        body = message.Body.ToString();
    }
    catch
    {
        return false; // non-text body can't contain the search string
    }
    return body.Contains(searchText, comparison);
}

async Task PreviewAsync()
{
    await using var receiver = client.CreateReceiver(queueName);

    long scanned = 0, matched = 0;
    long fromSequence = 0;
    var samplesShown = 0;
    var total = await TryGetActiveCountAsync();
    var started = System.Diagnostics.Stopwatch.StartNew();

    Console.WriteLine("Peeking through the queue (messages are not locked or modified)...");

    while (true)
    {
        var batch = await receiver.PeekMessagesAsync(BatchSize, fromSequence);
        if (batch.Count == 0)
            break;

        foreach (var message in batch)
        {
            scanned++;
            if (IsMatch(message))
            {
                matched++;
                if (samplesShown < 3)
                {
                    samplesShown++;
                    var preview = message.Body.ToString();
                    if (preview.Length > 120) preview = preview[..120] + "...";
                    Console.WriteLine();
                    Console.WriteLine($"  sample match (id {message.MessageId}): {preview}");
                }
            }
        }

        fromSequence = batch[^1].SequenceNumber + 1;
        WriteProgress($"  scanned {Progress(scanned, total, started)} | matched {matched:N0}");
    }

    Console.WriteLine();
    Console.WriteLine($"Done in {started.Elapsed:mm\\:ss}. Scanned {scanned:N0} messages; {matched:N0} contain \"{searchText}\".");
}

async Task DeleteAsync()
{
    Console.WriteLine();
    Console.WriteLine($"This will DELETE every message containing \"{searchText}\" from '{queueName}'.");
    Console.WriteLine("Matches are backed up to a local file first. All other messages are");
    Console.WriteLine("re-queued as fresh copies (new enqueue time, back of the queue).");
    Console.WriteLine("Make sure consumers of this queue are paused before continuing.");
    Console.Write("Type DELETE to proceed: ");
    if (Console.ReadLine()?.Trim() != "DELETE")
    {
        Console.WriteLine("Cancelled.");
        return;
    }

    Console.Write("Max messages to process this run (Enter = all): ");
    var limitInput = Console.ReadLine()?.Trim();
    long? limit = long.TryParse(limitInput, out var lim) && lim > 0 ? lim : null;

    Directory.CreateDirectory("backups");
    var backupPath = Path.Combine("backups", $"{queueName}-{DateTime.Now:yyyyMMdd-HHmmss}.jsonl");
    await using var backup = new StreamWriter(backupPath);

    var runId = Guid.NewGuid().ToString("N");
    await using var receiver = client.CreateReceiver(queueName); // default PeekLock
    await using var sender = client.CreateSender(queueName);

    long scanned = 0, deleted = 0, requeued = 0;
    var reachedOwnClones = false;
    var activeCount = await TryGetActiveCountAsync();
    var total = limit is null ? activeCount : Math.Min(limit.Value, activeCount ?? limit.Value);
    var started = System.Diagnostics.Stopwatch.StartNew();

    Console.WriteLine($"Backing up matches to {backupPath}");
    Console.WriteLine(limit is null ? "Processing entire queue..." : $"Processing up to {limit:N0} messages...");

    while (!reachedOwnClones && (limit is null || scanned < limit))
    {
        var batch = await receiver.ReceiveMessagesAsync(BatchSize, TimeSpan.FromSeconds(5));
        if (batch.Count == 0)
            break; // queue drained

        // Split the batch: matches get backed up + deleted, everything else is
        // re-queued as a fresh clone (delivery count resets to 0). A message tagged
        // with this run's ID is one of our own clones — FIFO order means every
        // original has now been seen, so this is the last batch.
        var matches = new List<ServiceBusReceivedMessage>();
        var others = new List<ServiceBusReceivedMessage>();
        foreach (var message in batch)
        {
            var isOwnClone = message.ApplicationProperties.TryGetValue(RunIdProperty, out var tag)
                             && (string?)tag == runId;
            reachedOwnClones |= isOwnClone;
            if (!isOwnClone && IsMatch(message)) matches.Add(message);
            else others.Add(message);
        }

        foreach (var message in matches)
        {
            await backup.WriteLineAsync(JsonSerializer.Serialize(new
            {
                message.MessageId,
                message.SequenceNumber,
                message.EnqueuedTime,
                message.Subject,
                message.ContentType,
                message.CorrelationId,
                ApplicationProperties = message.ApplicationProperties
                    .ToDictionary(p => p.Key, p => p.Value?.ToString()),
                Body = message.Body.ToString(),
            }));
        }
        await backup.FlushAsync();

        // Send all clones before completing anything: a crash mid-batch can at
        // worst duplicate messages, never lose one.
        if (others.Count > 0)
        {
            var messageBatch = await sender.CreateMessageBatchAsync();
            foreach (var message in others)
            {
                var clone = new ServiceBusMessage(message);
                clone.ApplicationProperties[RunIdProperty] = runId;
                if (!messageBatch.TryAddMessage(clone))
                {
                    await sender.SendMessagesAsync(messageBatch);
                    messageBatch.Dispose();
                    messageBatch = await sender.CreateMessageBatchAsync();
                    if (!messageBatch.TryAddMessage(clone))
                        await sender.SendMessageAsync(clone); // single oversized message
                }
            }
            if (messageBatch.Count > 0)
                await sender.SendMessagesAsync(messageBatch);
            messageBatch.Dispose();
        }

        await Task.WhenAll(batch.Select(m => receiver.CompleteMessageAsync(m)));

        deleted += matches.Count;
        requeued += others.Count;
        scanned += batch.Count;
        WriteProgress($"  processed {Progress(scanned, total, started)} | deleted {deleted:N0} | re-queued {requeued:N0}");
    }

    Console.WriteLine();
    Console.WriteLine($"Done in {started.Elapsed:mm\\:ss}. Processed {scanned:N0} messages: deleted {deleted:N0}, re-queued {requeued:N0}.");
    if (limit is not null && scanned >= limit && !reachedOwnClones)
        Console.WriteLine("Stopped at the run limit — run Delete again to continue where this left off.");
    Console.WriteLine(deleted > 0
        ? $"Backup of deleted messages: {backupPath}"
        : "No messages matched; backup file is empty.");
    Console.WriteLine("Note: re-queued clones carry an extra application property " +
                      $"'{RunIdProperty}' and a new enqueue time/sequence number.");
    await ShowQueueCountsAsync();
}

async Task RunLoggedAsync(string operation, Func<Task> action)
{
    try
    {
        await action();
    }
    catch (Exception ex)
    {
        Console.WriteLine();
        Console.WriteLine($"ERROR during {operation}:");
        for (var e = ex; e is not null; e = e.InnerException)
        {
            Console.WriteLine($"  [{e.GetType().Name}] {e.Message}");
            if (e is ServiceBusException sbe)
                Console.WriteLine($"    reason: {sbe.Reason}, transient: {sbe.IsTransient}");
        }

        if (ex is ServiceBusException { Reason: ServiceBusFailureReason.ServiceCommunicationProblem }
            or ServiceBusException { Reason: ServiceBusFailureReason.ServiceTimeout }
            or OperationCanceledException)
        {
            Console.WriteLine();
            Console.WriteLine("This looks like a network/connectivity problem. The default AMQP transport");
            Console.WriteLine("needs outbound TCP port 5671, which is often blocked on restricted networks.");
            Console.WriteLine("Try setting \"ServiceBus\": { \"UseWebSockets\": true } in appsettings.json");
            Console.WriteLine("to tunnel AMQP over port 443 instead (the port the queue counts already use).");
        }
    }
}

async Task ShowQueueCountsAsync()
{
    try
    {
        var admin = new ServiceBusAdministrationClient(connectionString);
        var props = (await admin.GetQueueRuntimePropertiesAsync(queueName)).Value;
        Console.WriteLine($"Active messages: {props.ActiveMessageCount:N0} | dead-lettered: {props.DeadLetterMessageCount:N0} | scheduled: {props.ScheduledMessageCount:N0}");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"(Could not read queue counts — connection string may lack Manage rights: {ex.Message})");
    }
}

async Task<long?> TryGetActiveCountAsync()
{
    try
    {
        var admin = new ServiceBusAdministrationClient(connectionString);
        var props = (await admin.GetQueueRuntimePropertiesAsync(queueName)).Value;
        return props.ActiveMessageCount;
    }
    catch
    {
        return null; // no Manage rights — progress shows counts without percentage/ETA
    }
}

string Progress(long done, long? total, System.Diagnostics.Stopwatch started)
{
    var rate = done / Math.Max(started.Elapsed.TotalSeconds, 0.001);
    var text = total is > 0
        ? $"{done:N0} / ~{total:N0} ({Math.Min(100, done * 100 / total.Value)}%)"
        : $"{done:N0}";
    text += $" | {rate:N0}/s";
    if (total is > 0 && rate > 0 && done < total)
    {
        var eta = TimeSpan.FromSeconds((total.Value - done) / rate);
        text += $" | ETA {eta:mm\\:ss}";
    }
    return text;
}

void WriteProgress(string text)
{
    // Rewrite a single console line in place; pad to erase leftovers from longer lines.
    Console.Write($"\r{text,-90}");
}

string PromptForSearchText()
{
    while (true)
    {
        Console.Write("Text to search for in message bodies: ");
        var input = Console.ReadLine();
        if (!string.IsNullOrEmpty(input))
            return input;
    }
}

StringComparison PromptForCaseSensitivity()
{
    Console.Write("Case-sensitive match? [y/N] ");
    return Console.ReadLine()?.Trim().ToLowerInvariant() == "y"
        ? StringComparison.Ordinal
        : StringComparison.OrdinalIgnoreCase;
}
