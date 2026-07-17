using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
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
//
// A pass ends when receives return only this pass's own clones (or nothing) — but that
// heuristic can end early: partitioned queues don't deliver strictly FIFO, and deferred/
// scheduled messages are invisible to a normal receive. So after each pass the queue is
// re-peeked (read-only) and delete only reports success once zero matches remain, running
// further passes if needed. Deferred matches are deleted via ReceiveDeferredMessages and
// scheduled matches via CancelScheduledMessage.

const string RunIdProperty = "ChipSbManager.RequeueRunId";
const int BatchSize = 100;
const int MaxDeletePasses = 5;

var config = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: true)
    .Build();

var useWebSockets = bool.TryParse(config["ServiceBus:UseWebSockets"], out var ws) && ws;

var queues = LoadQueueEntries();
if (queues.Count == 0)
{
    Console.WriteLine("Missing configuration.");
    Console.WriteLine("Copy appsettings.example.json to appsettings.json (next to the .csproj)");
    Console.WriteLine("and fill in the ServiceBus:Queues array (QueueName + ConnectionString per entry).");
    Console.WriteLine("The older single ServiceBus:ConnectionString/QueueName settings also still work.");
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

Console.WriteLine("=== ChipSbManager — Service Bus queue cleaner ===");

ServiceBusClient client = null!;
var connectionString = "";
var queueName = "";
await SelectQueueAsync();

// Matches "ItemId":<digits>, in message bodies — the value is always followed by
// a comma, so require it. Whitespace around the colon and key casing are tolerated.
// The comma (and greedy \d+) make matching boundary-safe: ID 449594 can never
// partially match "ItemId":4495945,
var itemIdRegex = new Regex("\"ItemId\"\\s*:\\s*(\\d+)\\s*,", RegexOptions.IgnoreCase | RegexOptions.Compiled);

var searchText = "";
var comparison = StringComparison.OrdinalIgnoreCase;
HashSet<long>? itemIds = null; // non-null → CSV item-ID filter is active
var filterDescription = "(none — choose option 1 or 2 first)";

while (true)
{
    Console.WriteLine();
    Console.WriteLine($"Queue: {queueName} | Filter: {filterDescription}");
    Console.WriteLine("  [1] Set filter: search text");
    Console.WriteLine("  [2] Set filter: CSV of item IDs (matches \"ItemId\":<id> in bodies)");
    Console.WriteLine("  [3] Preview — count matching messages (non-destructive)");
    Console.WriteLine("  [4] Delete  — back up & delete matches, re-queue everything else");
    Console.WriteLine("  [5] Watch queue counts (refreshes every 10s)");
    Console.WriteLine("  [6] Switch queue");
    Console.WriteLine("  [7] Analyze queue — why so many messages? (non-destructive)");
    Console.WriteLine("  [Q] Quit");
    Console.Write("> ");

    switch (Console.ReadLine()?.Trim().ToLowerInvariant())
    {
        case "1":
            SetTextFilter();
            break;
        case "2":
            SetCsvFilter();
            break;
        case "3":
            if (FilterIsSet())
                await RunLoggedAsync("preview", PreviewAsync);
            break;
        case "4":
            if (FilterIsSet())
                await RunLoggedAsync("delete", DeleteAsync);
            break;
        case "5":
            await RunLoggedAsync("watch", WatchCountsAsync);
            break;
        case "6":
            await SelectQueueAsync();
            break;
        case "7":
            await RunLoggedAsync("analyze", AnalyzeQueueAsync);
            break;
        case "q":
            await client.DisposeAsync();
            return 0;
    }
}

List<QueueEntry> LoadQueueEntries()
{
    var list = new List<QueueEntry>();

    foreach (var section in config.GetSection("ServiceBus:Queues").GetChildren())
        AddIfValid(section["QueueName"], section["ConnectionString"]);

    // The original single-queue settings still work alongside the array.
    AddIfValid(config["ServiceBus:QueueName"], config["ServiceBus:ConnectionString"]);

    return list;

    void AddIfValid(string? name, string? cs)
    {
        if (string.IsNullOrWhiteSpace(name) || string.IsNullOrWhiteSpace(cs)
            || cs.Contains("<your-") || name.Contains('<'))
            return;
        var ns = Regex.Match(cs, "Endpoint=sb://([^/;]+)", RegexOptions.IgnoreCase);
        list.Add(new QueueEntry(name, cs, ns.Success ? ns.Groups[1].Value : "unknown namespace"));
    }
}

async Task SelectQueueAsync()
{
    QueueEntry entry;
    if (queues.Count == 1)
    {
        entry = queues[0];
    }
    else
    {
        Console.WriteLine();
        Console.WriteLine("Configured queues:");
        for (var i = 0; i < queues.Count; i++)
            Console.WriteLine($"  [{i + 1}] {queues[i].QueueName}  ({queues[i].Namespace})");

        while (true)
        {
            Console.Write($"Pick a queue [1-{queues.Count}]: ");
            if (int.TryParse(Console.ReadLine()?.Trim(), out var choice)
                && choice >= 1 && choice <= queues.Count)
            {
                entry = queues[choice - 1];
                break;
            }
        }
    }

    if (client is not null)
        await client.DisposeAsync();
    connectionString = entry.ConnectionString;
    queueName = entry.QueueName;
    client = new ServiceBusClient(connectionString, clientOptions);

    Console.WriteLine($"Queue: {queueName} ({entry.Namespace}) | transport: {clientOptions.TransportType}");
    await ShowQueueCountsAsync();
}

bool FilterIsSet()
{
    if (itemIds is not null || searchText.Length > 0)
        return true;
    Console.WriteLine("No filter set yet — choose [1] to search by text or [2] to load a CSV of item IDs.");
    return false;
}

void SetTextFilter()
{
    searchText = PromptForSearchText();
    comparison = PromptForCaseSensitivity();
    itemIds = null;
    filterDescription = $"bodies containing \"{searchText}\" ({(comparison == StringComparison.Ordinal ? "case-sensitive" : "case-insensitive")})";
}

void SetCsvFilter()
{
    Console.Write("Path to CSV file [itemIds.csv]: ");
    var path = Console.ReadLine()?.Trim();
    if (string.IsNullOrEmpty(path))
        path = "itemIds.csv";
    if (!File.Exists(path))
    {
        Console.WriteLine($"File not found: {Path.GetFullPath(path)} — filter unchanged.");
        return;
    }

    var ids = new HashSet<long>();
    long skipped = 0;
    foreach (var line in File.ReadLines(path))
    {
        var cell = line.Split(',')[0].Trim().Trim('"');
        if (cell.Length == 0)
            continue;
        if (long.TryParse(cell, out var id))
            ids.Add(id);
        else
            skipped++; // e.g. the "Id" header row
    }

    if (ids.Count == 0)
    {
        Console.WriteLine("No numeric IDs found in the file — filter unchanged.");
        return;
    }

    itemIds = ids;
    filterDescription = $"bodies where \"ItemId\":<id> matches one of {ids.Count:N0} IDs from {Path.GetFileName(path)}";
    Console.WriteLine($"Loaded {ids.Count:N0} unique item IDs"
                      + (skipped > 0 ? $" ({skipped:N0} non-numeric row(s) skipped, e.g. the header)" : "")
                      + ".");
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
        return false; // non-text body can't match either filter
    }

    if (itemIds is not null)
    {
        foreach (Match m in itemIdRegex.Matches(body))
        {
            if (long.TryParse(m.Groups[1].Value, out var id) && itemIds.Contains(id))
                return true;
        }
        return false;
    }

    return body.Contains(searchText, comparison);
}

async Task PreviewAsync()
{
    await using var receiver = client.CreateReceiver(queueName);

    long scanned = 0, matchedActive = 0, matchedDeferred = 0, matchedScheduled = 0;
    long fromSequence = 0;
    var samplesShown = 0;
    var total = await TryGetActiveCountAsync();
    var started = Stopwatch.StartNew();

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
                switch (message.State)
                {
                    case ServiceBusMessageState.Deferred: matchedDeferred++; break;
                    case ServiceBusMessageState.Scheduled: matchedScheduled++; break;
                    default: matchedActive++; break;
                }

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
        WriteProgress($"  scanned {Progress(scanned, total, started)} | matched {matchedActive + matchedDeferred + matchedScheduled:N0}");
    }

    var matched = matchedActive + matchedDeferred + matchedScheduled;
    Console.WriteLine();
    Console.WriteLine($"Done in {started.Elapsed:mm\\:ss}. Scanned {scanned:N0} messages; {matched:N0} match the filter.");
    if (matchedDeferred > 0 || matchedScheduled > 0)
        Console.WriteLine($"  breakdown: {matchedActive:N0} active, {matchedDeferred:N0} deferred, {matchedScheduled:N0} scheduled — Delete handles all three.");
}

// Peeks every message on the queue (read-only) and writes a Markdown report of
// aggregate statistics — enqueue-time histogram, message kinds, repeated ItemIds,
// duplicate bodies — plus sample bodies, framed so the whole file can be pasted
// into an LLM to answer "why are there so many messages on this queue?".
async Task AnalyzeQueueAsync()
{
    await using var receiver = client.CreateReceiver(queueName);

    var runtime = await TryGetRuntimePropertiesAsync();
    var total = runtime?.ActiveMessageCount;
    var started = Stopwatch.StartNew();

    long scanned = 0, stateActive = 0, stateDeferred = 0, stateScheduled = 0, nonTextBodies = 0;
    long sizeMin = long.MaxValue, sizeMax = 0, sizeSum = 0;
    DateTimeOffset? oldest = null, newest = null;
    var hourBuckets = new Dictionary<DateTime, long>();
    var kinds = new Dictionary<string, KindStats>();
    var itemIdCounts = new Dictionary<long, long>();
    var bodyHashCounts = new Dictionary<string, long>();
    var bodyHashSamples = new Dictionary<string, string>();
    var propValueCounts = new Dictionary<string, Dictionary<string, long>>();

    Console.WriteLine("Analyzing the queue (messages are peeked — not locked or modified)...");

    long fromSequence = 0;
    while (true)
    {
        var batch = await receiver.PeekMessagesAsync(BatchSize, fromSequence);
        if (batch.Count == 0)
            break;

        foreach (var message in batch)
        {
            scanned++;
            Aggregate(message);
        }

        fromSequence = batch[^1].SequenceNumber + 1;
        WriteProgress($"  analyzed {Progress(scanned, total, started)}");
    }
    Console.WriteLine();

    // Aggregate hourly while scanning; only collapse to daily buckets if the queue
    // spans more than a few days, so the histogram stays readable either way.
    var daily = oldest is not null && newest!.Value - oldest.Value > TimeSpan.FromDays(3);
    var orderedBuckets = (daily
            ? hourBuckets.GroupBy(kv => kv.Key.Date).Select(g => KeyValuePair.Create(g.Key, g.Sum(kv => kv.Value)))
            : hourBuckets)
        .OrderBy(kv => kv.Key).ToList();
    var orderedKinds = kinds.OrderByDescending(kv => kv.Value.Count).ToList();
    string BucketLabel(DateTime bucket) => daily ? $"{bucket:yyyy-MM-dd}" : $"{bucket:yyyy-MM-dd HH:00}";

    Directory.CreateDirectory("analysis");
    var reportPath = Path.Combine("analysis", $"{queueName}-{DateTime.Now:yyyyMMdd-HHmmss}.md");
    var report = new StringBuilder();

    report.AppendLine($"# Queue backlog analysis: {queueName}");
    report.AppendLine();
    report.AppendLine("You are analyzing an Azure Service Bus queue backlog. Below are aggregate");
    report.AppendLine($"statistics and sample messages peeked (read-only) from the queue `{queueName}`,");
    report.AppendLine("which holds far more messages than it normally does. Answer: **why are there");
    report.AppendLine("so many messages on this queue?** Look for retry/poison loops (repeated");
    report.AppendLine("ItemIds, duplicate bodies), producer surges (enqueue-time histogram), stuck or");
    report.AppendLine("slow consumers (old oldest-message age with steady arrivals), and dominant");
    report.AppendLine("message kinds. Suggest concrete next diagnostic or remediation steps.");
    report.AppendLine();
    report.AppendLine("## Queue counts");
    report.AppendLine();
    report.AppendLine($"- Generated: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC");
    report.AppendLine(runtime is not null
        ? $"- Active: {runtime.ActiveMessageCount:N0} | dead-lettered: {runtime.DeadLetterMessageCount:N0} | scheduled: {runtime.ScheduledMessageCount:N0}"
        : "- Queue counts unavailable (connection string may lack Manage rights)");
    report.AppendLine($"- Messages scanned (peeked): {scanned:N0} in {started.Elapsed:mm\\:ss}");
    report.AppendLine($"- States seen: {stateActive:N0} active, {stateDeferred:N0} deferred, {stateScheduled:N0} scheduled");
    if (scanned > nonTextBodies)
        report.AppendLine($"- Body size (chars): min {sizeMin:N0}, avg {sizeSum / Math.Max(scanned - nonTextBodies, 1):N0}, max {sizeMax:N0}");
    if (nonTextBodies > 0)
        report.AppendLine($"- {nonTextBodies:N0} message(s) had bodies that could not be read as text");
    if (scanned == 0)
        report.AppendLine($"{Environment.NewLine}No messages were peeked — the queue appears to be empty.");

    report.AppendLine();
    report.AppendLine($"## Enqueue-time histogram ({(daily ? "per day" : "per hour")}, UTC)");
    report.AppendLine();
    if (oldest is not null)
    {
        report.AppendLine($"Oldest message: {oldest.Value.UtcDateTime:yyyy-MM-dd HH:mm:ss} UTC "
                          + $"(age {(DateTimeOffset.UtcNow - oldest.Value).TotalHours:N1} h); "
                          + $"newest: {newest!.Value.UtcDateTime:yyyy-MM-dd HH:mm:ss} UTC.");
        report.AppendLine();
    }
    report.AppendLine("| Bucket (UTC) | Count |");
    report.AppendLine("| --- | ---: |");
    foreach (var (bucket, count) in orderedBuckets)
        report.AppendLine($"| {BucketLabel(bucket)} | {count:N0} |");

    report.AppendLine();
    report.AppendLine("## Message kinds");
    report.AppendLine();
    report.AppendLine($"{kinds.Count:N0} distinct kind(s). Kind = Subject when set; otherwise the body's");
    report.AppendLine("top-level JSON property names; otherwise the start of the body text.");
    report.AppendLine();
    report.AppendLine("| Kind | Count | % |");
    report.AppendLine("| --- | ---: | ---: |");
    foreach (var (kind, stats) in orderedKinds.Take(20))
        report.AppendLine($"| {Cell(kind)} | {stats.Count:N0} | {stats.Count * 100.0 / Math.Max(scanned, 1):N1}% |");
    if (orderedKinds.Count > 20)
        report.AppendLine($"| (other {orderedKinds.Count - 20:N0} kinds) | {orderedKinds.Skip(20).Sum(kv => kv.Value.Count):N0} | |");

    report.AppendLine();
    report.AppendLine("## Top repeated ItemIds");
    report.AppendLine();
    if (itemIdCounts.Count == 0)
    {
        report.AppendLine("No `\"ItemId\":<number>` values found in message bodies.");
    }
    else
    {
        report.AppendLine($"{itemIdCounts.Count:N0} distinct ItemId(s) seen. High repetition of one ID suggests a retry/poison loop.");
        report.AppendLine();
        report.AppendLine("| ItemId | Occurrences |");
        report.AppendLine("| --- | ---: |");
        foreach (var (id, count) in itemIdCounts.OrderByDescending(kv => kv.Value).Take(20))
            report.AppendLine($"| {id} | {count:N0} |");
    }

    report.AppendLine();
    report.AppendLine("## Duplicate bodies");
    report.AppendLine();
    report.AppendLine($"Distinct bodies: {bodyHashCounts.Count:N0} of {scanned - nonTextBodies:N0} text bodies scanned.");
    var dupes = bodyHashCounts.Where(kv => kv.Value >= 2).OrderByDescending(kv => kv.Value).Take(10).ToList();
    if (dupes.Count > 0)
    {
        report.AppendLine();
        report.AppendLine("| Copies | Sample (first 120 chars) |");
        report.AppendLine("| ---: | --- |");
        foreach (var (hash, count) in dupes)
            report.AppendLine($"| {count:N0} | {Cell(bodyHashSamples[hash])} |");
    }

    report.AppendLine();
    report.AppendLine("## Application properties");
    report.AppendLine();
    if (propValueCounts.Count == 0)
    {
        report.AppendLine("No application properties on any scanned message.");
    }
    else
    {
        report.AppendLine("| Property | Messages | Top values |");
        report.AppendLine("| --- | ---: | --- |");
        foreach (var (key, values) in propValueCounts.OrderByDescending(kv => kv.Value.Values.Sum()))
        {
            var top = string.Join("; ", values.OrderByDescending(kv => kv.Value).Take(5)
                .Select(kv => $"{Cell(kv.Key)} ({kv.Value:N0})"));
            report.AppendLine($"| {Cell(key)} | {values.Values.Sum():N0} | {top} |");
        }
    }

    report.AppendLine();
    report.AppendLine("## Sample bodies (top kinds, truncated to 1,000 chars)");
    foreach (var (kind, stats) in orderedKinds.Take(5))
    {
        report.AppendLine();
        report.AppendLine($"### Kind: {Cell(kind)} ({stats.Count:N0} messages)");
        foreach (var sample in stats.Samples)
        {
            // Bodies can themselves contain ``` — use a longer fence when they do.
            var fence = sample.Contains("```") ? "````" : "```";
            report.AppendLine();
            report.AppendLine(fence);
            report.AppendLine(sample);
            report.AppendLine(fence);
        }
    }

    await File.WriteAllTextAsync(reportPath, report.ToString());

    Console.WriteLine($"Done in {started.Elapsed:mm\\:ss}. Scanned {scanned:N0} messages"
                      + (oldest is not null
                          ? $" spanning {oldest.Value.UtcDateTime:yyyy-MM-dd HH:mm} → {newest!.Value.UtcDateTime:yyyy-MM-dd HH:mm} (UTC)"
                          : "") + ".");
    if (orderedKinds.Count > 0)
        Console.WriteLine("Top kinds: " + string.Join(" | ", orderedKinds.Take(3).Select(kv => $"{kv.Key} ({kv.Value.Count:N0})")));
    if (orderedBuckets.Count > 0)
    {
        var busiest = orderedBuckets.MaxBy(kv => kv.Value);
        Console.WriteLine($"Busiest {(daily ? "day" : "hour")}: {BucketLabel(busiest.Key)} UTC — {busiest.Value:N0} messages");
    }
    Console.WriteLine($"Report: {reportPath}  (paste it into an LLM to get the \"why\" interpreted)");
    return;

    void Aggregate(ServiceBusReceivedMessage message)
    {
        switch (message.State)
        {
            case ServiceBusMessageState.Deferred: stateDeferred++; break;
            case ServiceBusMessageState.Scheduled: stateScheduled++; break;
            default: stateActive++; break;
        }

        var enqueued = message.EnqueuedTime;
        if (oldest is null || enqueued < oldest) oldest = enqueued;
        if (newest is null || enqueued > newest) newest = enqueued;
        var t = enqueued.UtcDateTime;
        var bucket = new DateTime(t.Year, t.Month, t.Day, t.Hour, 0, 0, DateTimeKind.Utc);
        hourBuckets[bucket] = hourBuckets.GetValueOrDefault(bucket) + 1;

        foreach (var (key, value) in message.ApplicationProperties)
        {
            if (!propValueCounts.TryGetValue(key, out var values))
                propValueCounts[key] = values = new Dictionary<string, long>();
            var text = value?.ToString() ?? "(null)";
            if (text.Length > 40) text = text[..40] + "…";
            // Cap distinct tracked values per key so GUID-valued properties can't balloon memory.
            if (values.Count >= 50 && !values.ContainsKey(text))
                text = "(other values)";
            values[text] = values.GetValueOrDefault(text) + 1;
        }

        string body;
        try
        {
            body = message.Body.ToString();
        }
        catch
        {
            nonTextBodies++;
            AddKind("(non-text body)", "");
            return;
        }

        sizeMin = Math.Min(sizeMin, body.Length);
        sizeMax = Math.Max(sizeMax, body.Length);
        sizeSum += body.Length;

        foreach (Match m in itemIdRegex.Matches(body))
        {
            if (long.TryParse(m.Groups[1].Value, out var id))
                itemIdCounts[id] = itemIdCounts.GetValueOrDefault(id) + 1;
        }

        var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(body)));
        bodyHashCounts[hash] = bodyHashCounts.GetValueOrDefault(hash) + 1;
        if (!bodyHashSamples.ContainsKey(hash))
            bodyHashSamples[hash] = body.Length > 120 ? body[..120] + "…" : body;

        AddKind(KindOf(message, body), body);
    }

    void AddKind(string kind, string body)
    {
        if (!kinds.TryGetValue(kind, out var stats))
            kinds[kind] = stats = new KindStats();
        stats.Count++;
        if (stats.Samples.Count < 3 && body.Length > 0)
            stats.Samples.Add(body.Length > 1000 ? body[..1000] + "\n… [truncated]" : body);
    }

    static string KindOf(ServiceBusReceivedMessage message, string body)
    {
        if (!string.IsNullOrEmpty(message.Subject))
            return $"subject: {message.Subject}";

        try
        {
            using var doc = JsonDocument.Parse(body);
            var root = doc.RootElement;
            if (root.ValueKind == JsonValueKind.Object)
            {
                var names = root.EnumerateObject().Select(p => p.Name)
                    .OrderBy(n => n, StringComparer.Ordinal).ToList();
                var shown = string.Join(", ", names.Take(12));
                if (names.Count > 12) shown += ", …";
                return $"json {{{shown}}}";
            }
            return root.ValueKind == JsonValueKind.Array ? "json array" : $"json {root.ValueKind}";
        }
        catch (JsonException)
        {
            var head = Regex.Replace(body.Trim(), @"\s+", " ");
            if (head.Length > 60) head = head[..60] + "…";
            return $"text: {head}";
        }
    }

    // Markdown table cells can't contain newlines or unescaped pipes.
    static string Cell(string text) => Regex.Replace(text, @"\s+", " ").Replace("|", "\\|");
}

async Task DeleteAsync()
{
    Console.WriteLine();
    Console.WriteLine($"This will DELETE every message from '{queueName}' matching:");
    Console.WriteLine($"  {filterDescription}");
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

    await using var receiver = client.CreateReceiver(queueName); // default PeekLock
    await using var sender = client.CreateSender(queueName);

    long scanned = 0, deleted = 0, requeued = 0;
    var started = Stopwatch.StartNew();
    var activeCount = await TryGetActiveCountAsync();
    var total = limit is null ? activeCount : Math.Min(limit.Value, activeCount ?? limit.Value);

    Console.WriteLine($"Backing up matches to {backupPath}");
    Console.WriteLine(limit is null ? "Processing entire queue..." : $"Processing up to {limit:N0} messages...");

    for (var pass = 1; ; pass++)
    {
        // Fresh run ID per pass so this pass's clones are distinguishable from an
        // earlier pass's clones (which get treated as ordinary messages again).
        var runId = Guid.NewGuid().ToString("N");
        var hitLimit = false;
        var consecutiveEmpty = 0;
        var allCloneStreak = 0;

        while (true)
        {
            var batch = await receiver.ReceiveMessagesAsync(BatchSize, TimeSpan.FromSeconds(5));
            if (batch.Count == 0)
            {
                // One empty receive can be a transient blip rather than an empty queue.
                if (++consecutiveEmpty >= 3)
                    break;
                continue;
            }
            consecutiveEmpty = 0;

            // Split the batch: matches get backed up + deleted, everything else
            // (including this pass's own clones coming back around) is re-queued.
            var matches = new List<ServiceBusReceivedMessage>();
            var others = new List<ServiceBusReceivedMessage>();
            var clonesInBatch = 0;
            foreach (var message in batch)
            {
                var isOwnClone = message.ApplicationProperties.TryGetValue(RunIdProperty, out var tag)
                                 && (string?)tag == runId;
                if (isOwnClone) clonesInBatch++;
                if (!isOwnClone && IsMatch(message)) matches.Add(message);
                else others.Add(message);
            }

            foreach (var message in matches)
                await WriteBackupAsync(backup, message);
            await backup.FlushAsync();

            // Send all clones before completing anything: a crash mid-batch can at
            // worst duplicate messages, never lose one.
            await SendClonesAsync(sender, others, runId);
            await Task.WhenAll(batch.Select(m => receiver.CompleteMessageAsync(m)));

            deleted += matches.Count;
            requeued += others.Count - clonesInBatch;
            scanned += batch.Count - clonesInBatch;
            WriteProgress($"  pass {pass}: processed {Progress(scanned, total, started)} | deleted {deleted:N0} | re-queued {requeued:N0}");

            if (limit is not null && scanned >= limit)
            {
                hitLimit = true;
                break;
            }

            // Receiving nothing but our own clones twice in a row means the originals
            // are exhausted (subject to the peek verification below).
            if (clonesInBatch == batch.Count)
            {
                if (++allCloneStreak >= 2)
                    break;
            }
            else
            {
                allCloneStreak = 0;
            }
        }

        Console.WriteLine();
        if (hitLimit)
        {
            Console.WriteLine("Stopped at the run limit — run Delete again to continue where this left off.");
            break;
        }

        // A receive pass alone can't be trusted to have seen everything: partitioned
        // queues don't deliver strictly FIFO (so the clone-streak stop can fire early),
        // and deferred/scheduled messages never appear in a normal receive. Verify with
        // a read-only peek and only stop once zero matches remain.
        Console.WriteLine($"Pass {pass} complete — verifying with a peek scan...");
        var remaining = await ScanRemainingMatchesAsync(receiver);

        if (remaining.DeferredSequenceNumbers.Count > 0)
            deleted += await DeleteDeferredMatchesAsync(receiver, backup, remaining.DeferredSequenceNumbers);
        if (remaining.Scheduled.Count > 0)
            deleted += await CancelScheduledMatchesAsync(sender, backup, remaining.Scheduled);

        if (remaining.ActiveCount == 0)
        {
            Console.WriteLine("Verified: no matching messages remain on the queue.");
            break;
        }
        if (pass >= MaxDeletePasses)
        {
            Console.WriteLine($"WARNING: {remaining.ActiveCount:N0} matching messages still remain after {pass} passes.");
            Console.WriteLine("Run Delete again to continue.");
            break;
        }
        Console.WriteLine($"{remaining.ActiveCount:N0} matching messages still active — starting pass {pass + 1}...");
    }

    Console.WriteLine($"Done in {started.Elapsed:mm\\:ss}. Processed {scanned:N0} messages: deleted {deleted:N0}, re-queued {requeued:N0}.");
    Console.WriteLine(deleted > 0
        ? $"Backup of deleted messages: {backupPath}"
        : "No messages matched; backup file is empty.");
    Console.WriteLine("Note: re-queued clones carry an extra application property " +
                      $"'{RunIdProperty}' and a new enqueue time/sequence number.");
    await ShowQueueCountsAsync();
}

async Task<(long ActiveCount, List<long> DeferredSequenceNumbers, List<ServiceBusReceivedMessage> Scheduled)>
    ScanRemainingMatchesAsync(ServiceBusReceiver receiver)
{
    long active = 0, scanned = 0;
    var deferred = new List<long>();
    var scheduled = new List<ServiceBusReceivedMessage>();
    long fromSequence = 0;

    while (true)
    {
        var batch = await receiver.PeekMessagesAsync(BatchSize, fromSequence);
        if (batch.Count == 0)
            break;

        foreach (var message in batch)
        {
            scanned++;
            if (!IsMatch(message))
                continue;
            switch (message.State)
            {
                case ServiceBusMessageState.Deferred: deferred.Add(message.SequenceNumber); break;
                case ServiceBusMessageState.Scheduled: scheduled.Add(message); break;
                default: active++; break;
            }
        }

        fromSequence = batch[^1].SequenceNumber + 1;
        WriteProgress($"  verifying: peeked {scanned:N0} | remaining matches {active + deferred.Count + scheduled.Count:N0}");
    }

    Console.WriteLine();
    return (active, deferred, scheduled);
}

async Task<long> DeleteDeferredMatchesAsync(ServiceBusReceiver receiver, StreamWriter backup, List<long> sequenceNumbers)
{
    Console.WriteLine($"Deleting {sequenceNumbers.Count:N0} deferred matching messages...");
    long deleted = 0;
    foreach (var chunk in sequenceNumbers.Chunk(50))
    {
        try
        {
            var messages = await receiver.ReceiveDeferredMessagesAsync(chunk);
            foreach (var message in messages)
                await WriteBackupAsync(backup, message);
            await backup.FlushAsync();
            await Task.WhenAll(messages.Select(m => receiver.CompleteMessageAsync(m)));
            deleted += messages.Count;
        }
        catch (ServiceBusException ex)
        {
            Console.WriteLine($"  could not delete a deferred chunk ({ex.Reason}); skipped {chunk.Length} messages.");
        }
    }
    return deleted;
}

async Task<long> CancelScheduledMatchesAsync(ServiceBusSender sender, StreamWriter backup, List<ServiceBusReceivedMessage> scheduled)
{
    Console.WriteLine($"Cancelling {scheduled.Count:N0} scheduled matching messages...");
    long deleted = 0;
    foreach (var message in scheduled)
    {
        try
        {
            await WriteBackupAsync(backup, message);
            await sender.CancelScheduledMessageAsync(message.SequenceNumber);
            deleted++;
        }
        catch (ServiceBusException ex)
        {
            Console.WriteLine($"  could not cancel scheduled message {message.SequenceNumber} ({ex.Reason}).");
        }
    }
    await backup.FlushAsync();
    return deleted;
}

async Task SendClonesAsync(ServiceBusSender sender, List<ServiceBusReceivedMessage> messages, string runId)
{
    if (messages.Count == 0)
        return;

    var messageBatch = await sender.CreateMessageBatchAsync();
    foreach (var message in messages)
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

async Task WriteBackupAsync(StreamWriter backup, ServiceBusReceivedMessage message)
{
    await backup.WriteLineAsync(JsonSerializer.Serialize(new
    {
        message.MessageId,
        message.SequenceNumber,
        message.EnqueuedTime,
        message.Subject,
        message.ContentType,
        message.CorrelationId,
        State = message.State.ToString(),
        ApplicationProperties = message.ApplicationProperties
            .ToDictionary(p => p.Key, p => p.Value?.ToString()),
        Body = message.Body.ToString(),
    }));
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

async Task WatchCountsAsync()
{
    var admin = new ServiceBusAdministrationClient(connectionString);
    Console.WriteLine("Watching queue counts — one line per refresh, press any key to stop.");

    while (true)
    {
        try
        {
            var props = (await admin.GetQueueRuntimePropertiesAsync(queueName)).Value;
            Console.WriteLine($"  {DateTime.Now:HH:mm:ss}  active {props.ActiveMessageCount:N0} | dead-lettered {props.DeadLetterMessageCount:N0} | scheduled {props.ScheduledMessageCount:N0}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"  {DateTime.Now:HH:mm:ss}  (failed to read counts: {ex.Message})");
        }

        // Wait 10s in short slices so a key press is noticed promptly.
        for (var i = 0; i < 100; i++)
        {
            if (Console.KeyAvailable)
            {
                Console.ReadKey(intercept: true);
                return;
            }
            await Task.Delay(100);
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
    => (await TryGetRuntimePropertiesAsync())?.ActiveMessageCount;

async Task<QueueRuntimeProperties?> TryGetRuntimePropertiesAsync()
{
    try
    {
        var admin = new ServiceBusAdministrationClient(connectionString);
        return (await admin.GetQueueRuntimePropertiesAsync(queueName)).Value;
    }
    catch
    {
        return null; // no Manage rights — callers degrade gracefully (no %/ETA, no counts)
    }
}

string Progress(long done, long? total, Stopwatch started)
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

record QueueEntry(string QueueName, string ConnectionString, string Namespace);

// Per-kind aggregate for the analyze report. A class (not a record) because Count
// is incremented in place while the instance lives inside a dictionary.
sealed class KindStats
{
    public long Count;
    public List<string> Samples { get; } = new();
}
