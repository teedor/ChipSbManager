# ChipSbManager

Console tool for cleaning up an Azure Service Bus queue: finds messages whose body
contains a given text string, previews the match count, and deletes matches after
backing them up to a local file.

## Setup

1. Copy the example config and fill in your values:

   ```sh
   cp appsettings.example.json appsettings.json
   ```

   `appsettings.json` is gitignored so connection strings are never committed.
   Multiple queues can be configured in the `ServiceBus:Queues` array (each
   entry has its own `QueueName` and `ConnectionString`); the app asks which
   one to work with at startup and offers a "switch queue" menu option. The
   older single `ServiceBus:ConnectionString`/`QueueName` settings still work.

   On restricted networks (e.g. ExpressRoute with no internet egress) where
   outbound TCP port 5671 is blocked, set `"UseWebSockets": true` to tunnel
   AMQP over port 443 instead. Symptom of needing this: the queue counts at
   startup work (they use HTTPS/443) but peek/receive hangs and times out.

2. Run:

   ```sh
   dotnet run
   ```

## Usage

The app starts by picking a queue (automatic if only one is configured), then
shows a menu ordered by workflow: set a filter (option 1 or 2), preview,
delete. Filter types:

- **Search text** — substring match against the message body, optionally
  case-sensitive.
- **CSV of item IDs** — reads integer IDs from a CSV file (default
  `itemIds.csv`; header row and blank/non-numeric rows are skipped). A message
  matches when its body contains `"ItemId":<id>,` for any listed ID (the value
  is always followed by a comma in these messages). Matching tolerates
  whitespace around the colon and key casing, and is boundary-safe: ID `449594`
  never matches `"ItemId":4495945,` and vice versa.

Preview and Delete both operate on whichever filter is currently active:

- **Preview** — peeks through the whole queue and counts matching messages.
  Completely non-destructive: peeking doesn't lock messages or touch
  delivery counts, so it's always safe to run.
- **Delete** — backs up each matching message to `backups/<queue>-<timestamp>.jsonl`
  (body, message ID, properties, enqueue time), then deletes it. Requires typing
  `DELETE` to confirm.

There is also a **watch** option that prints the queue's active /
dead-lettered / scheduled counts every 10 seconds (one timestamped line per
refresh, so you can see the trend during a big delete); press any key to stop.

A **send** option puts a single message on the queue: paste the body (multi-line
is fine), finish with a line containing only a dot (`.`), and confirm. If the
body parses as JSON the message is sent with content type `application/json`;
otherwise it is sent as plain text (with a note, in case a paste went wrong).

An **analyze** option answers "why are there so many messages on this queue?"
by peeking every message (read-only, no filter needed) and writing a Markdown
report designed to be handed to an LLM — see
[Analyzing a backlog](#analyzing-a-backlog-why-are-there-so-many-messages)
below for a worked example.

Both operations show live progress (count, percentage, rate, ETA — percentage and
ETA need Manage rights on the connection string to read the queue's total).

Delete can optionally be limited to N messages per run. Stopping early — via the
limit, Ctrl+C, or a crash — is always safe: each message is either untouched or
fully processed, so simply run Delete again to continue where the last run
stopped.

## Analyzing a backlog (why are there so many messages?)

When a queue that normally holds a handful of messages suddenly has thousands,
pick **[7] Analyze queue** from the menu. No filter is needed — it scans
everything. Peeking is completely non-destructive (nothing is locked, delivery
counts are untouched), so it's always safe to run, even with consumers active.

```
> 7
Analyzing the queue (messages are peeked — not locked or modified)...
  analyzed 16,234 / ~16,234 (100%) | 210/s
Done in 01:17. Scanned 16,234 messages spanning 2026-07-10 04:12 → 2026-07-17 09:46 (UTC).
Top kinds: json {ItemId, MovementReference, Status} (14,982) | subject: DecisionNotification (1,101)
Busiest day: 2026-07-15 UTC — 9,873 messages
Report: analysis/myqueue-20260717-101110.md  (paste it into an LLM to get the "why" interpreted)
```

That console summary alone often points at the cause, but the full story is in
the report. It opens with a framing prompt ("You are analyzing an Azure Service
Bus queue backlog... why are there so many messages on this queue?") followed
by the evidence, so the whole file can be pasted verbatim into Claude (or any
LLM) — or piped through the Claude Code CLI:

```sh
claude -p < analysis/myqueue-20260717-101110.md
```

An excerpt of what the report contains:

```markdown
## Enqueue-time histogram (per day, UTC)

Oldest message: 2026-07-10 04:12:09 UTC (age 173.6 h); newest: 2026-07-17 09:46:45 UTC.

| Bucket (UTC) | Count |
| --- | ---: |
| 2026-07-10 | 312 |
| 2026-07-15 | 9,873 |
| 2026-07-16 | 4,988 |

## Top repeated ItemIds

3,207 distinct ItemId(s) seen. High repetition of one ID suggests a retry/poison loop.

| ItemId | Occurrences |
| --- | ---: |
| 449594 | 4,120 |
| 451022 | 17 |

## Duplicate bodies

Distinct bodies: 3,251 of 16,234 text bodies scanned.

| Copies | Sample (first 120 chars) |
| ---: | --- |
| 4,120 | {"ItemId":449594,"MovementReference":"GB2026...","Status":"Retry"} |
```

Each section is a diagnostic signal:

| Section | What it tells you |
| --- | --- |
| Enqueue-time histogram | *When* the buildup started. A cliff on one day with an old oldest-message age usually means the consumer stopped or slowed; a steady spread means arrivals simply outpace processing. |
| Message kinds | *What* is piling up — grouped by Subject, or by the body's top-level JSON property names. One kind at ~100% means a single producer or flow is responsible. |
| Top repeated ItemIds / duplicate bodies | The retry/poison-loop indicators. The same ItemId or identical body appearing thousands of times means something is re-sending the same work over and over. |
| Application properties | Producer fingerprints — e.g. messages carrying `ChipSbManager.RequeueRunId` are clones from an earlier delete run by this tool. |
| Sample bodies | Up to 3 truncated real examples per top kind, so the LLM (or you) can see what the messages actually are. |

In the example above the answer jumps out even without an LLM: one `ItemId`
accounts for 4,120 identical `"Status":"Retry"` messages — a poison-message
retry loop that started on the 15th — which you could then clean up with the
CSV filter (option 2) and Delete.

Reports may contain message bodies, so `analysis/` is gitignored.

## How delete works (and why)

Service Bus has no server-side content search, so every message must be received
and inspected client-side. The queues here have **MaxDeliveryCount = 1**, which
means the usual approach (abandon non-matching messages) would dead-letter them
on their next delivery.

Instead the tool drains and re-queues: each message is received exactly once;
matches are backed up and completed (deleted), and non-matches are re-sent as a
fresh clone (same body, message ID, and properties; delivery count reset to 0)
before the original is completed. The clone is sent before the original is
completed, so a crash mid-run can at worst duplicate a message, never lose one.

A single receive pass can't be trusted to see every message: partitioned queues
don't deliver strictly FIFO, and deferred/scheduled messages never appear in a
normal receive (though peek counts them). So after each pass, delete re-peeks
the queue (read-only) and only reports success once **zero** matches remain,
running further passes if needed (up to 5 per run). Deferred matches are
deleted via receive-by-sequence-number and scheduled matches via
cancel-scheduled-message; preview shows a breakdown by message state whenever
deferred/scheduled matches exist.

Consequences of a delete run:

- **Pause consumers first.** The tool competes for messages with anything else
  receiving from the queue.
- Re-queued messages get a new enqueue time and sequence number and move to the
  back of the queue (ordering is not preserved).
- Re-queued messages carry an extra application property
  `ChipSbManager.RequeueRunId`, which the tool also uses to know when it has
  cycled through every original message.
- Not suitable as-is for queues with duplicate detection or sessions enabled.
