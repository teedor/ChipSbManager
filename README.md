# ChipSbManager

Console tool for cleaning up an Azure Service Bus queue: finds messages whose body
contains a given text string, previews the match count, and deletes matches after
backing them up to a local file.

## Setup

1. Copy the example config and fill in your values:

   ```sh
   cp appsettings.example.json appsettings.json
   ```

   `appsettings.json` is gitignored so the connection string is never committed.

2. Run:

   ```sh
   dotnet run
   ```

## Usage

The app prompts for a search string, then offers:

- **Preview** — peeks through the whole queue and counts messages containing the
  string. Completely non-destructive: peeking doesn't lock messages or touch
  delivery counts, so it's always safe to run.
- **Delete** — backs up each matching message to `backups/<queue>-<timestamp>.jsonl`
  (body, message ID, properties, enqueue time), then deletes it. Requires typing
  `DELETE` to confirm.

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

Consequences of a delete run:

- **Pause consumers first.** The tool competes for messages with anything else
  receiving from the queue.
- Re-queued messages get a new enqueue time and sequence number and move to the
  back of the queue (ordering is not preserved).
- Re-queued messages carry an extra application property
  `ChipSbManager.RequeueRunId`, which the tool also uses to know when it has
  cycled through every original message.
- Not suitable as-is for queues with duplicate detection or sessions enabled.
