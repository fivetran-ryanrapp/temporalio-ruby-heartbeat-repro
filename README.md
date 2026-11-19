# temporalio-ruby heartbeat repro

A minimal reproduction for a question about `temporalio` (sdk-ruby). It reproduces identically on 1.11.0 (the latest
stable release when this was written) and on 1.6.0:

> If an activity's heartbeats come from outside the SDK's activity context (a separate process, using the by-ID
> heartbeat API), the server records them, but the worker still cancels the activity locally and discards its result.
> Is that intended, and is there a way to leave heartbeat-timeout enforcement to the server?

## Why heartbeat from a separate process

Our activities sometimes make long native calls (C extensions) that hold the GVL. While one runs, no other Ruby thread in
the worker can run, so a heartbeat thread cannot beat. On another Ruby SDK we avoided this by heartbeating from a separate
process, using the heartbeat-by-ID API (`client.async_activity_handle(ActivityIDReference.new(workflow_id:, run_id:,
activity_id:)).heartbeat(...)`). The server keeps the activity alive on those heartbeats.

## What this repo shows

`repro.rb` starts a dev server, a workflow worker, and an **activity worker in its own process**, then runs five scenarios.
Every activity has `heartbeat_timeout = 6s` and `max_attempts = 1`. "SDK heartbeat" means a thread calling
`Temporalio::Activity::Context.current.heartbeat` every 2s. "Separate process" means `heartbeater.rb` calling the
heartbeat-by-ID API every 2s, for as long as the activity runs.

| | Activity | SDK heartbeat | Separate process | What happens |
|---|---|---|---|---|
| A | sleeps 20s | no | yes | The worker raises `CanceledError` into the activity at **t+11.0s**, although the server shows a heartbeat about 2s old. The workflow later fails with `Heartbeat timeout`, after the separate process stops. |
| B | sleeps 20s | yes | no | Completes. |
| C | holds the GVL for 20s | yes | yes | The SDK heartbeat thread is starved. The activity finishes normally, but the workflow fails with `Heartbeat timeout`: **the finished result was never delivered.** |
| D | like A, with `activity_cancel_raise false` | no | yes | The activity runs to the end and returns normally (`cancellation.canceled?` is `true`), but again **the result is dropped** and the workflow fails with `Heartbeat timeout`. |
| E | holds the GVL for 4s | yes | yes | Completes (the hold is shorter than the timeout plus the offset seen in A). |

The offset in A is the heartbeat timeout plus about 5s (11.0s for a 6s timeout). We saw the same offset in a larger
application: 49.8s for a 45s timeout, and 2:00 to 2:12 for a 120s timeout.

The GVL hold is OpenSSL's PBKDF2 (`OpenSSL::PKCS5.pbkdf2_hmac`), which runs in C without releasing the GVL. It stands in
for any such native call and is not itself the subject of the question.

## Questions

1. Is the worker's cancellation, at about the heartbeat timeout after the last heartbeat made through the SDK's activity
   context, intended? Which layer does it?
2. Is there a way to disable it, so that only the server's heartbeat-timeout enforcement applies?
3. If the worker cancelled the activity but the server still considers the attempt running (as in A, B and D, because it is
   receiving heartbeats), why is the finished activity's result discarded instead of being sent?
4. What is the recommended way to heartbeat an activity that makes long native calls holding the GVL?

## Run it

Requires Ruby 3.3 or newer (the gem's requirement; tested on 3.3.13, macOS arm64) and network access the first time,
because the SDK's test environment downloads a dev server. The `Gemfile` pins `temporalio` 1.11.0.

```sh
bundle install
bundle exec ruby repro.rb
```

To use a server you already run (namespace `default`), set `TEMPORAL_ADDRESS`:

```sh
TEMPORAL_ADDRESS=localhost:7233 bundle exec ruby repro.rb
```

It takes about two minutes. Two captured runs show the same results: `sample_output.txt` (temporalio 1.11.0, Ruby 3.3.13,
macOS arm64) and `reproduced_output.txt` (temporalio 1.6.0, Ruby 3.3.13, Linux ARM64).

## Files

- `repro.rb` runs the scenarios and prints the worker's and the server's view of each activity.
- `activity_worker.rb` is the activity worker, a separate process so that holding the GVL does not freeze the orchestrator.
- `heartbeater.rb` is the separate heartbeat process.
- `workflow.rb` is a workflow that runs one activity with a short heartbeat timeout and no retries.
