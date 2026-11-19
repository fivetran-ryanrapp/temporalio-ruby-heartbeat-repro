# frozen_string_literal: true

# Heartbeats a running activity from a separate process, by workflow id, run id and activity id. This is how a worker
# can keep an activity alive while a native call holds the GVL (no thread in the worker can run then).
# Usage: ruby heartbeater.rb TEMPORAL_ADDRESS WORKFLOW_ID RUN_ID ACTIVITY_ID INTERVAL_SECONDS
require "temporalio/client"

$stdout.sync = true

address, workflow_id, run_id, activity_id, interval = ARGV
client = Temporalio::Client.connect(address, "default")
handle =
  client.async_activity_handle(
    Temporalio::Client::ActivityIDReference.new(workflow_id:, run_id:, activity_id:)
  )

loop do
  handle.heartbeat("beat from a separate process")
  sleep interval.to_f
rescue Temporalio::Error::AsyncActivityCanceledError
  puts "HEARTBEATER the server reported the activity cancelled"
  exit 0
end
