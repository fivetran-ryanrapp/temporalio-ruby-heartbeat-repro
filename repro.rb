# frozen_string_literal: true

# Runs five scenarios against a local Temporal dev server and prints what the worker and the server each see.
# Usage: bundle exec ruby repro.rb            (starts a dev server through the SDK's test environment)
#        TEMPORAL_ADDRESS=host:port bundle exec ruby repro.rb   (uses a server you already run, namespace "default")
require "rbconfig"
require "securerandom"
require "timeout"
require "temporalio/client"
require "temporalio/testing"
require "temporalio/worker"
require_relative "workflow"

$stdout.sync = true

HEARTBEAT_TIMEOUT = 6 # seconds, on the activity
BEAT_EVERY = 2 # seconds, for both kinds of heartbeat
WORK = 20 # seconds the activity works for, in scenarios A to D

SCENARIOS = [
  {
    key: "A",
    title: "sleeps #{WORK}s; no heartbeat through the SDK; a separate process heartbeats by id",
    activity: "HoldActivity",
    params: { "mode" => "sleep", "seconds" => WORK, "sdk_heartbeat" => false },
    external: true
  },
  {
    key: "B",
    title: "sleeps #{WORK}s; heartbeats through the SDK's activity context from a thread; nothing else",
    activity: "HoldActivity",
    params: { "mode" => "sleep", "seconds" => WORK, "sdk_heartbeat" => true },
    external: false
  },
  {
    key: "C",
    title: "holds the GVL for #{WORK}s; SDK heartbeat thread; a separate process heartbeats by id",
    activity: "HoldActivity",
    params: { "mode" => "gvl", "seconds" => WORK, "sdk_heartbeat" => true },
    external: true
  },
  {
    key: "D",
    title: "like A, but registered with activity_cancel_raise false",
    activity: "HoldActivityNoRaise",
    params: { "mode" => "sleep", "seconds" => WORK, "sdk_heartbeat" => false },
    external: true
  },
  {
    key: "E",
    title: "holds the GVL for 4s; SDK heartbeat thread; a separate process heartbeats by id",
    activity: "HoldActivity",
    params: { "mode" => "gvl", "seconds" => 4, "sdk_heartbeat" => true },
    external: true
  }
].freeze

# Lines the activity worker process prints, as [epoch_seconds, text].
ACTIVITY_LOG = Queue.new

def stamp(epoch, started)
  format("t+%5.1fs", epoch - started)
end

def server_view(handle)
  pending = handle.describe.raw_description.pending_activities.first
  return "no pending activity" unless pending

  beat = pending.last_heartbeat_time
  details = pending.heartbeat_details&.payloads&.size.to_i.positive? ? "details recorded" : "no details"
  heartbeat =
    if beat
      "last heartbeat #{(Time.now.to_f - (beat.seconds + (beat.nanos / 1e9))).round(1)}s ago"
    else
      "no heartbeat yet"
    end
  "state=#{pending.state.to_s.delete_prefix("PENDING_ACTIVITY_STATE_")} attempt=#{pending.attempt} #{heartbeat} (#{details})"
end

def wait_for_pending_activity(handle)
  Timeout.timeout(30) do
    loop do
      pending = handle.describe.raw_description.pending_activities.first
      return pending if pending && pending.state.to_s.include?("STARTED")

      sleep 0.2
    end
  end
end

def failure_chain(error)
  chain = []
  while error
    chain << "#{error.class}: #{error.message}"
    error = error.cause
  end
  chain
end

def run_scenario(client, address, scenario)
  puts
  puts "=== #{scenario[:key]}: #{scenario[:title]}"
  workflow_id = "repro-#{scenario[:key]}-#{SecureRandom.hex(3)}"
  started = Time.now.to_f
  handle =
    client.start_workflow(
      "ScenarioWorkflow",
      { "activity" => scenario[:activity], "params" => scenario[:params], "heartbeat_timeout" => HEARTBEAT_TIMEOUT },
      id: workflow_id,
      task_queue: "repro-workflows"
    )
  pending = wait_for_pending_activity(handle)
  heartbeater =
    if scenario[:external]
      Process.spawn(
        RbConfig.ruby,
        File.join(__dir__, "heartbeater.rb"),
        address,
        workflow_id,
        handle.result_run_id,
        pending.activity_id,
        BEAT_EVERY.to_s
      )
    end

  activity_ended_at = nil
  next_view = Time.now.to_f
  until activity_ended_at || Time.now.to_f - started > 60
    while (line = ACTIVITY_LOG.pop(true) rescue nil)
      epoch, text = line
      puts "#{stamp(epoch, started)}  worker:  #{text}"
      activity_ended_at = epoch if text == "ACTIVITY_END"
    end
    if Time.now.to_f >= next_view && !activity_ended_at
      puts "#{stamp(Time.now.to_f, started)}  server:  #{server_view(handle)}"
      next_view += 2
    end
    sleep 0.1
  end
  # The separate process lives as long as the activity does, as it would in a worker that heartbeats from one.
  if heartbeater
    Process.kill("KILL", heartbeater)
    Process.wait(heartbeater)
  end

  outcome =
    begin
      "completed: #{Timeout.timeout(60) { handle.result }.inspect}"
    rescue Temporalio::Error::WorkflowFailedError => e
      "failed: #{failure_chain(e).last(2).join(" <- ")}"
    end
  puts "#{stamp(Time.now.to_f, started)}  workflow #{outcome}"
  { key: scenario[:key], activity_ended: activity_ended_at && (activity_ended_at - started).round(1), outcome: outcome }
end

env = ENV["TEMPORAL_ADDRESS"] ? nil : Temporalio::Testing::WorkflowEnvironment.start_local
begin
  client = env ? env.client : Temporalio::Client.connect(ENV.fetch("TEMPORAL_ADDRESS"), "default")
  address = ENV["TEMPORAL_ADDRESS"] || env.client.connection.target_host
  puts "Temporal server at #{address}; temporalio #{Temporalio::VERSION}; ruby #{RUBY_VERSION}"
  puts "activity heartbeat_timeout=#{HEARTBEAT_TIMEOUT}s, heartbeats every #{BEAT_EVERY}s, max_attempts=1"

  activity_worker =
    IO.popen([RbConfig.ruby, File.join(__dir__, "activity_worker.rb"), address], err: %i[child out])
  finished = false
  Thread.new do
    activity_worker.each_line(chomp: true) do |line|
      if (match = line.match(/\AACTIVITY-LOG (\S+) (.*)\z/))
        ACTIVITY_LOG << [match[1].to_f, match[2]]
      elsif !finished
        puts "worker (raw): #{line}"
      end
    end
  end
  Timeout.timeout(60) { sleep 0.2 until ACTIVITY_LOG.size.positive? }
  ACTIVITY_LOG.clear

  results = []
  workflow_worker = Temporalio::Worker.new(client:, task_queue: "repro-workflows", workflows: [ScenarioWorkflow])
  workflow_worker.run do
    SCENARIOS.each do |scenario|
      results << run_scenario(client, address, scenario)
      sleep 1 # let the single activity slot free up before the next scenario
      ACTIVITY_LOG.clear
    end
  end

  finished = true
  puts
  puts "=== Summary"
  results.each do |result|
    puts "#{result[:key]}: activity ended at t+#{result[:activity_ended]}s, workflow #{result[:outcome]}"
  end
ensure
  Process.kill("TERM", activity_worker.pid) if activity_worker
  env&.shutdown
end
