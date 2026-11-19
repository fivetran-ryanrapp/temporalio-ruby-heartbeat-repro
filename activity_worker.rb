# frozen_string_literal: true

# The activity worker, run as its own process so that holding the GVL here does not freeze the orchestrator.
# Usage: ruby activity_worker.rb TEMPORAL_ADDRESS
require "openssl"
require "temporalio/activity"
require "temporalio/client"
require "temporalio/worker"

$stdout.sync = true

# Wall-clock for the log lines, relative to when this process started handing out activities.
def log(message)
  puts "ACTIVITY-LOG #{Process.clock_gettime(Process::CLOCK_REALTIME).round(3)} #{message}"
end

# OpenSSL's PBKDF2 is C code that never releases the GVL, so while it runs no other Ruby thread in this process can run.
# It stands in for any native call that holds the GVL (a long native extension call, for example).
def hold_gvl_for(seconds)
  @iterations_per_second ||=
    begin
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      OpenSSL::PKCS5.pbkdf2_hmac("pw", "salt", 3_000_000, 32, "sha256")
      3_000_000 / (Process.clock_gettime(Process::CLOCK_MONOTONIC) - started)
    end
  OpenSSL::PKCS5.pbkdf2_hmac("pw", "salt", (@iterations_per_second * seconds).to_i, 32, "sha256")
end

module Work
  # params: "mode" ("sleep" or "gvl"), "seconds", "sdk_heartbeat" (beat through the SDK's activity context from a thread).
  def self.run(params)
    context = Temporalio::Activity::Context.current
    log "ACTIVITY_START id=#{context.info.activity_id} attempt=#{context.info.attempt} heartbeat_timeout=#{context.info.heartbeat_timeout}"

    beater =
      if params.fetch("sdk_heartbeat")
        Thread.new do
          loop do
            context.heartbeat("beat from the SDK activity context")
            sleep 2
          end
        end
      end

    begin
      case params.fetch("mode")
      when "sleep"
        sleep params.fetch("seconds")
      when "gvl"
        hold_gvl_for(params.fetch("seconds"))
      end
      log "WORK_DONE canceled_flag=#{context.cancellation.canceled?}"
      "completed"
    rescue Exception => e # rubocop:disable Lint/RescueException
      log "WORK_INTERRUPTED #{e.class}: #{e.message}"
      raise
    ensure
      beater&.kill
      log "ACTIVITY_END"
    end
  end
end

# The SDK raises the cancellation into the activity's thread (the default).
class HoldActivity < Temporalio::Activity::Definition
  def execute(params) = Work.run(params)
end

# Same activity, but the cancellation does not interrupt the thread: the activity keeps running and returns normally.
class HoldActivityNoRaise < Temporalio::Activity::Definition
  activity_cancel_raise false

  def execute(params) = Work.run(params)
end

client = Temporalio::Client.connect(ARGV.fetch(0), "default")
worker =
  Temporalio::Worker.new(
    client:,
    task_queue: "repro-activities",
    activities: [HoldActivity, HoldActivityNoRaise],
    # One activity at a time, as a worker that heartbeats from a single process would run.
    tuner: Temporalio::Worker::Tuner.create_fixed(workflow_slots: 1, activity_slots: 1, local_activity_slots: 1)
  )
log "WORKER_READY"
worker.run(shutdown_signals: %w[SIGTERM SIGINT])
