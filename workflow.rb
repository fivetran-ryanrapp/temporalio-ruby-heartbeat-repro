# frozen_string_literal: true

require "temporalio/workflow"
require "temporalio/retry_policy"

# Runs one activity by name, with a short heartbeat timeout and no retries, so a single attempt's fate is easy to read.
class ScenarioWorkflow < Temporalio::Workflow::Definition
  workflow_name "ScenarioWorkflow"

  def execute(scenario)
    Temporalio::Workflow.execute_activity(
      scenario.fetch("activity"),
      scenario.fetch("params"),
      task_queue: "repro-activities",
      start_to_close_timeout: 120,
      heartbeat_timeout: scenario.fetch("heartbeat_timeout"),
      retry_policy: Temporalio::RetryPolicy.new(max_attempts: 1)
    )
  end
end
