-module(werken_client).
-include("records.hrl").

%% API
-export([submit_job/3, get_status/1, submit_job_bg/3, submit_job_high/3,
submit_job_high_bg/3, submit_job_low/3, submit_job_low_bg/3, submit_job_sched/3,
submit_job_epoch/3, option_req/1]).

%% API
submit_job(FunctionName, UniqueId, Data) ->
  submit_job(FunctionName, UniqueId, Data, normal, false).

submit_job(FunctionName, UniqueId, Data, Priority, Bg) ->
  JobId = werken_utils:generate_job_id(),
  Job = #job{job_id = JobId,
              function_name = FunctionName,
              data = Data,
              submitted_at = erlang:now(),
              unique_id = UniqueId,
              client_pid = self(),
              priority = Priority,
              bg = Bg},
  Client = #client{function_name = FunctionName,
              data = Data},
  gen_server:call(werken_coordinator, {add_client, Client}),
  gen_server:call(werken_coordinator, {add_job, Job}),
  {binary, ["JOB_CREATED", JobId]}.

submit_job_high(FunctionName, UniqueId, Data) ->
  submit_job(FunctionName, UniqueId, Data, high, false).

submit_job_low(FunctionName, UniqueId, Data) ->
  submit_job(FunctionName, UniqueId, Data, low, false).

submit_job_bg(FunctionName, UniqueId, Data) ->
  submit_job(FunctionName, UniqueId, Data, normal, true).

submit_job_high_bg(FunctionName, UniqueId, Data) ->
  submit_job(FunctionName, UniqueId, Data, high, true).

submit_job_low_bg(FunctionName, UniqueId, Data) ->
  submit_job(FunctionName, UniqueId, Data, low, true).

get_status(_JobHandle) ->
  ok.

option_req(_Option) ->
  ok.

submit_job_sched(_FunctionName, _UniqueId, _Data) ->
  ok.

submit_job_epoch(_FunctionName, _UniqueId, _Data) ->
  ok.
