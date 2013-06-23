-module(werken_client).
-include("records.hrl").

%% API
-export([submit_job/3, get_status/1, submit_job_bg/3, submit_job_high/3,
submit_job_high_bg/3, submit_job_low/3, submit_job_low_bg/3, submit_job_sched/8,
submit_job_epoch/4, option_req/1]).

-export([generate_records_and_insert_job/7]).

%% API
submit_job(FunctionName, UniqueId, Data) ->
  submit_job(FunctionName, UniqueId, Data, normal, false).

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

submit_job_sched(FunctionName, UniqueId, Minute, Hour, DayOfMonth, Month, DayOfWeek, Data) ->
  Time = werken_utils:date_to_milliseconds(Minute, Hour, DayOfMonth, Month, DayOfWeek),
  submit_job(FunctionName, UniqueId, Data, normal, true, Time).

submit_job_epoch(FunctionName, UniqueId, Epoch, Data) ->
  Time = werken_utils:epoch_to_milliseconds(Epoch),
  submit_job(FunctionName, UniqueId, Data, normal, true, Time).

get_status(_JobHandle) ->
  ok.

option_req(_Option) ->
  ok.

% private
submit_job(FunctionName, UniqueId, Data, Priority, Bg) ->
  F = fun(JobId, ClientPid) -> apply(?MODULE, generate_records_and_insert_job, [FunctionName, UniqueId, Data, Priority, Bg, JobId, ClientPid]) end,
  submit_job(F).

submit_job(FunctionName, UniqueId, Data, Priority, Bg, Time) ->
  F = fun(JobId, ClientPid) -> timer:apply_after(Time, ?MODULE, generate_records_and_insert_job, [FunctionName, UniqueId, Data, Priority, Bg, JobId, ClientPid]) end,
  submit_job(F).

submit_job(Func) when is_function(Func) ->
  JobId = werken_utils:generate_job_id(),
  ClientPid = self(),
  spawn(fun() -> Func(JobId, ClientPid) end),
  job_created_packet(JobId).

job_created_packet(JobId) ->
  {binary, ["JOB_CREATED", JobId]}.

generate_records_and_insert_job(FunctionName, UniqueId, Data, Priority, Bg, JobId, ClientPid) ->
  Job = #job{job_id = JobId,
             data = Data,
             submitted_at = erlang:now(),
             unique_id = UniqueId,
             client_pid = ClientPid,
             bg = Bg},
  JobFunction = #job_function{job_id = JobId,
                              priority = Priority,
                              available = true,
                              function_name = FunctionName},
  Client = #client{pid = ClientPid,
                   function_name = FunctionName,
                   data = Data},
  werken_storage_client:add_client(Client),
  werken_storage_job:add_job(Job),
  werken_storage_job:add_job(JobFunction),
  spawn(fun() -> wakeup_workers_for_job(JobFunction) end),
  ok.

wakeup_workers_for_job(JobFunction) ->
  io:format("wakeup_workers_for_job, JobFunction = ~p~n", [JobFunction]),
  Pids = werken_storage_worker:get_worker_pids_for_function_name(JobFunction#job_function.function_name),
  io:format("wakeup_workers_for_job, Pids = ~p~n", [Pids]),
  wakeup_workers(Pids).

wakeup_workers([]) ->
  io:format("wakeup_workers, all out of workers. bye bye~n"),
  ok;

wakeup_workers([Pid|Rest]) ->
  io:format("wakeup_workers, Pid = ~p, Rest = ~p~n", [Pid, Rest]),
  Record = werken_storage_worker:get_worker_status(Pid),
  io:format("wakeup_workers, Record = ~p~n", [Record]),
  case Record#worker_status.status of
    asleep ->
      gen_server:call(Pid, wakeup_worker),
      werken_storage_worker:update_worker_status(Pid, awake);
    _ ->
      ok
  end,
  wakeup_workers(Rest).
