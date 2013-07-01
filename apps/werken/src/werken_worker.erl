-module(werken_worker).
-compile([{parse_transform, lager_transform}]).
-include("records.hrl").

%% API
-export([can_do/1, cant_do/1, reset_abilities/0, pre_sleep/0, grab_job/0,
work_status/3, work_complete/2, work_fail/1, set_client_id/0, set_client_id/1,
can_do_timeout/2, all_yours/0, work_exception/2, work_data/2,
work_warning/2, grab_job_uniq/0]).

can_do(FunctionName) ->
  WorkerFunction = #worker_function{pid = self(), function_name = FunctionName},
  WorkerStatus = #worker_status{pid = self(), status = awake},
  werken_storage_worker:add_worker(WorkerFunction),
  werken_storage_worker:add_worker(WorkerStatus),
  lager:debug("HI THERE"),
  case werken_storage_worker:get_worker_id_for_pid(self()) of
    [] -> set_client_id();
    _ -> ok
  end,
  ok.

cant_do(FunctionName) ->
  werken_storage_worker:remove_function_from_worker(FunctionName, self()),
  ok.

reset_abilities() ->
  werken_storage_worker:remove_function_from_worker(all, self()),
  ok.

pre_sleep() ->
  WorkerStatus = #worker_status{pid = self(), status = asleep},
  werken_storage_worker:add_worker(WorkerStatus),
  ok.

grab_job() ->
  case werken_storage_job:get_job(self()) of
    [] ->
      {binary, ["NO_JOB"]};
    JobFunction ->
      Job = werken_storage_job:get_job_for_job_function(JobFunction),
      {binary, ["JOB_ASSIGN", JobFunction#job_function.job_id, JobFunction#job_function.function_name, Job#job.data]}
  end.

work_status(JobHandle, Numerator, Denominator) ->
  forward_packet_to_client("WORK_STATUS", [JobHandle, Numerator, Denominator]),
  ok.

work_complete(JobHandle, Data) ->
  lager:debug("inside werken_worker, work_complete function. JobHandle = ~p, Data = ~p", [JobHandle, Data]),
  forward_packet_to_client("WORK_COMPLETE", [JobHandle, Data]),
  werken_storage_job:delete_job(JobHandle),
  ok.

work_fail(JobHandle) ->
  forward_packet_to_client("WORK_FAIL", [JobHandle]),
  ok.

set_client_id() ->
  Id = werken_utils:generate_worker_id(),
  lager:debug("Id ~p", [Id]),
  set_client_id(Id).

set_client_id(ClientId) ->
  Worker = #worker{pid = self(), worker_id = ClientId},
  lager:debug("Worker ~p", [Worker]),
  werken_storage_worker:add_worker(Worker),
  ok.

can_do_timeout(_FunctionName, _Timeout) ->
  ok.

all_yours() ->
  ok.

work_exception(JobHandle, Data) ->
  forward_packet_to_client("WORK_EXCEPTION", [JobHandle, Data]),
  ok.

work_data(JobHandle, Data) ->
  forward_packet_to_client("WORK_DATA", [JobHandle, Data]),
  ok.

work_warning(JobHandle, Data) ->
  forward_packet_to_client("WORK_WARNING", [JobHandle, Data]),
  ok.

grab_job_uniq() ->
  ok.

% private functions
notify_clients_if_necessary(Job, Packet) ->
  lager:debug("notify_clients_if_necessary. Job = ~p, Packet = ~p", [Job, Packet]),
  case Job#job.bg of
    false ->
      Pid = Job#job.client_pid,
      Func = fun() -> {binary, Packet} end,
      gen_server:call(Pid, {process_packet, Func});
      % gen_server:cast(Pid, {process_packet, Func});
    _ -> ok
  end.

forward_packet_to_client(Name, Args) ->
  lager:debug("forward_packet_to_client. Name = ~p, Args = ~p", [Name, Args]),
  JobHandle = hd(Args),
  Job = werken_storage_job:get_job(JobHandle),
  lager:debug("forward_packet_to_client. JobHandle = ~p, Job = ~p", [JobHandle, Job]),
  notify_clients_if_necessary(Job, [Name|Args]).
