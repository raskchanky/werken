-module(werken_worker).
-include("records.hrl").

%% API
-export([can_do/1, cant_do/1, reset_abilities/0, pre_sleep/0, grab_job/0,
work_status/3, work_complete/2, work_fail/1, set_client_id/0, set_client_id/1,
can_do_timeout/2, all_yours/0, work_exception/2, work_data/2,
work_warning/2, grab_job_uniq/0]).

can_do(FunctionName) ->
  Record = #worker{function_name = FunctionName, status = awake},
  add_worker(Record),
  ok.

cant_do(FunctionName) ->
  gen_server:cast(werken_coordinator, {remove_function_from_worker, FunctionName, self()}),
  ok.

reset_abilities() ->
  gen_server:cast(werken_coordinator, {remove_function_from_worker, all, self()}),
  ok.

pre_sleep() ->
  Record = #worker{status = asleep},
  add_worker(Record),
  ok.

grab_job() ->
  case gen_server:call(werken_coordinator, {get_job, self()}) of
    {ok, []} ->
      {binary, ["NO_JOB"]};
    {ok, Job} ->
      {binary, ["JOB_ASSIGN", Job#job.job_id, Job#job.function_name, Job#job.data]}
  end.

work_status(JobHandle, Numerator, Denominator) ->
  forward_packet_to_client("WORK_STATUS", [JobHandle, Numerator, Denominator]),
  ok.

work_complete(JobHandle, Data) ->
  forward_packet_to_client("WORK_COMPLETE", [JobHandle, Data]),
  gen_server:cast(werken_coordinator, {delete_job, JobHandle}),
  ok.

work_fail(JobHandle) ->
  forward_packet_to_client("WORK_FAIL", [JobHandle]),
  ok.

set_client_id() ->
  Id = werken_utils:generate_worker_id(),
  set_client_id(Id).

set_client_id(ClientId) ->
  Record = #worker{worker_id = ClientId},
  add_worker(Record),
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
  case Job#job.bg of
    false ->
      gen_server:cast(werken_coordinator, {respond_to_client, Job#job.client_pid, Packet});
    _ -> ok
  end.

get_job(JobHandle) ->
  {ok, Job} = gen_server:call(werken_coordinator, {get_job, JobHandle}),
  Job.

forward_packet_to_client(Name, Args) ->
  JobHandle = hd(Args),
  Job = get_job(JobHandle),
  notify_clients_if_necessary(Job, [Name|Args]).

add_worker(Worker) ->
  gen_server:cast(werken_coordinator, {add_worker, Worker}).
