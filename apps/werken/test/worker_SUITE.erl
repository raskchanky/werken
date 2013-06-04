-module(worker_SUITE).
-include_lib("common_test/include/ct.hrl").
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([cant_do/1, reset_abilities/1, work_complete/1, work_status/1, work_fail/1, work_exception/1, work_data/1, work_warning/1]).
 
all() -> [cant_do, reset_abilities, work_complete, work_status, work_fail, work_exception, work_data, work_warning].

init_per_testcase(_, Config) ->
  application:start(werken),
  {ok, _Pid} = werken_sup:start_link(),
  Config.

end_per_testcase(_, Config) ->
  application:stop(werken),
  Config.
 
cant_do(_Config) ->
  {ok, WorkerSocket} = gearman_test_common:connect(),
  gearman_test_worker:can_do(WorkerSocket, "reverse"),
  gearman_test_worker:can_do(WorkerSocket, "other_job"),
  timer:sleep(1000),
  Workers = werken_storage:list_workers(),
  [{_Info, Jobs}] = Workers,
  Jobs = ["other_job", "reverse"],
  gearman_test_worker:cant_do(WorkerSocket, "other_job"),
  timer:sleep(1000),
  NewWorkers = werken_storage:list_workers(),
  [{_NewInfo, NewJobs}] = NewWorkers,
  NewJobs = ["reverse"],
  gearman_test_common:disconnect(WorkerSocket).

reset_abilities(_Config) ->
  {ok, WorkerSocket} = gearman_test_common:connect(),
  gearman_test_worker:can_do(WorkerSocket, "reverse"),
  gearman_test_worker:can_do(WorkerSocket, "other_job"),
  timer:sleep(1000),
  Workers = werken_storage:list_workers(),
  [{_Info, Jobs}] = Workers,
  Jobs = ["other_job", "reverse"],
  gearman_test_worker:reset_abilities(WorkerSocket),
  timer:sleep(1000),
  NewWorkers = werken_storage:list_workers(),
  io:format("reset_abilities, NewWorkers = ~p~n", [NewWorkers]),
  [{_NewInfo, NewJobs}] = NewWorkers,
  NewJobs = [],
  gearman_test_common:disconnect(WorkerSocket).

work_complete(_Config) ->
  {ok, WorkerSocket} = gearman_test_common:connect(),
  gearman_test_worker:can_do(WorkerSocket, "reverse"),
  {ok, ClientSocket} = gearman_test_common:connect(),
  gearman_test_client:submit_job(ClientSocket, "reverse", "test"),
  {ok, R1} = gearman_test_common:get_response(ClientSocket),
  {ok, JobHandle} = gearman_test_client:job_created(R1),
  gearman_test_worker:work_complete(WorkerSocket, JobHandle, "tset"),
  {ok, R2} = gearman_test_common:get_response(ClientSocket),
  Size = werken_utils:size_or_length(JobHandle) + werken_utils:size_or_length("tset") + 1,
  R2 = iolist_to_binary([0, "RES", <<13:32/big>>, <<Size:32/big>>, JobHandle, 0, "tset"]),
  gearman_test_common:disconnect(WorkerSocket),
  gearman_test_common:disconnect(ClientSocket).

work_status(_Config) ->
  {ok, WorkerSocket} = gearman_test_common:connect(),
  gearman_test_worker:can_do(WorkerSocket, "reverse"),
  {ok, ClientSocket} = gearman_test_common:connect(),
  gearman_test_client:submit_job(ClientSocket, "reverse", "test"),
  {ok, R1} = gearman_test_common:get_response(ClientSocket),
  {ok, JobHandle} = gearman_test_client:job_created(R1),
  gearman_test_worker:work_status(WorkerSocket, JobHandle, "1", "2"),
  {ok, R2} = gearman_test_common:get_response(ClientSocket),
  Size = werken_utils:size_or_length(JobHandle) + 4, % "1", "2" and 2 null bytes makes 4
  R2 = iolist_to_binary([0, "RES", <<12:32/big>>, <<Size:32/big>>, JobHandle, 0, "1", 0, "2"]),
  gearman_test_common:disconnect(WorkerSocket),
  gearman_test_common:disconnect(ClientSocket).

work_fail(_Config) ->
  {ok, WorkerSocket} = gearman_test_common:connect(),
  gearman_test_worker:can_do(WorkerSocket, "reverse"),
  {ok, ClientSocket} = gearman_test_common:connect(),
  gearman_test_client:submit_job(ClientSocket, "reverse", "test"),
  {ok, R1} = gearman_test_common:get_response(ClientSocket),
  {ok, JobHandle} = gearman_test_client:job_created(R1),
  gearman_test_worker:work_fail(WorkerSocket, JobHandle),
  {ok, R2} = gearman_test_common:get_response(ClientSocket),
  Size = werken_utils:size_or_length(JobHandle),
  R2 = iolist_to_binary([0, "RES", <<14:32/big>>, <<Size:32/big>>, JobHandle]),
  gearman_test_common:disconnect(WorkerSocket),
  gearman_test_common:disconnect(ClientSocket).

work_exception(_Config) ->
  {ok, WorkerSocket} = gearman_test_common:connect(),
  gearman_test_worker:can_do(WorkerSocket, "reverse"),
  {ok, ClientSocket} = gearman_test_common:connect(),
  gearman_test_client:submit_job(ClientSocket, "reverse", "test"),
  {ok, R1} = gearman_test_common:get_response(ClientSocket),
  {ok, JobHandle} = gearman_test_client:job_created(R1),
  Exception = "exception",
  gearman_test_worker:work_exception(WorkerSocket, JobHandle, Exception),
  {ok, R2} = gearman_test_common:get_response(ClientSocket),
  Size = werken_utils:size_or_length(JobHandle) + werken_utils:size_or_length(Exception) + 1,
  R2 = iolist_to_binary([0, "RES", <<25:32/big>>, <<Size:32/big>>, JobHandle, 0, Exception]),
  gearman_test_common:disconnect(WorkerSocket),
  gearman_test_common:disconnect(ClientSocket).

work_data(_Config) ->
  {ok, WorkerSocket} = gearman_test_common:connect(),
  gearman_test_worker:can_do(WorkerSocket, "reverse"),
  {ok, ClientSocket} = gearman_test_common:connect(),
  gearman_test_client:submit_job(ClientSocket, "reverse", "test"),
  {ok, R1} = gearman_test_common:get_response(ClientSocket),
  {ok, JobHandle} = gearman_test_client:job_created(R1),
  Data = "data",
  gearman_test_worker:work_data(WorkerSocket, JobHandle, Data),
  {ok, R2} = gearman_test_common:get_response(ClientSocket),
  Size = werken_utils:size_or_length(JobHandle) + werken_utils:size_or_length(Data) + 1,
  R2 = iolist_to_binary([0, "RES", <<28:32/big>>, <<Size:32/big>>, JobHandle, 0, Data]),
  gearman_test_common:disconnect(WorkerSocket),
  gearman_test_common:disconnect(ClientSocket).

work_warning(_Config) ->
  {ok, WorkerSocket} = gearman_test_common:connect(),
  gearman_test_worker:can_do(WorkerSocket, "reverse"),
  {ok, ClientSocket} = gearman_test_common:connect(),
  gearman_test_client:submit_job(ClientSocket, "reverse", "test"),
  {ok, R1} = gearman_test_common:get_response(ClientSocket),
  {ok, JobHandle} = gearman_test_client:job_created(R1),
  Data = "data",
  gearman_test_worker:work_warning(WorkerSocket, JobHandle, Data),
  {ok, R2} = gearman_test_common:get_response(ClientSocket),
  Size = werken_utils:size_or_length(JobHandle) + werken_utils:size_or_length(Data) + 1,
  R2 = iolist_to_binary([0, "RES", <<29:32/big>>, <<Size:32/big>>, JobHandle, 0, Data]),
  gearman_test_common:disconnect(WorkerSocket),
  gearman_test_common:disconnect(ClientSocket).
