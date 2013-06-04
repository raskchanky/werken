-module(integration_SUITE).
-include_lib("common_test/include/ct.hrl").
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([end_to_end/1]).
 
all() -> [end_to_end].

init_per_testcase(_, Config) ->
  application:start(werken),
  {ok, _Pid} = werken_sup:start_link(),
  Config.

end_per_testcase(_, Config) ->
  application:stop(werken),
  Config.
 
end_to_end(_Config) ->
  {ok, WorkerSocket} = gearman_test_common:connect(),
  gearman_test_worker:can_do(WorkerSocket, "reverse"),
  gearman_test_worker:grab_job(WorkerSocket),
  {ok, R1} = gearman_test_common:get_response(WorkerSocket),
  R1 = gearman_test_worker:no_job(),
  gearman_test_worker:pre_sleep(WorkerSocket),
  {ok, ClientSocket} = gearman_test_common:connect(),
  gearman_test_client:submit_job(ClientSocket, "reverse", "normal test"),
  {ok, R2} = gearman_test_common:get_response(ClientSocket),
  {ok, JobHandle1} = gearman_test_client:job_created(R2),
  gearman_test_client:submit_job_low(ClientSocket, "reverse", "low test"),
  {ok, R3} = gearman_test_common:get_response(ClientSocket),
  {ok, JobHandle2} = gearman_test_client:job_created(R3),
  gearman_test_client:submit_job_high(ClientSocket, "reverse", "high test"),
  {ok, R4} = gearman_test_common:get_response(ClientSocket),
  {ok, JobHandle3} = gearman_test_client:job_created(R4),
  {ok, R5} = gearman_test_common:get_response(WorkerSocket),
  io:format("R5 = ~p~n", [R5]),
  R5 = gearman_test_worker:noop(),
  gearman_test_worker:grab_job(WorkerSocket),
  {ok, R6} = gearman_test_common:get_response(WorkerSocket),
  R6 = gearman_test_worker:job_assign(JobHandle3, "reverse", "high test"),
  gearman_test_worker:work_complete(WorkerSocket, JobHandle3, "tset hgih"),
  {ok, R7} = gearman_test_common:get_response(ClientSocket),
  R7 = gearman_test_client:work_complete(JobHandle3, "tset hgih"),
  gearman_test_worker:grab_job(WorkerSocket),
  {ok, R8} = gearman_test_common:get_response(WorkerSocket),
  R8 = gearman_test_worker:job_assign(JobHandle1, "reverse", "normal test"),
  gearman_test_worker:work_complete(WorkerSocket, JobHandle1, "tset lamron"),
  {ok, R9} = gearman_test_common:get_response(ClientSocket),
  R9 = gearman_test_client:work_complete(JobHandle1, "tset lamron"),
  gearman_test_worker:grab_job(WorkerSocket),
  {ok, R10} = gearman_test_common:get_response(WorkerSocket),
  R10 = gearman_test_worker:job_assign(JobHandle2, "reverse", "low test"),
  gearman_test_worker:work_complete(WorkerSocket, JobHandle2, "tset wol"),
  {ok, R11} = gearman_test_common:get_response(ClientSocket),
  R11 = gearman_test_client:work_complete(JobHandle2, "tset wol"),
  gearman_test_common:disconnect(WorkerSocket),
  gearman_test_common:disconnect(ClientSocket).
