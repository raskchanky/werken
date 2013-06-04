-module(admin_SUITE).
-include_lib("common_test/include/ct.hrl").
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([workers/1]).

all() -> [workers].

init_per_testcase(_, Config) ->
  application:start(werken),
  {ok, _Pid} = werken_sup:start_link(),
  Config.

end_per_testcase(_, Config) ->
  application:stop(werken),
  Config.
 
workers(_Config) ->
  {ok, WorkerSocket1} = gearman_test_common:connect(),
  gearman_test_worker:set_client_id(WorkerSocket1, "worker1"),
  gearman_test_worker:can_do(WorkerSocket1, "reverse"),
  {ok, WorkerSocket2} = gearman_test_common:connect(),
  gearman_test_worker:set_client_id(WorkerSocket2, "worker2"),
  gearman_test_worker:can_do(WorkerSocket2, "some_job"),
  gearman_test_worker:can_do(WorkerSocket2, "some_other_job"),
  {ok, AdminSocket} = gearman_test_common:connect(),
  gearman_test_admin:workers(AdminSocket),
  {ok, WorkerOutput} = gearman_test_common:get_response(AdminSocket),
  WorkerOutput = <<"0 127.0.0.1 worker1 : reverse\n0 127.0.0.1 worker2 : some_job some_other_job\n.\n">>,
  gearman_test_common:disconnect(WorkerSocket1),
  gearman_test_common:disconnect(WorkerSocket2),
  gearman_test_common:disconnect(AdminSocket).
