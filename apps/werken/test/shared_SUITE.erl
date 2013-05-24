-module(shared_SUITE).
-include_lib("common_test/include/ct.hrl").
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([echo_req/1]).
 
all() -> [echo_req].

init_per_testcase(_, Config) ->
  application:start(werken),
  {ok, _Pid} = werken_sup:start_link(),
  Config.

end_per_testcase(_, Config) ->
  application:stop(werken),
  Config.
 
echo_req(_Config) ->
  {ok, Socket} = gearman_test_common:connect(),
  Data = "testing 123",
  gearman_test_common:echo_req(Socket, Data),
  {ok, R1} = gearman_test_common:get_response(Socket),
  R1 = gearman_test_common:echo_res(Data),
  gearman_test_common:disconnect(Socket).
