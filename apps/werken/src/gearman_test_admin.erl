-module(gearman_test_admin).
-export([workers/1]).

workers(Socket) ->
  gen_tcp:send(Socket, "workers").
