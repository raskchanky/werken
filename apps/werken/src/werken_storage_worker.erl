-module(werken_storage_worker).
-export([add_worker/1, list_workers/0, delete_worker/1, get_worker_pids_for_function_name/1, get_worker_status/1, get_worker_function_names_for_pid/1, remove_function_from_worker/2, get_worker_id_for_pid/1, update_worker_status/2]).

-include("records.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

add_worker(#worker{pid=Pid, worker_id=WorkerId} = NewWorker) when is_pid(Pid) andalso is_list(WorkerId) ->
  io:format("inside add_worker when adding a worker_id. worker = ~p~n", [NewWorker]),
  ets:insert(workers, NewWorker),
  ok;

add_worker(#worker_status{pid=Pid, status=Status} = NewWorker) when is_pid(Pid) andalso is_atom(Status) ->
  io:format("inside add_worker when adding a status. worker = ~p~n", [NewWorker]),
  ets:insert(worker_statuses, NewWorker),
  ok;

add_worker(#worker_function{pid=Pid, function_name=FunctionName} = NewWorker) when is_pid(Pid) andalso is_list(FunctionName) ->
  io:format("inside add_worker when adding a function. worker = ~p~n", [NewWorker]),
  ets:insert(worker_functions, NewWorker),
  ok.

list_workers() ->
  F = fun(#worker{worker_id=W1}, #worker{worker_id=W2}) -> W1 =< W2 end,
  WorkerList = lists:sort(F, ets:tab2list(workers)),
  lists:map(fun(#worker{pid=Pid, worker_id=WorkerId}) ->
    FunctionNames = get_worker_function_names_for_pid(Pid),
    {ok, Socket} = gen_server:call(Pid, get_socket),
    {ok, {Ip, _Port}} = inet:peername(Socket),
    {["0", inet_parse:ntoa(Ip), WorkerId], FunctionNames}
  end, WorkerList).

delete_worker(Pid) when is_pid(Pid) ->
  ets:delete(workers, Pid),
  ets:delete(worker_statuses, Pid),
  ets:delete(worker_functions, Pid),
  ok.

get_worker_function_names_for_pid(Pid) when is_pid(Pid) ->
  case ets:lookup(worker_functions, Pid) of
    [] ->
      io:format("tried to find some function names for pid ~p, failed. got [].~n", [Pid]),
      [];
    Workers ->
      io:format("found some worker function name(s) = ~p~n", [Workers]),
      FunctionNames = lists:map(fun(W) -> W#worker_function.function_name end, Workers),
      SortedFunctionNames = lists:sort(FunctionNames),
      io:format("just gonna return the function names ~p~n", [SortedFunctionNames]),
      SortedFunctionNames
  end.

get_worker_pids_for_function_name(FunctionName) ->
  MatchSpec = ets:fun2ms(fun(W = #worker_function{function_name=F}) when F == FunctionName -> W end),
  case ets:select(worker_functions, MatchSpec) of
    [] ->
      io:format("tried to find a worker for FunctionName ~p, failed. got [].~n", [FunctionName]);
    Workers ->
      io:format("FOUND SOME WORKERS! Worker(s) = ~p~n", [Workers]),
      Pids = lists:map(fun(W) -> W#worker_function.pid end, Workers),
      io:format("just gonna return the pids ~p~n", [Pids]),
      Pids
  end.

get_worker_status(Pid) when is_pid(Pid) ->
  io:format("gonna check the status of me, pid = ~p~n", [Pid]),
  X = case ets:lookup(worker_statuses, Pid) of
    [] -> [];
    [Status] -> Status
  end,
  io:format("and the result was ~p~n", [X]),
  X.

update_worker_status(Pid, Status) when is_pid(Pid) ->
  io:format("gonna update a worker, pid = ~p, status = ~p~n", [Pid, Status]),
  WorkerStatus = #worker_status{pid = Pid, status = Status},
  ets:insert(worker_statuses, WorkerStatus),
  ok.

get_worker_id_for_pid(Pid) when is_pid(Pid) ->
  io:format("gonna check the worker_id of me, pid = ~p~n", [Pid]),
  X = case ets:lookup(workers, Pid) of
    [] -> [];
    [WorkerId] -> WorkerId
  end,
  io:format("and the result was ~p~n", [X]),
  X.

remove_function_from_worker(all, Pid) when is_pid(Pid) ->
  ets:delete(worker_functions, Pid),
  ok;
 
remove_function_from_worker(FunctionName, Pid) when is_pid(Pid) ->
  io:format("going to try and delete a function from a worker. FunctionName = ~p, Pid = ~p~n", [FunctionName, Pid]),
  FullTable = ets:tab2list(worker_functions),
  io:format("right now, table looks like this: ~p~n", [FullTable]),
  MS = ets:fun2ms(fun(#worker_function{pid=P, function_name=F}) when F == FunctionName andalso P == Pid -> true end),
  Num = ets:select_delete(worker_functions, MS),
  io:format("num deleted = ~p~n", [Num]),
  FullTable1 = ets:tab2list(worker_functions),
  io:format("just finished the delete. now the table looks like this: ~p~n", [FullTable1]),
  ok.
