-module(werken_storage_worker).
-compile([{parse_transform, lager_transform}]).
-export([add_worker/1, list_workers/0, delete_worker/1, get_worker_pids_for_function_name/1, get_worker_status/1, get_worker_function_names_for_pid/1, remove_function_from_worker/2, get_worker_id_for_pid/1, update_worker_status/2, all_worker_functions/0, get_worker_function/2]).

-include("records.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

all_worker_functions() ->
  ets:tab2list(worker_functions).

add_worker(#worker{pid=Pid, worker_id=WorkerId} = NewWorker) when is_pid(Pid) andalso is_list(WorkerId) ->
  lager:debug("worker = ~p", [NewWorker]),
  ets:insert(workers, NewWorker),
  ok;

add_worker(#worker_status{pid=Pid, status=Status} = NewWorker) when is_pid(Pid) andalso is_atom(Status) ->
  lager:debug("worker = ~p", [NewWorker]),
  ets:insert(worker_statuses, NewWorker),
  ok;

add_worker(#worker_function{pid=Pid, function_name=FunctionName} = NewWorker) when is_pid(Pid) andalso is_list(FunctionName) ->
  lager:debug("worker = ~p", [NewWorker]),
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
      lager:debug("tried to find some function names for pid ~p, failed. got [].", [Pid]),
      [];
    Workers ->
      lager:debug("found some worker function name(s) = ~p", [Workers]),
      FunctionNames = lists:map(fun(W) -> W#worker_function.function_name end, Workers),
      SortedFunctionNames = lists:sort(FunctionNames),
      lager:debug("just gonna return the function names ~p", [SortedFunctionNames]),
      SortedFunctionNames
  end.

get_worker_pids_for_function_name(FunctionName) ->
  MatchSpec = ets:fun2ms(fun(W = #worker_function{function_name=F}) when F == FunctionName -> W end),
  case ets:select(worker_functions, MatchSpec) of
    [] ->
      lager:debug("tried to find a worker for FunctionName ~p, failed. got [].", [FunctionName]);
    Workers ->
      lager:debug("FOUND SOME WORKERS! Worker(s) = ~p", [Workers]),
      Pids = lists:map(fun(W) -> W#worker_function.pid end, Workers),
      lager:debug("just gonna return the pids ~p", [Pids]),
      Pids
  end.

get_worker_status(Pid) when is_pid(Pid) ->
  lager:debug("gonna check the status of me, pid = ~p", [Pid]),
  X = case ets:lookup(worker_statuses, Pid) of
    [] -> [];
    [Status] -> Status
  end,
  lager:debug("and the result was ~p", [X]),
  X.

update_worker_status(Pid, Status) when is_pid(Pid) ->
  lager:debug("gonna update a worker, pid = ~p, status = ~p", [Pid, Status]),
  WorkerStatus = #worker_status{pid = Pid, status = Status},
  ets:insert(worker_statuses, WorkerStatus),
  ok.

get_worker_id_for_pid(Pid) when is_pid(Pid) ->
  lager:debug("gonna check the worker_id of me, pid = ~p", [Pid]),
  X = case ets:lookup(workers, Pid) of
    [] -> [];
    [WorkerId] -> WorkerId
  end,
  lager:debug("and the result was ~p", [X]),
  X.

get_worker_function(Pid, #job_function{function_name = FunctionName}) when is_pid(Pid) ->
  MatchSpec = ets:fun2ms(fun(W = #worker_function{function_name=F, pid=P}) when F == FunctionName andalso P == Pid -> W end),
  case ets:select(worker_functions, MatchSpec) of
    [] -> {error, no_worker_function};
    [WorkerFunction] -> WorkerFunction
  end.

remove_function_from_worker(all, Pid) when is_pid(Pid) ->
  ets:delete(worker_functions, Pid),
  ok;

remove_function_from_worker(FunctionName, Pid) when is_pid(Pid) ->
  lager:debug("going to try and delete a function from a worker. FunctionName = ~p, Pid = ~p", [FunctionName, Pid]),
  FullTable = ets:tab2list(worker_functions),
  lager:debug("right now, table looks like this: ~p", [FullTable]),
  MS = ets:fun2ms(fun(#worker_function{pid=P, function_name=F}) when F == FunctionName andalso P == Pid -> true end),
  Num = ets:select_delete(worker_functions, MS),
  lager:debug("num deleted = ~p", [Num]),
  FullTable1 = ets:tab2list(worker_functions),
  lager:debug("just finished the delete. now the table looks like this: ~p", [FullTable1]),
  ok.
