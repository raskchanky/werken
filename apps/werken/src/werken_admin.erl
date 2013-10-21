-module(werken_admin).
-compile([{parse_transform, lager_transform}]).
-export([workers/0, status/0, version/0, shutdown/0, shutdown/1, maxqueue/0, check_connections/1]).

-include("records.hrl").

maxqueue() ->
  {text, "OK"}. % lulz

workers() ->
  Workers = werken_storage_worker:list_workers(),
  Result = case Workers of
    [] ->
      ".\n";
    _ ->
      Data = lists:map(fun(X) ->
            {Info, FunctionNames} = X,
            Y = [string:join(Info, " "), string:join(FunctionNames, " ")],
            string:join(Y, " : ")
        end, Workers),
      NewData = string:join(Data, "\n"),
      io_lib:format("~s~n.~n", [NewData])
  end,
  {text, Result}.

status() ->
  AllJobs = werken_storage_job:all_jobs(),
  AllWorkers = werken_storage_worker:all_worker_functions(),
  WorkerCounts = worker_counts(AllWorkers, dict:new()),
  StartingDict = setup_initial_data(WorkerCounts),
  Dict = job_statistics(AllJobs, WorkerCounts, StartingDict),
  Data = lists:map(fun({FunctionName, {Total, Running, Available}}) ->
          [FunctionName, 9, integer_to_list(Total), 9, integer_to_list(Running), 9, integer_to_list(Available)]
      end, dict:to_list(Dict)),
  NewData = string:join(Data, "\n"),
  Result = io_lib:format("~s~n.~n", [NewData]),
  {text, Result}.

version() ->
  Vsn = werken_app:version(),
  Data = ["OK ", Vsn],
  Result = io_lib:format("~s~n", [Data]),
  {text, Result}.

shutdown() ->
  timer:apply_after(1000, werken, stop, []),
  return_ok().

shutdown("graceful") ->
  Children = supervisor:which_children(werken_connection_sup),
  Pids = lists:map(fun({_, Pid, _, _}) -> Pid end, Children),
  tell_children_to_stop_listening(Pids),
  check_connections(30).

check_connections(0) ->
  shutdown();

check_connections(Tries) ->
  [_, {active, C}, _, _] = supervisor:count_children(werken_connection_sup),
  case C of
    0 -> shutdown();
    _ ->
      NewTries = Tries - 1,
      timer:apply_after(1000, ?MODULE, check_connections, [NewTries])
  end.

% private
return_ok() ->
  Result = io_lib:format("~s~n", ["OK"]),
  {text, Result}.

job_statistics([], _, Dict) ->
  Dict;

job_statistics([#job_function{function_name=FN}|Rest], WorkerCounts, Dict) ->
  T = case dict:find(FN, Dict) of
    {ok, Value} ->
      statistics_tuple(Value, FN, WorkerCounts);
    error ->
      statistics_tuple({0, 0, 0}, FN, WorkerCounts)
  end,
  NewDict = dict:store(FN, T, Dict),
  job_statistics(Rest, WorkerCounts, NewDict).

statistics_tuple({Total, Running, _AvailableWorkers}, FunctionName, WorkerCounts) ->
  C = worker_count_for_function_name(FunctionName, WorkerCounts),
  NewRunning = case werken_storage_job:is_job_running({function_name, FunctionName}) of
    true -> Running + 1;
    _ -> Running
  end,
  {Total+1, NewRunning, C}.

worker_counts([], Dict) ->
  Dict;

worker_counts([#worker_function{function_name=FN}|Rest], Dict) ->
  C = case dict:find(FN, Dict) of
    {ok, Count} -> Count+1;
    error -> 1
  end,
  NewDict = dict:store(FN, C, Dict),
  worker_counts(Rest, NewDict).

worker_count_for_function_name(FunctionName, WorkerCounts) ->
  X = case dict:find(FunctionName, WorkerCounts) of
    {ok, Count} -> Count;
    error -> 0
  end,
  X.

tell_children_to_stop_listening([]) ->
  ok;

tell_children_to_stop_listening([P|Rest]) ->
  gen_server:cast(P, close_socket),
  tell_children_to_stop_listening(Rest).

setup_initial_data(WorkerCounts) ->
  dict:map(fun(_Key, Val) -> {0, 0, Val} end, WorkerCounts).
