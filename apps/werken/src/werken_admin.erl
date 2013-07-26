-module(werken_admin).
-compile([{parse_transform, lager_transform}]).
-export([workers/0, status/0, version/0, shutdown/0, shutdown/1, maxqueue/0, check_connections/1]).

-include("records.hrl").

maxqueue() ->
  {text, "OK"}. % lulz

workers() ->
  lager:debug("AWWW SHIT INSIDE WORKERS() YO"),
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
  lager:debug("AllJobs = ~p", [AllJobs]),
  AllWorkers = werken_storage_worker:all_worker_functions(),
  lager:debug("AllWorkers = ~p", [AllWorkers]),
  WorkerCounts = worker_counts(AllWorkers, dict:new()),
  lager:debug("WorkerCounts = ~p", [WorkerCounts]),
  StartingDict = setup_initial_data(WorkerCounts),
  lager:debug("StartingDict = ~p", [StartingDict]),
  Dict = job_statistics(AllJobs, WorkerCounts, StartingDict),
  lager:debug("Dict = ~p", [Dict]),
  Data = lists:map(fun({FunctionName, {Total, Running, Available}}) ->
          [FunctionName, 9, integer_to_list(Total), 9, integer_to_list(Running), 9, integer_to_list(Available)]
      end, dict:to_list(Dict)),
  lager:debug("Data = ~p", [Data]),
  NewData = string:join(Data, "\n"),
  lager:debug("NewData = ~p", [NewData]),
  Result = io_lib:format("~s~n.~n", [NewData]),
  lager:debug("Result = ~p", [Result]),
  {text, Result}.

version() ->
  Vsn = werken_app:version(),
  Data = ["OK ", Vsn],
  Result = io_lib:format("~s~n", [Data]),
  {text, Result}.

shutdown() ->
  timer:apply_after(1000, werken, stop, []),
  return_ok().

shutdown("graceful") -> % shutdown will happen in 30 seconds whether it wants to or not
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
  lager:debug("got an empty list, gonna return Dict = ~p", [Dict]),
  Dict;

job_statistics([#job_function{function_name=FN}|Rest], WorkerCounts, Dict) ->
  lager:debug("function_name = ~p, Rest = ~p, WorkerCounts = ~p, Dict = ~p", [FN, Rest, WorkerCounts, Dict]),
  T = case dict:find(FN, Dict) of
    {ok, Value} ->
      lager:debug("Value = ~p", [Value]),
      statistics_tuple(Value, FN, WorkerCounts);
    error ->
      lager:debug("got an error because ~p is not in Dict ~p", [FN, Dict]),
      statistics_tuple({0, 0, 0}, FN, WorkerCounts)
  end,
  lager:debug("T = ~p", [T]),
  NewDict = dict:store(FN, T, Dict),
  lager:debug("NewDict = ~p", [NewDict]),
  job_statistics(Rest, WorkerCounts, NewDict).

statistics_tuple({Total, Running, AvailableWorkers}, FunctionName, WorkerCounts) ->
  lager:debug("Total = ~p, Running = ~p, AvailableWorkers = ~p, FunctionName = ~p, WorkerCounts = ~p", [Total, Running, AvailableWorkers, FunctionName, WorkerCounts]),
  C = worker_count_for_function_name(FunctionName, WorkerCounts),
  NewRunning = case werken_storage_job:is_job_running({function_name, FunctionName}) of
    true -> Running + 1;
    _ -> Running
  end,
  lager:debug("C = ~p", [C]),
  {Total+1, NewRunning, C}.

worker_counts([], Dict) ->
  lager:debug("got an empty list, returning Dict ~p", [Dict]),
  Dict;

worker_counts([#worker_function{function_name=FN}|Rest], Dict) ->
  lager:debug("function_name = ~p, Rest = ~p, Dict = ~p", [FN, Rest, Dict]),
  C = case dict:find(FN, Dict) of
    {ok, Count} -> Count+1;
    error -> 1
  end,
  lager:debug("C = ~p", [C]),
  NewDict = dict:store(FN, C, Dict),
  lager:debug("NewDict = ~p", [NewDict]),
  worker_counts(Rest, NewDict).

worker_count_for_function_name(FunctionName, WorkerCounts) ->
  X = case dict:find(FunctionName, WorkerCounts) of
    {ok, Count} -> Count;
    error -> 0
  end,
  lager:debug("FunctionName = ~p, WorkerCounts = ~p, X = ~p", [FunctionName, WorkerCounts, X]),
  X.

tell_children_to_stop_listening([]) ->
  ok;

tell_children_to_stop_listening([P|Rest]) ->
  gen_server:cast(P, close_socket),
  tell_children_to_stop_listening(Rest).

setup_initial_data(WorkerCounts) ->
  dict:map(fun(_Key, Val) -> {0, 0, Val} end, WorkerCounts).
