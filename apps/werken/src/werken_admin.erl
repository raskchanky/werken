-module(werken_admin).
-compile([{parse_transform, lager_transform}]).
-export([workers/0, status/0, version/0, shutdown/0, shutdown/1]).

-include("records.hrl").

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
  lager:debug("werken_admin/status, AllJobs = ~p", [AllJobs]),
  AllWorkers = werken_storage_worker:all_worker_functions(),
  lager:debug("werken_admin/status, AllWorkers = ~p", [AllWorkers]),
  WorkerCounts = worker_counts(AllWorkers, dict:new()),
  lager:debug("werken_admin/status, WorkerCounts = ~p", [WorkerCounts]),
  StartingDict = setup_initial_data(WorkerCounts),
  lager:debug("werken_admin/status, StartingDict = ~p", [StartingDict]),
  Dict = job_statistics(AllJobs, WorkerCounts, StartingDict),
  lager:debug("werken_admin/status, Dict = ~p", [Dict]),
  Data = lists:map(fun({FunctionName, {Total, Running, Available}}) ->
          [FunctionName, 9, integer_to_list(Total), 9, integer_to_list(Running), 9, integer_to_list(Available)]
      end, dict:to_list(Dict)),
  lager:debug("werken_admin/status, Data = ~p", [Data]),
  NewData = string:join(Data, "\n"),
  lager:debug("werken_admin/status, NewData = ~p", [NewData]),
  Result = io_lib:format("~s~n.~n", [NewData]),
  lager:debug("werken_admin/status, Result = ~p", [Result]),
  {text, Result}.

version() ->
  Vsn = werken_app:version(),
  Data = ["OK ", Vsn],
  Result = io_lib:format("~s~n", [Data]),
  {text, Result}.

shutdown() ->
  timer:apply_after(1000, werken, stop, []),
  return_ok().

shutdown("testing") ->
  return_ok();

shutdown("graceful") ->
  %% this will be tricky. essentially, we need to:
  %% 1. close the listening socket
  %% 2. stop the supervisor from creating any new children after sockets close
  %% 3. wait for each child to complete its interaction and clean up after itself (see terminate/2 for gen_server)
  %% 4. kill all children
  %% 5. kill supervisors
  %% https://github.com/agner/agner/blob/master/src/agner_app.erl
  %% http://engineering.yakaz.com/2011/09/erlangotp-supervisors-corner-cases.html
  shutdown(). % this is just temporary

% private
return_ok() ->
  Result = io_lib:format("~s~n", ["OK"]),
  {text, Result}.

job_statistics([], _, Dict) ->
  lager:debug("werken_admin/job_statistics, got an empty list, gonna return Dict = ~p", [Dict]),
  Dict;

job_statistics([#job_function{function_name=FN, available=A}|Rest], WorkerCounts, Dict) ->
  lager:debug("werken_admin/job_statistics, function_name = ~p, available = ~p, Rest = ~p, WorkerCounts = ~p, Dict = ~p", [FN, A, Rest, WorkerCounts, Dict]),
  T = case dict:find(FN, Dict) of
    {ok, Value} ->
      lager:debug("werken_admin/job_statistics, Value = ~p", [Value]),
      statistics_tuple(Value, FN, WorkerCounts, A);
    error ->
      lager:debug("werken_admin/job_statistics, got an error because ~p is not in Dict ~p", [FN, Dict]),
      statistics_tuple({0, 0, 0}, FN, WorkerCounts, A)
  end,
  lager:debug("werken_admin/job_statistics, T = ~p", [T]),
  NewDict = dict:store(FN, T, Dict),
  lager:debug("werken_admin/job_statistics, NewDict = ~p", [NewDict]),
  job_statistics(Rest, WorkerCounts, NewDict).

statistics_tuple({Total, Running, AvailableWorkers}, FunctionName, WorkerCounts, true) ->
  lager:debug("werken_admin/statistics_tuple/true, Total = ~p, Running = ~p, AvailableWorkers = ~p, FunctionName = ~p, WorkerCounts = ~p", [Total, Running, AvailableWorkers, FunctionName, WorkerCounts]),
  C = worker_count_for_function_name(FunctionName, WorkerCounts),
  lager:debug("werken_admin/statistics_tuple/true, C = ~p", [C]),
  {Total+1, Running+1, C};

statistics_tuple({Total, Running, AvailableWorkers}, FunctionName, WorkerCounts, false) ->
  lager:debug("werken_admin/statistics_tuple/false, Total = ~p, Running = ~p, AvailableWorkers = ~p, FunctionName = ~p, WorkerCounts = ~p", [Total, Running, AvailableWorkers, FunctionName, WorkerCounts]),
  C = worker_count_for_function_name(FunctionName, WorkerCounts),
  lager:debug("werken_admin/statistics_tuple/false, C = ~p", [C]),
  {Total+1, Running, C}.

worker_counts([], Dict) ->
  lager:debug("werken_admin/worker_counts, got an empty list, returning Dict ~p", [Dict]),
  Dict;

worker_counts([#worker_function{function_name=FN}|Rest], Dict) ->
  lager:debug("werken_admin/worker_counts, function_name = ~p, Rest = ~p, Dict = ~p", [FN, Rest, Dict]),
  C = case dict:find(FN, Dict) of
    {ok, Count} -> Count+1;
    error -> 1
  end,
  lager:debug("werken_admin/worker_counts, C = ~p", [C]),
  NewDict = dict:store(FN, C, Dict),
  lager:debug("werken_admin/worker_counts, NewDict = ~p", [NewDict]),
  worker_counts(Rest, NewDict).

worker_count_for_function_name(FunctionName, WorkerCounts) ->
  X = case dict:find(FunctionName, WorkerCounts) of
    {ok, Count} -> Count;
    error -> 0
  end,
  lager:debug("werken_admin/worker_count_for_function_name, FunctionName = ~p, WorkerCounts = ~p, X = ~p", [FunctionName, WorkerCounts, X]),
  X.

setup_initial_data(WorkerCounts) ->
  dict:map(fun(_Key, Val) -> {0, 0, Val} end, WorkerCounts).
