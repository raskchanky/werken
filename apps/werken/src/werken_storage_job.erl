-module(werken_storage_job).
-export([add_job/1, get_job/1, delete_job/1]).

-include("records.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

add_job(Job) ->
  case ets:insert(jobs, Job) of
    false -> duplicate_job;
    _ -> ok
  end.

get_job(Pid) when is_pid(Pid) ->
  Workers1 = ets:tab2list(workers),
  Workers2 = ets:tab2list(worker_statuses),
  Workers3 = ets:tab2list(worker_functions),
  io:format("werken_storage_job. get_job/pid Pid = ~p~n", [Pid]),
  io:format("werken_storage_job. get_job/pid workers 1 = ~p~n", [Workers1]),
  io:format("werken_storage_job. get_job/pid workers 2 = ~p~n", [Workers2]),
  io:format("werken_storage_job. get_job/pid workers 3 = ~p~n", [Workers3]),
  X = case ets:lookup(worker_functions, Pid) of
    [] -> [];
    Workers ->
      FunctionNames = lists:map(fun(W) -> W#worker_function.function_name end, Workers),
      io:format("werken_storage_job. get_job/pid FunctionNames = ~p~n", [FunctionNames]),
      get_job(FunctionNames, [high, normal, low])
  end,
  io:format("werken_storage_job. get_job/pid X = ~p~n", [X]),
  X;

get_job(JobHandle) when is_binary(JobHandle) ->
  NewJobHandle = binary_to_list(JobHandle),
  get_job(NewJobHandle);

get_job(JobHandle) ->
  case ets:lookup(jobs, JobHandle) of
    [] -> [];
    [Job] -> Job
  end.

get_job(_, []) ->
  io:format("aw shit. no priorities left. what happens now?~n"),
  [];

get_job(FunctionNames, [Priority|OtherPriorities]) ->
  io:format("get_job, FunctionNames = ~p, Priority = ~p, OtherPriorities = ~p~n", [FunctionNames, Priority, OtherPriorities]),
  case get_job(FunctionNames, Priority) of
    [] ->
      io:format("tried to get jobs with FunctionNames = ~p and Priority = ~p and it failed. Trying with OtherPriorities = ~p now~n", [FunctionNames, Priority, OtherPriorities]),
      get_job(FunctionNames, OtherPriorities);
    Job ->
      io:format("succeeded in getting Job = ~p~n", [Job]),
      Job
  end;

get_job([], Priority) when is_atom(Priority) ->
  io:format("bummer. out of jobs for Priority = ~p~n", [Priority]),
  [];

get_job([FunctionName|OtherFunctionNames], Priority) when is_atom(Priority) ->
  io:format("get_job, FunctionName = ~p, OtherFunctionNames = ~p, Priority = ~p~n", [FunctionName, OtherFunctionNames, Priority]),
  MatchSpec = ets:fun2ms(fun(J = #job{function_name=F, priority=P}) when F == FunctionName andalso P == Priority -> J end),
  case ets:select(jobs, MatchSpec) of
    [] ->
      io:format("tried to find a job, failed. got []. trying with ~p now~n", [OtherFunctionNames]),
      get_job(OtherFunctionNames, Priority);
    [Job] ->
      io:format("FOUND A JOB! Job = ~p~n", [Job]),
      Job
  end.

delete_job(JobHandle) ->
  ets:delete(jobs, JobHandle).
