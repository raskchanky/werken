-module(werken_storage_job).
-compile([{parse_transform, lager_transform}]).
-export([add_job/1, get_job/1, delete_job/1, all_jobs/0, get_job_function_for_job/1, get_job_for_job_function/1]).

-include("records.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

all_jobs() ->
  ets:tab2list(job_functions).

add_job(Job=#job{}) ->
  case ets:insert_new(jobs, Job) of
    false -> duplicate_job;
    _ -> ok
  end;

add_job(JobFunction=#job_function{}) ->
  ets:insert(job_functions, JobFunction),
  ok.

get_job_function_for_job(Job) ->
  MatchSpec = ets:fun2ms(fun(J = #job_function{job_id=JI}) when JI == Job#job.job_id -> J end),
  case ets:select(job_functions, MatchSpec) of
    [] -> error;
    [JobFunction] -> JobFunction
  end.

get_job_for_job_function(JobFunction) ->
  MatchSpec = ets:fun2ms(fun(J = #job{job_id=JI}) when JI == JobFunction#job_function.job_id -> J end),
  case ets:select(jobs, MatchSpec) of
    [] -> error;
    [Job] -> Job
  end.

get_job(Pid) when is_pid(Pid) ->
  Workers1 = ets:tab2list(workers),
  Workers2 = ets:tab2list(worker_statuses),
  Workers3 = ets:tab2list(worker_functions),
  lager:debug("werken_storage_job. get_job/pid Pid = ~p", [Pid]),
  lager:debug("werken_storage_job. get_job/pid workers 1 = ~p", [Workers1]),
  lager:debug("werken_storage_job. get_job/pid workers 2 = ~p", [Workers2]),
  lager:debug("werken_storage_job. get_job/pid workers 3 = ~p", [Workers3]),
  X = case ets:lookup(worker_functions, Pid) of
    [] -> [];
    Workers ->
      FunctionNames = lists:map(fun(W) -> W#worker_function.function_name end, Workers),
      lager:debug("werken_storage_job. get_job/pid FunctionNames = ~p", [FunctionNames]),
      get_job(FunctionNames, [high, normal, low])
  end,
  lager:debug("werken_storage_job. get_job/pid X = ~p", [X]),
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
  lager:debug("aw shit. no priorities left. what happens now?"),
  [];

get_job(FunctionNames, [Priority|OtherPriorities]) ->
  lager:debug("get_job, FunctionNames = ~p, Priority = ~p, OtherPriorities = ~p", [FunctionNames, Priority, OtherPriorities]),
  case get_job(FunctionNames, Priority) of
    [] ->
      lager:debug("tried to get jobs with FunctionNames = ~p and Priority = ~p and it failed. Trying with OtherPriorities = ~p now", [FunctionNames, Priority, OtherPriorities]),
      get_job(FunctionNames, OtherPriorities);
    Job ->
      lager:debug("succeeded in getting Job = ~p", [Job]),
      Job
  end;

get_job([], Priority) when is_atom(Priority) ->
  lager:debug("bummer. out of jobs for Priority = ~p", [Priority]),
  [];

get_job([FunctionName|OtherFunctionNames], Priority) when is_atom(Priority) ->
  lager:debug("get_job, FunctionName = ~p, OtherFunctionNames = ~p, Priority = ~p", [FunctionName, OtherFunctionNames, Priority]),
  MatchSpec = ets:fun2ms(fun(J = #job_function{function_name=F, priority=P, available=true}) when F == FunctionName andalso P == Priority -> J end),
  case ets:select(job_functions, MatchSpec) of
    [] ->
      lager:debug("tried to find a job, failed. got []. trying with ~p now", [OtherFunctionNames]),
      get_job(OtherFunctionNames, Priority);
    JobFunctions ->
      lager:debug("FOUND JOB(S)! JobFunctions = ~p", [JobFunctions]),
      JobFunction = hd(JobFunctions),
      NewJobFunction = JobFunction#job_function{available = false},
      ets:insert(job_functions, NewJobFunction),
      NewJobFunction
  end.

delete_job(JobHandle) ->
  MS = ets:fun2ms(fun(#job_function{job_id=J}) when J == JobHandle -> true end),
  ets:select_delete(job_functions, MS),
  ets:delete(jobs, JobHandle),
  ok.
