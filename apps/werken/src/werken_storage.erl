-module(werken_storage).
-include("records.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-export([init/0]).
-export([add_job/1, get_job/1, delete_job/1]).
-export([add_client/1, delete_client/1]).
-export([add_worker/1, list_workers/0, delete_worker/1, get_worker_pids_for_function_name/1, get_worker_status/1, get_worker_function_names_for_pid/1]).

init() ->
  Tables = [
        {jobs, [set, named_table, {keypos, #job.job_id}]},
        {clients, [set, named_table, {keypos, #client.pid}]},
        {worker_functions, [bag, named_table, {keypos, #worker_function.pid}]},
        {worker_statuses, [set, named_table, {keypos, #worker_status.pid}]},
        {workers, [set, named_table, {keypos, #worker.pid}]}
      ],
  create_tables(Tables).

add_job(Job) ->
  case ets:insert(jobs, Job) of
    false -> duplicate_job;
    _ -> ok
  end.

get_job(Pid) when is_pid(Pid) ->
  Workers1 = ets:tab2list(workers),
  Workers2 = ets:tab2list(worker_statuses),
  Workers3 = ets:tab2list(worker_functions),
  io:format("werken_storage. get_job/pid Pid = ~p~n", [Pid]),
  io:format("werken_storage. get_job/pid workers 1 = ~p~n", [Workers1]),
  io:format("werken_storage. get_job/pid workers 2 = ~p~n", [Workers2]),
  io:format("werken_storage. get_job/pid workers 3 = ~p~n", [Workers3]),
  X = case ets:lookup(worker_functions, Pid) of
    [] -> [];
    Workers ->
      FunctionNames = lists:map(fun(W) -> W#worker_function.function_name end, Workers),
      io:format("werken_storage. get_job/pid FunctionNames = ~p~n", [FunctionNames]),
      get_job(FunctionNames, [high, normal, low])
  end,
  io:format("werken_storage. get_job/pid X = ~p~n", [X]),
  X;

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

add_client(Client) ->
  case ets:insert(clients, Client) of
    false -> duplicate_client;
    _ -> ok
  end.

delete_client(Pid) when is_pid(Pid) ->
  ets:delete(clients, Pid);

delete_client(ClientId) ->
  ets:match_delete(clients, {'_', '_', ClientId, '_'}).

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
  WorkerList = ets:tab2list(workers),
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
      io:format("tried to find some function names for pid ~p, failed. got [].~n", [Pid]);
    Workers ->
      io:format("found some worker function name(s) = ~p~n", [Workers]),
      FunctionNames = lists:map(fun(W) -> W#worker_function.function_name end, Workers),
      io:format("just gonna return the function names ~p~n", [FunctionNames]),
      FunctionNames
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
  [Status] = ets:lookup(worker_statuses, Pid),
  Status.

% private functions
create_tables([]) ->
  ok;

create_tables([{Name,Attributes}|Rest]) ->
  case ets:info(Name) of
    undefined -> ets:new(Name, Attributes);
    _ -> ok
  end,
  create_tables(Rest).
