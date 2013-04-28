-module(werken_storage).
-include("records.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-export([init/0]).
-export([add_job/1, get_job/1, delete_job/1]).
-export([add_client/1, delete_client/1]).
-export([add_worker/1, list_workers/0, delete_worker/1]).

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

% GRAB_JOB from a worker
% get_job(Pid) when is_pid(Pid) ->
%   AllWorkers = ets:tab2list(workers),
%   io:format("werken_storage. get_job/pid Pid = ~p~n", [Pid]),
%   io:format("werken_storage. get_job/pid all workers = ~p~n", [AllWorkers]),
%   X = case ets:lookup(workers, Pid) of
%     [] -> [];
%     Workers ->
%       io:format("WEEEEEEEEEEE~n"),
%       FunctionNames = lists:map(fun(Worker) -> Worker#worker.function_name end, Workers),
%       get_job(FunctionNames, [high, normal, low])
%   end,
%   io:format("werken_storage. get_job/pid X = ~p~n", [X]),
%   X;
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
      io:format("WEEEEEEEEEEE~n"),
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
  [];

get_job(FunctionNames, [Priority|OtherPriorities]) ->
  case get_job(FunctionNames, Priority) of
    [] -> get_job(FunctionNames, OtherPriorities);
    [Job] -> Job
  end;

get_job([], Priority) when is_atom(Priority) ->
  [];

get_job([FunctionName|OtherFunctionNames], Priority) when is_atom(Priority) ->
  MatchSpec = ets:fun2ms(fun(J = #job{function_name=F, priority=P}) when F == FunctionName; P == Priority -> J end),
  case ets:select(jobs, MatchSpec) of
    [] -> get_job(OtherFunctionNames, Priority);
    [Job] -> Job
  end.

% get_job(Pid) when is_pid(Pid) ->
%   case ets:match_object(workers, {'_', '_', '_', Pid}) of
%     [] -> [];
%     Workers -> get_job(Workers, [high, normal, low])
%   end;

% get_job(JobHandle) ->
%   case ets:lookup(jobs, JobHandle) of
%     [] -> no_job;
%     [Job] -> Job
%   end.

% get_job(_, []) ->
%   no_job;

% get_job(Workers, [Priority|OtherPriorities]) ->
%   case get_job(Workers, Priority) of
%     no_job -> get_job(Workers, OtherPriorities);
%     Job -> Job
%   end;

% get_job([], Priority) when is_atom(Priority) ->
%   no_job;

% get_job([Worker|OtherWorkers], Priority) when is_atom(Priority) ->
%   Pattern = {'_', '_', '_', '_', Worker#worker.function_name, '_', '_', Priority, '_'},
%   case ets:match_object(jobs, Pattern) of
%     [] -> get_job(OtherWorkers, Priority);
%     [Job] -> Job
%   end.

delete_job(JobHandle) ->
  ets:delete(jobs, JobHandle).

add_client(Client) ->
  case ets:insert(clients, Client) of
    false -> duplicate_client;
    _ -> ok
  end.

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

% add_worker(NewWorker) ->
%   io:format("inside add_worker. worker = ~p~n", [NewWorker]),
%   FinalWorker = case get_worker(NewWorker#worker.pid, NewWorker#worker.function_name) of
%     not_found -> NewWorker;
%     CurrentWorker -> merge_records(worker, NewWorker, CurrentWorker)
%   end,
%   io:format("final worker = ~p~n", [FinalWorker]),
%   R = case ets:insert(workers, FinalWorker) of
%     false -> duplicate_worker;
%     _ -> ok
%   end,
%   io:format("R = ~p~n", [R]),
%   ok.

list_workers() ->
  ets:tab2list(workers).

delete_worker(Pid) when is_pid(Pid) ->
  ets:match_delete(workers, {Pid, '_'}),
  ets:match_delete(worker_statuses, {Pid, '_'}),
  ets:match_delete(worker_functions, {Pid, '_'}),
  ok.

% private functions
%%% @spec merge(A, B, []) -> [term()]
%%%     A = [term()]
%%%     B = [term()]
%%%
%%% @doc Merges the lists `A' and `B' into to a new list
%%%
%%% Each element in `A' and `B' are compared.
%%% If they match, the matching element is added to the result.
%%% If one is undefined and the other is not, the one that is not undefined
%%% is added to the result. If each has a value and they differ, `A' takes
%%% precedence.
%%%
%%% This is a slightly modified version of some code that Adam Lindberg posted
%%% to StackOverflow, here:
%%% http://stackoverflow.com/questions/62245/merging-records-for-mnesia
%%% Thanks Adam.
%%% @end

% merge([C|ATail], [C|BTail], Result) ->
%   merge(ATail, BTail, [C|Result]);
% merge([undefined|ATail], [C|BTail], Result) ->
%   merge(ATail, BTail, [C|Result]);
% merge([C|ATail], [undefined|BTail], Result) ->
%   merge(ATail, BTail, [C|Result]);
% merge([C|ATail], [_|BTail], Result) ->
%   merge(ATail, BTail, [C|Result]);
% merge([], [], Result) ->
%   lists:reverse(Result).

% merge_records(RecordName, RecordA, RecordB) ->
%   list_to_tuple(
%     lists:append([RecordName],
%       merge(tl(tuple_to_list(RecordA)),
%             tl(tuple_to_list(RecordB)),
%             []))).

% get_worker(Pid, undefined) when is_pid(Pid) ->
%   not_found;

% get_worker(Pid, FunctionName) when is_pid(Pid) ->
%   WorkerTable = ets:tab2list(workers),
%   io:format("worker table = ~p~n", [WorkerTable]),
%   MatchSpec = ets:fun2ms(fun(W = #worker{function_name=F, pid=P}) when F == FunctionName; P == Pid -> W end),
%   case ets:select(workers, MatchSpec) of
%     [] -> not_found;
%     [Worker] -> Worker
%   end.

create_tables([]) ->
  ok;

create_tables([{Name,Attributes}|Rest]) ->
  case ets:info(Name) of
    undefined -> ets:new(Name, Attributes);
    _ -> ok
  end,
  create_tables(Rest).
