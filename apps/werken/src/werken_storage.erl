-module(werken_storage).
-include("records.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-export([init/0]).
-export([add_job/1, get_job/1, delete_job/1]).
-export([add_client/1, delete_client/1]).
-export([add_worker/1, list_workers/0, delete_worker/1]).

init() ->
  Tables = [
        {jobs, [set, named_table]},
        {clients, [set, named_table]},
        {workers, [bag, named_table]} % a bag lets us store multiple worker records for the same function name
      ],
  create_tables(Tables).

add_job(Job) ->
  case ets:insert_new(jobs, Job) of
    false -> duplicate_job;
    _ -> ok
  end.

% GRAB_JOB from a worker
get_job(Pid) when is_pid(Pid) ->
  case ets:match_object(workers, {'_', '_', '_', Pid}) of
    [] -> no_job;
    Workers ->
      FunctionNames = lists:map(fun(Worker) -> Worker#worker.function_name end, Workers),
      get_job(FunctionNames, [high, normal, low])
  end;

get_job(JobHandle) ->
  case ets:lookup(jobs, JobHandle) of
    [] -> no_job;
    [Job] -> Job
  end.

get_job(_, []) ->
  no_job;

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
  case ets:insert_new(clients, Client) of
    false -> duplicate_client;
    _ -> ok
  end.

delete_client(ClientId) ->
  ets:match_delete(clients, {'_', '_', ClientId, '_'}).

add_worker(NewWorker) ->
  FinalWorker = case get_worker(NewWorker#worker.pid, NewWorker#worker.function_name) of
    not_found -> NewWorker;
    CurrentWorker -> merge_records(worker, NewWorker, CurrentWorker)
  end,
  case ets:insert_new(workers, FinalWorker) of
    false -> duplicate_worker;
    _ -> ok
  end.

list_workers() ->
  ets:tab2list(workers).

delete_worker(WorkerId) ->
  ets:match_delete(workers, {'_', '_', WorkerId}).

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

merge([C|ATail], [C|BTail], Result) ->
  merge(ATail, BTail, [C|Result]);
merge([undefined|ATail], [C|BTail], Result) ->
  merge(ATail, BTail, [C|Result]);
merge([C|ATail], [undefined|BTail], Result) ->
  merge(ATail, BTail, [C|Result]);
merge([C|ATail], [_|BTail], Result) ->
  merge(ATail, BTail, [C|Result]);
merge([], [], Result) ->
  lists:reverse(Result).

merge_records(RecordName, RecordA, RecordB) ->
  list_to_tuple(
    lists:append([RecordName],
      merge(tl(tuple_to_list(RecordA)),
            tl(tuple_to_list(RecordB)),
            []))).

get_worker(Pid, FunctionName) ->
  MatchSpec = ets:fun2ms(fun(W = #worker{function_name=F, pid=P}) when F == FunctionName; P == Pid -> W end),
  case ets:select(workers, MatchSpec) of
    [] -> not_found;
    [Worker] -> Worker
  end.

create_tables([]) ->
  ok;

create_tables([{Name,Attributes}|_]) ->
  case ets:info(Name) of
    undefined -> ets:new(Name, Attributes);
    _ -> ok
  end.
