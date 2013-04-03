-module(werken_storage).
-include("records.hrl").

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
    [] -> [];
    Workers -> get_job(Workers, [high, normal, low])
  end;

get_job(JobHandle) ->
  case ets:lookup(jobs, JobHandle) of
    [] -> no_job;
    [Job] -> Job
  end.

get_job(_, []) ->
  no_job;

get_job(Workers, [Priority|OtherPriorities]) ->
  case get_job(Workers, Priority) of
    no_job -> get_job(Workers, OtherPriorities);
    Job -> Job
  end;

get_job([], Priority) when is_atom(Priority) ->
  no_job;

get_job([Worker|OtherWorkers], Priority) when is_atom(Priority) ->
  Pattern = {'_', '_', '_', '_', Worker#worker.function_name, '_', '_', Priority, '_'},
  case ets:match_object(jobs, Pattern) of
    [] -> get_job(OtherWorkers, Priority);
    [Job] -> Job
  end.

delete_job(JobHandle) ->
  ets:delete(jobs, JobHandle).

add_client(Client) ->
  case ets:insert_new(clients, Client) of
    false -> duplicate_client;
    _ -> ok
  end.

delete_client(ClientId) ->
  ets:match_delete(clients, {'_', '_', ClientId, '_'}).

add_worker(Worker) ->
  case ets:insert_new(workers, Worker) of
    false -> duplicate_worker;
    _ -> ok
  end.

list_workers() ->
  ets:tab2list(workers).

delete_worker(WorkerId) ->
  ets:match_delete(workers, {'_', '_', WorkerId}).

% private functions
create_tables([]) ->
  ok;

create_tables([{Name,Attributes}|_]) ->
  case ets:info(Name) of
    undefined -> ets:new(Name, Attributes);
    _ -> ok
  end.
