-module(werken_storage).
-include("records.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-export([init/0]).

init() ->
  Tables = [
        {jobs, [set, named_table, public, {keypos, #job.job_id}]},
        {job_functions, [bag, named_table, public, {keypos, #job_function.function_name}]},
        {job_statuses, [set, named_table, public, {keypos, #job_status.job_id}]},
        {job_clients, [set, named_table, public, {keypos, #job_client.job_id}]},
        {clients, [set, named_table, public, {keypos, #client.pid}]},
        {worker_functions, [bag, named_table, public, {keypos, #worker_function.pid}]},
        {worker_statuses, [set, named_table, public, {keypos, #worker_status.pid}]},
        {workers, [set, named_table, public, {keypos, #worker.pid}]}
      ],
  create_tables(Tables).

% private functions
create_tables([]) ->
  ok;

create_tables([{Name,Attributes}|Rest]) ->
  case ets:info(Name) of
    undefined -> ets:new(Name, Attributes);
    _ -> ok
  end,
  create_tables(Rest).
