-module(werken_storage_client).
-export([add_client/1, delete_client/1, get_client/1]).

-include("records.hrl").

add_client(Client) ->
  ets:insert(clients, Client).

get_client(Pid) when is_pid(Pid) ->
  ets:lookup(clients, Pid).

delete_client(Pid) when is_pid(Pid) ->
  ets:delete(clients, Pid),
  ets:match_delete(job_clients, {'_', Pid});

delete_client(ClientId) ->
  ets:match_delete(clients, {'_', '_', '_', ClientId, '_', '_'}).
