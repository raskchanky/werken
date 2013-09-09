-module(werken_storage_client).
-export([add_client/1, delete_client/1, get_client/1]).

-include("records.hrl").

add_client(Client) ->
  ets:insert(clients, Client).

get_client(Pid) when is_pid(Pid) ->
  ets:lookup(clients, Pid).

delete_client(Pid) when is_pid(Pid) ->
  ets:delete(clients, Pid),
  ets:match_delete(job_clients, {job_client, '_', Pid});

delete_client(ClientId) ->
  ets:match_delete(clients, {client, '_', '_', '_', ClientId, '_', '_'}).
