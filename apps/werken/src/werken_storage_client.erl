-module(werken_storage_client).
-export([add_client/1, delete_client/1]).

-include("records.hrl").

add_client(Client) ->
  case ets:insert(clients, Client) of
    false -> duplicate_client;
    _ -> ok
  end.

delete_client(Pid) when is_pid(Pid) ->
  ets:delete(clients, Pid);

delete_client(ClientId) ->
  ets:match_delete(clients, {'_', '_', ClientId, '_'}).
