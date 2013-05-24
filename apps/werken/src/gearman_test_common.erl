-module(gearman_test_common).
-export([connect/0, connect/2, get_response/1, disconnect/1]).
-export([echo_req/2, echo_res/1]).

connect() ->
	connect({127, 0, 0, 1}, 4730).

connect(IP, Port) ->
	{ok, _Socket} = gen_tcp:connect(IP, Port, [binary, {active, false}]).

get_response(Socket) ->
	{ok, _Response} = gen_tcp:recv(Socket, 0).

disconnect(Socket) ->
	gen_tcp:close(Socket).

echo_req(Socket, Data) ->
	Size = werken_utils:size_or_length(Data),
  Packet = [0, "REQ", <<16:32/big>>, <<Size:32/big>>, Data],
  gen_tcp:send(Socket, Packet).

echo_res(Data) ->
	Size = werken_utils:size_or_length(Data),
  Packet = [0, "RES", <<17:32/big>>, <<Size:32/big>>, Data],
  iolist_to_binary(Packet).
