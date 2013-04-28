-module(werken_connection).
-behavior(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-record(state, {socket}).

start_link(Socket) ->
  gen_server:start_link(?MODULE, Socket, []).

init(Socket) ->
  gen_server:cast(self(), accept),
  {ok, #state{socket = Socket}}.

handle_call(_Msg, _From, State) ->
  {noreply, State}.

handle_cast(accept, State = #state{socket=LSock}) ->
  {ok, Socket} = gen_tcp:accept(LSock),
  werken_connection_sup:start_socket(), %% maintain 20 listeners
  inet:setopts(Socket, [{active, once}]),
  {noreply, State#state{socket=Socket}};

handle_cast({process_packet, Func}, #state{socket = Socket} = State) ->
  io:format("Func ~p~n", [Func]),
  Result = Func(),
  io:format("Result ~p~n", [Result]),
  werken_response:send_response(Result, Socket),
  {noreply, State};

handle_cast(stop, State) ->
  {stop, normal, State}.

handle_info({tcp, Sock, RawData}, State) when is_binary(RawData) ->
  io:format("just received raw data ~p~n", [RawData]),
  werken_parser:parse(RawData),
  inet:setopts(Sock, [{active, once}]),
  {noreply, State};

handle_info({tcp_closed, _Sock}, State) ->
  gen_server:cast(werken_coordinator, {delete_connection, self()}),
  {stop, normal, State};

handle_info(_M, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.
