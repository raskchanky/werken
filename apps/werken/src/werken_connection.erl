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
  % ok = gen_server:call(werken_coordinator, {map_pid_to_socket, self(), Socket}),
  inet:setopts(Socket, [{active, once}]),
  {noreply, State#state{socket=Socket}};

handle_cast(stop, State) ->
  {stop, normal, State}.

handle_info({tcp, Sock, RawData}, State) when is_binary(RawData) ->
  shiva_request:parse(RawData),
  inet:setopts(Sock, [{active, once}]),
  {noreply, State};

handle_info({tcp_closed, _Sock}, State) ->
  gen_server:cast(shiva_server, {delete_dispatcher, self()}),
  {stop, normal, State};

handle_info(_M, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.
