-module(werken_connection).
-behavior(gen_server).

%% API
-export([start_link/1, echo_req/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-record(state, {socket}).

% API
start_link(Socket) ->
  gen_server:start_link(?MODULE, Socket, []).

echo_req(Data) ->
  {binary, ["ECHO_RES", Data]}.

% callbacks
init(Socket) ->
  gen_server:cast(self(), accept),
  {ok, #state{socket = Socket}}.

handle_call(wakeup_worker, _From, State = #state{socket = Socket}) ->
  werken_response:send_response({binary, ["NOOP"]}, Socket),
  {reply, ok, State};

handle_call(get_socket, _From, State = #state{socket = Socket}) ->
  {reply, {ok, Socket}, State};

handle_call({process_packet, Func}, _From, #state{socket = Socket} = State) ->
  io:format("werken_connection, process_packet, Func ~p~n", [Func]),
  Result = Func(),
  io:format("werken_connection, process_packet, Result ~p~n", [Result]),
  werken_response:send_response(Result, Socket),
  {reply, ok, State};

handle_call(_Msg, _From, State) ->
  {noreply, State}.

handle_cast(accept, State = #state{socket=LSock}) ->
  {ok, Socket} = gen_tcp:accept(LSock),
  werken_connection_sup:start_socket(), %% maintain 20 listeners
  inet:setopts(Socket, [{active, once}]),
  {noreply, State#state{socket=Socket}};

% handle_cast({process_packet, Func}, #state{socket = Socket} = State) ->
%   io:format("werken_connection, process_packet, Func ~p~n", [Func]),
%   Result = Func(),
%   io:format("werken_connection, process_packet, Result ~p~n", [Result]),
%   werken_response:send_response(Result, Socket),
%   {noreply, State};

handle_cast(stop, State) ->
  {stop, normal, State}.

handle_info({tcp, Sock, RawData}, State) when is_binary(RawData) ->
  io:format("just received raw data ~p~n", [RawData]),
  Results = werken_parser:parse(RawData),
  io:format("finished parsing all the shit. Results = ~p~n", [Results]),
  process_results(lists:reverse(Results), Sock),
  inet:setopts(Sock, [{active, once}]),
  {noreply, State};

handle_info({tcp_closed, _Sock}, State) ->
  werken_storage_client:delete_client(self()),
  werken_storage_worker:delete_worker(self()),
  {stop, normal, State};

handle_info(_M, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

% private
process_results([], _Socket) ->
  io:format("process_results.  all done processing. returning ok."),
  ok;

process_results([Result|Rest], Socket) ->
  io:format("process_results. Result = ~p, Rest = ~p~n", [Result, Rest]),
  Data = Result(),
  io:format("process_results. Data = ~p~n", [Data]),
  werken_response:send_response(Data, Socket),
  io:format("process_results. just finished sending a response. recursing now~n"),
  process_results(Rest, Socket).
