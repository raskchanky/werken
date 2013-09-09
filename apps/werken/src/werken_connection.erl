-module(werken_connection).
-compile([{parse_transform, lager_transform}]).
-behavior(gen_server).

%% API
-export([start_link/1, echo_req/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-record(state, {socket, shutdown}).

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
  lager:debug("process_packet, Func ~p", [Func]),
  Result = Func(),
  lager:debug("process_packet, Result ~p", [Result]),
  werken_response:send_response(Result, Socket),
  {reply, ok, State};

handle_call(_Msg, _From, State) ->
  {noreply, State}.

handle_cast(accept, State = #state{socket=LSock, shutdown=SD}) ->
  {ok, Socket} = gen_tcp:accept(LSock),
  case SD of
    true -> gen_tcp:close(LSock);
    _ -> werken_connection_sup:start_socket() %% maintain 20 listeners
  end,
  inet:setopts(Socket, [{active, once}]),
  {noreply, State#state{socket=Socket}};

handle_cast(close_socket, State = #state{socket=Socket}) ->
  case erlang:port_info(Socket) of
    undefined ->
      gen_server:cast(self(), stop);
    _ -> ok
  end,
  {noreply, State#state{shutdown=true}};

handle_cast(stop, State = #state{socket=Socket}) ->
  gen_tcp:close(Socket),
  {stop, normal, State}.

handle_info({tcp, Sock, RawData}, State) when is_binary(RawData) ->
  lager:debug("just received raw data ~p", [RawData]),
  Results = werken_parser:parse(RawData),
  lager:debug("finished parsing all the shit. Results = ~p", [Results]),
  process_results(lists:reverse(Results), Sock),
  inet:setopts(Sock, [{active, once}]),
  {noreply, State};

handle_info({tcp_closed, _Sock}, State) ->
  maybe_requeue_job(),
  remove_myself(),
  {stop, normal, State};

handle_info(_M, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

% private
remove_myself() ->
  werken_storage_client:delete_client(self()),
  werken_storage_worker:delete_worker(self()),
  ok.

maybe_requeue_job() ->
  case am_i_a_worker() of
    false -> ok;
    WorkerId ->
      werken_storage_job:mark_job_as_available_for_worker_id(WorkerId)
  end.

am_i_a_worker() ->
  case werken_storage_worker:get_worker_id_for_pid(self()) of
    error -> false;
    WorkerId -> WorkerId
  end.

process_results([], _Socket) ->
  lager:debug("all done processing. returning ok."),
  ok;

process_results([Result|Rest], Socket) ->
  lager:debug("Result = ~p, Rest = ~p", [Result, Rest]),
  Data = Result(),
  lager:debug("Data = ~p", [Data]),
  werken_response:send_response(Data, Socket),
  lager:debug("just finished sending a response. recursing now"),
  process_results(Rest, Socket).
