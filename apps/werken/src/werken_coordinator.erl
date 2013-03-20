-module(werken_coordinator).
-behavior(gen_server).
-include("records.hrl").

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-record(state, {pids_to_sockets}).

%% API
start_link() ->
  gen_server:start_link(?MODULE, [], []).

%% gen_server callbacks
init([]) ->
  {ok, #state{}, 0}. % jump to the timeout

handle_info(timeout, State) ->
  register(werken_coordinator, self()),
  {noreply, State}.

handle_call(Msg, _From, State) ->
  {reply, {ok, Msg}, State}.

handle_cast(stop, State) ->
  {stop, normal, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.
