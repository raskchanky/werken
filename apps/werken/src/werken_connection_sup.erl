-module(werken_connection_sup).
-behavior(supervisor).

%% API
-export([start_link/0, start_socket/0]).

%% Supervisor callbacks
-export([init/1]).

%% standard gearman port, but can be overridden
-define(DEFAULT_PORT, 4730).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
  Port = case application:get_env(werken, port) of
    {ok, P} -> P;
    undefined -> ?DEFAULT_PORT
  end,

  {ok, LSock} = gen_tcp:listen(Port, [binary,
                                      {active, false},
                                      {reuseaddr, true}]),

  spawn_link(fun initial_listeners/0),
  RestartStrategy = {simple_one_for_one, 60, 3600},
  WerkenConnection = {werken_connection,
                      {werken_connection, start_link, [LSock]},
                      temporary, 1000, worker, [werken_connection]},
  Children = [WerkenConnection],
  {ok, {RestartStrategy, Children}}.

start_socket() ->
  supervisor:start_child(?MODULE, []).

initial_listeners() ->
  [start_socket() || _ <- lists:seq(1,20)],
  ok.
