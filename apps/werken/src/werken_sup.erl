-module(werken_sup).
-behavior(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
  Coordinator = {werken_coordinator, {werken_coordinator, start_link, []}, permanent, 5000, worker, [werken_coordinator]},
  ConnectionSupervisor = {werken_connection_sup, {werken_connection_sup, start_link, []}, permanent, 5000, supervisor, [werken_connection_sup]},
  Children = [Coordinator, ConnectionSupervisor],
  RestartStrategy = {one_for_one, 6, 3600},
  {ok, {RestartStrategy, Children}}.
