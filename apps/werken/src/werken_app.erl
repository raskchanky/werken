-module(werken_app).
-behavior(application).

%% API
-export([version/0]).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
  case werken_sup:start_link() of
    {ok, Pid} ->
      {ok, Pid};
    Other ->
      {error, Other}
  end.

stop(_State) ->
    ok.

%% API
version() ->
  application:load(werken),
  {ok, Vsn} = application:get_key(werken, vsn),
  Vsn.
