-module(werken).
-export([start/0, stop/0]).

start() ->
  application:start(lager),
  application:start(werken).

stop() ->
  application:stop(lager),
  application:stop(werken).
