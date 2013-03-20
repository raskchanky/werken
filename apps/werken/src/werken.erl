-module(werken).
-export([start/0, stop/0]).

start() ->
  application:start(werken).

stop() ->
  application:stop(werken).
