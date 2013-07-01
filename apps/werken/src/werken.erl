-module(werken).
-export([start/0, stop/0]).

start() ->
  % lager:set_loglevel(lager_console_backend, error),
  % lager:set_loglevel(lager_file_backend, "console.log", debug),
  application:start(lager),
  application:start(werken).

stop() ->
  application:stop(lager),
  application:stop(werken).
