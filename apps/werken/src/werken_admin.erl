-module(werken_admin).
-export([workers/0, status/0, version/0]).

workers() ->
  {ok, WorkerList} = gen_server:call(werken_coordinator, list_workers),
  Result = case WorkerList of
    [] ->
      ".\n";
    _ ->
      Data = lists:map(fun(X) ->
            {H, F} = X,
            Y = [string:join(H, " "), string:join(F, " ")],
            string:join(Y, " : ")
        end, WorkerList),
      NewData = string:join(Data, "\n"),
      io_lib:format("~s~n.~n", [NewData])
  end,
  {text, Result}.

status() ->
  % get a list of the unique function names from the existing jobs in the queue
  % get a list of the unique function names from the existing workers connected
  % jobs will need a 'running' flag when they get sent to a worker to get the running job count
  Result = 1,
  {text, Result}.

version() ->
  {text, "0.1.0"}.
