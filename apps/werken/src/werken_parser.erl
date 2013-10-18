-module(werken_parser).
-compile([{parse_transform, lager_transform}]).

%% API
-export([parse/1]).

parse(Data) ->
  parse(Data, []).

parse(Command = <<"workers">>, Acc) ->
  parse_admin_command(Command, Acc);

parse(Command = <<"status">>, Acc) ->
  parse_admin_command(Command, Acc);

parse(Command = <<"maxqueue", _Rest/bytes>>, Acc) ->
  parse_admin_command(Command, Acc);

parse(Command = <<"shutdown", _Rest/bytes>>, Acc) ->
  parse_admin_command(Command, Acc);

parse(Command = <<"version">>, Acc) ->
  parse_admin_command(Command, Acc);

parse(<<0, "REQ", Command:32, Size:32, Data:Size/bytes, Rest/bytes>>, Acc) ->
  lager:debug("Command = ~p, Data = ~p", [Command, Data]),
  Result = decode(Command, Data),
  lager:debug("Result = ~p", [Result]),
  NewList = [Result|Acc],
  case Rest of
    <<>> ->
      [NewList, undefined];
    Other ->
      parse(Other, NewList)
  end;

parse(<<ExtraData/bytes>>, Acc) ->
  lager:debug("there's extra data.  just gonna return it. ~p", [ExtraData]),
  [Acc, ExtraData].

parse_admin_command(<<AdminCommand/bytes>>, _Acc) ->
  NewCommand = binary_to_list(binary:replace(AdminCommand, [<<10>>,<<13>>], <<>>, [global])),
  lager:debug("PARSING AN ADMIN COMMAND YO. NewCommand = ~p", [NewCommand]),
  [Command|Args] = case string:words(NewCommand) > 1 of
    false -> [NewCommand];
    true ->
      Tokens = string:tokens(NewCommand, " "),
      [hd(Tokens), lists:flatten(tl(Tokens))]
  end,
  lager:debug("Command = ~p, Args = ~p", [Command, Args]),
  Func = fun() ->
    apply(werken_admin, list_to_atom(Command), Args)
  end,
  [Func].

decode(Num, Data) when is_binary(Data), is_integer(Num) ->
  Module = num_to_module(Num),
  Command = num_to_command(Num),
  lager:debug("Module = ~p, Command = ~p", [Module, Command]),
  Z = werken_utils:args_to_list(Data),
  lager:debug("Data = ~p", [Z]),
  Func = fun() ->
    apply(Module, Command, werken_utils:args_to_list(Data))
  end,
  Func.

num_to_module(16) ->
  werken_connection;

num_to_module(Num) ->
  WorkerCommands = [1, 2, 3, 4, 9, 12, 13, 14, 22, 23, 24, 25, 28, 29, 30],
  case lists:member(Num, WorkerCommands) of
    true -> werken_worker;
    false -> werken_client
  end.

num_to_command(Num) ->
  case Num of
    1 -> can_do;
    2 -> cant_do;
    3 -> reset_abilities;
    4 -> pre_sleep;
    7 -> submit_job;
    9 -> grab_job;
    12 -> work_status;
    13 -> work_complete;
    14 -> work_fail;
    15 -> get_status;
    16 -> echo_req;
    18 -> submit_job_bg;
    21 -> submit_job_high;
    22 -> set_client_id;
    23 -> can_do_timeout;
    24 -> all_yours;
    25 -> work_exception;
    26 -> option_req;
    28 -> work_data;
    29 -> work_warning;
    30 -> grab_job_uniq;
    32 -> submit_job_high_bg;
    33 -> submit_job_low;
    34 -> submit_job_low_bg;
    35 -> submit_job_sched;
    36 -> submit_job_epoch
  end.
