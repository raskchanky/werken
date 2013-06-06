-module(werken_parser).

%% API
-export([parse/1]).

parse(<<0, "REQ", Command:32, Size:32, Data:Size/bytes, Rest/bytes>>) ->
  io:format("werken_parser, parse, Command = ~p, Data = ~p~n", [Command, Data]),
  Result = decode(Command, Data),
  io:format("werken_parser, parse, Result = ~p~n", [Result]),
  notify_connection_of_packet(Result),
  case Rest of
    <<>> ->
      ok;
    Other ->
      parse(Other)
  end;

parse(<<AdminCommand/bytes>>) ->
  NewCommand = binary_to_atom(binary:replace(AdminCommand, [<<10>>,<<13>>], <<>>, [global]), utf8),
  io:format("PARSING AN ADMIN COMMAND YO. NewCommand = ~p~n", [NewCommand]),
  Func = fun() ->
    apply(werken_admin, NewCommand, [])
  end,
  notify_connection_of_packet(Func),
  ok.

% internal functions
notify_connection_of_packet(Packet) ->
  gen_server:cast(self(), {process_packet, Packet}).

decode(Num, Data) when is_binary(Data), is_integer(Num) ->
  Module = num_to_module(Num),
  Command = num_to_command(Num),
  io:format("Module = ~p, Command = ~p~n", [Module, Command]),
  Z = werken_utils:args_to_list(Data),
  io:format("Data = ~p~n", [Z]),
  Func = fun() ->
    apply(Module, Command, werken_utils:args_to_list(Data))
  end,
  Func.

num_to_module(16) ->
  werken_connection;

num_to_module(26) ->
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
