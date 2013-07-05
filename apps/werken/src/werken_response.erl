-module(werken_response).
-compile([{parse_transform, lager_transform}]).

%% API
-export([send_response/2]).

send_response({text, Data}, Socket) ->
  gen_tcp:send(Socket, Data);

send_response({binary, Data}, Socket) ->
  [Command | Args] = Data,
  CommandNum = command_to_num(Command),
  BinArgs = werken_utils:list_to_null_list(Args),
  DataSize = size(BinArgs),
  Response = [0, "RES", <<CommandNum:32/big, DataSize:32/big, BinArgs/binary>>],
  lager:debug("Response = ~p", [Response]),
  gen_tcp:send(Socket, Response);

send_response(_Data, _Socket) ->
  ok.

%% internal functions
command_to_num(Command) ->
  case Command of
    "NOOP"            -> 6;
    "JOB_CREATED"     -> 8;
    "NO_JOB"          -> 10;
    "JOB_ASSIGN"      -> 11;
    "WORK_STATUS"     -> 12;
    "WORK_COMPLETE"   -> 13;
    "WORK_FAIL"       -> 14;
    "ECHO_RES"        -> 17;
    "ERROR"           -> 19;
    "STATUS_RES"      -> 20;
    "WORK_EXCEPTION"  -> 25;
    "OPTION_RES"      -> 27;
    "WORK_DATA"       -> 28;
    "WORK_WARNING"    -> 29;
    "JOB_ASSIGN_UNIQ" -> 31
  end.
