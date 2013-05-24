-module(gearman_test_worker).
-export([can_do/2, cant_do/2, grab_job/1, no_job/0, pre_sleep/1, noop/0, job_assign/3, work_complete/3, set_client_id/2, reset_abilities/1, work_status/4, work_fail/2, work_exception/3, work_data/3, work_warning/3]).

can_do(Socket, FunctionName) ->
  Size = length(FunctionName),
  Packet = [0, "REQ", <<1:32/big>>, <<Size:32/big>>, FunctionName],
  gen_tcp:send(Socket, Packet).

cant_do(Socket, FunctionName) ->
  Size = length(FunctionName),
  Packet = [0, "REQ", <<2:32/big>>, <<Size:32/big>>, FunctionName],
  gen_tcp:send(Socket, Packet).

reset_abilities(Socket) ->
  Packet = [0, "REQ", <<3:32/big>>, <<0:32/big>>],
  gen_tcp:send(Socket, Packet).

grab_job(Socket) ->
  Packet = [0, "REQ", <<9:32/big>>, <<0:32/big>>],
  gen_tcp:send(Socket, Packet).

no_job() ->
  Packet = [0, "RES", <<10:32/big>>, <<0:32/big>>],
  iolist_to_binary(Packet).

pre_sleep(Socket) ->
  Packet = [0, "REQ", <<4:32/big>>, <<0:32/big>>],
  gen_tcp:send(Socket, Packet).

noop() ->
  Packet = [0, "RES", <<6:32/big>>, <<0:32/big>>],
  iolist_to_binary(Packet).

job_assign(JobHandle, JobName, Data) ->
  Args = [JobHandle, JobName, Data],
  Size = calculate_size(Args),
  Packet = [0, "RES", <<11:32/big>>, <<Size:32/big>>, JobHandle, 0, JobName, 0, Data],
  iolist_to_binary(Packet).

work_complete(Socket, JobHandle, Result) ->
  Args = [JobHandle, Result],
  Size = calculate_size(Args),
  Packet = [0, "REQ", <<13:32/big>>, <<Size:32/big>>, JobHandle, 0, Result],
  gen_tcp:send(Socket, Packet).

work_status(Socket, JobHandle, Numerator, Denominator) ->
  Args = [JobHandle, Numerator, Denominator],
  Size = calculate_size(Args),
  Packet = [0, "REQ", <<12:32/big>>, <<Size:32/big>>, JobHandle, 0, Numerator, 0, Denominator],
  gen_tcp:send(Socket, Packet).

work_fail(Socket, JobHandle) ->
  Args = [JobHandle],
  Size = calculate_size(Args),
  Packet = [0, "REQ", <<14:32/big>>, <<Size:32/big>>, JobHandle],
  gen_tcp:send(Socket, Packet).

work_exception(Socket, JobHandle, Data) ->
  Args = [JobHandle, Data],
  Size = calculate_size(Args),
  Packet = [0, "REQ", <<25:32/big>>, <<Size:32/big>>, JobHandle, 0, Data],
  gen_tcp:send(Socket, Packet).

work_data(Socket, JobHandle, Data) ->
  Args = [JobHandle, Data],
  Size = calculate_size(Args),
  Packet = [0, "REQ", <<28:32/big>>, <<Size:32/big>>, JobHandle, 0, Data],
  gen_tcp:send(Socket, Packet).

work_warning(Socket, JobHandle, Data) ->
  Args = [JobHandle, Data],
  Size = calculate_size(Args),
  Packet = [0, "REQ", <<29:32/big>>, <<Size:32/big>>, JobHandle, 0, Data],
  gen_tcp:send(Socket, Packet).

set_client_id(Socket, ClientId) ->
  Size = werken_utils:size_or_length(ClientId),
  Packet = [0, "REQ", <<22:32/big>>, <<Size:32/big>>, ClientId],
  gen_tcp:send(Socket, Packet).

% private functions
calculate_size(Args) ->
  calculate_size(Args, length(Args) - 1).

calculate_size([], Size) ->
  Size;

calculate_size([H|T], Size) ->
  S = werken_utils:size_or_length(H),
  calculate_size(T, Size + S).
