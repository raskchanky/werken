-module(gearman_test_client).
-export([submit_job/3, submit_job_low/3, submit_job_high/3, submit_job_bg/3, submit_job_low_bg/3, submit_job_high_bg/3, job_created/1, work_complete/2]).

submit_job(Socket, JobName, Data) ->
  submit_a_job(Socket, JobName, Data, 7).

submit_job_low(Socket, JobName, Data) ->
  submit_a_job(Socket, JobName, Data, 33).

submit_job_high(Socket, JobName, Data) ->
  submit_a_job(Socket, JobName, Data, 21).

submit_job_bg(Socket, JobName, Data) ->
  submit_a_job(Socket, JobName, Data, 18).

submit_job_low_bg(Socket, JobName, Data) ->
  submit_a_job(Socket, JobName, Data, 34).

submit_job_high_bg(Socket, JobName, Data) ->
  submit_a_job(Socket, JobName, Data, 32).

job_created(Response) ->
  <<0, 82, 69, 83, 0, 0, 0, 8, 0, 0, 0, 58, JobHandle/bytes>> = Response,
	{ok, JobHandle}.

work_complete(JobHandle, Result) ->
	Size = size(JobHandle) + length(Result) + 1,
  Packet = [0, "RES", <<13:32/big>>, <<Size:32/big>>, JobHandle, 0, Result],
  iolist_to_binary(Packet).

submit_a_job(Socket, JobName, Data, CommandNum) ->
	Size = werken_utils:size_or_length(JobName) + werken_utils:size_or_length(Data) + 2,
  Packet = [0, "REQ", <<CommandNum:32/big>>, <<Size:32/big>>, JobName, 0, 0, Data],
  gen_tcp:send(Socket, Packet).
