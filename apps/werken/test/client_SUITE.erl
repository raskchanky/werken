-module(client_SUITE).
-include_lib("common_test/include/ct.hrl").
-include("records.hrl").
-export([all/0, init_per_testcase/2, end_per_testcase/2]).
-export([submit_job/1, submit_job_low/1, submit_job_high/1, submit_job_bg/1, submit_job_low_bg/1, submit_job_high_bg/1]).
 
all() -> [submit_job, submit_job_low, submit_job_high, submit_job_bg, submit_job_low_bg, submit_job_high_bg].

init_per_testcase(_, Config) ->
  application:start(werken),
  {ok, _Pid} = werken_sup:start_link(),
  Config.

end_per_testcase(_, Config) ->
  application:stop(werken),
  Config.
 
%% TODO: lots of duplication here. refactor out.
submit_job(_Config) ->
  F = fun(Job) ->
        normal = Job#job.priority,
        false = Job#job.bg
      end,
  submit_job_and_verify(submit_job, F, ["reverse", "test"]).

submit_job_low(_Config) ->
  F = fun(Job) ->
        low = Job#job.priority,
        false = Job#job.bg
      end,
  submit_job_and_verify(submit_job_low, F, ["reverse", "test"]).

submit_job_high(_Config) ->
  F = fun(Job) ->
        high = Job#job.priority,
        false = Job#job.bg
      end,
  submit_job_and_verify(submit_job_high, F, ["reverse", "test"]).

submit_job_bg(_Config) ->
  F = fun(Job) ->
        normal = Job#job.priority,
        true = Job#job.bg
      end,
  submit_job_and_verify(submit_job_bg, F, ["reverse", "test"]).

submit_job_low_bg(_Config) ->
  F = fun(Job) ->
        low = Job#job.priority,
        true = Job#job.bg
      end,
  submit_job_and_verify(submit_job_low_bg, F, ["reverse", "test"]).

submit_job_high_bg(_Config) ->
  F = fun(Job) ->
        high = Job#job.priority,
        true = Job#job.bg
      end,
  submit_job_and_verify(submit_job_high_bg, F, ["reverse", "test"]).

submit_job_and_verify(ClientFunc, VerifyFunc, Args) ->
  {ok, ClientSocket} = gearman_test_common:connect(),
  apply(gearman_test_client, ClientFunc, [ClientSocket|Args]),
  {ok, R2} = gearman_test_common:get_response(ClientSocket),
  <<0, 82, 69, 83, 8:32/big, Size:32/big, Handle:Size/bytes>> = R2,
  {ok, JobHandle} = gearman_test_client:job_created(R2),
  Handle = JobHandle,
  timer:sleep(1000),
  Job = werken_storage_job:get_job(JobHandle),
  VerifyFunc(Job),
  gearman_test_common:disconnect(ClientSocket).
