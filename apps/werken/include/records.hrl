%%% record definitions

-record(worker, {pid, function_name, status, worker_id}).
-record(client, {pid, function_name, data, client_id, result}).
-record(job, {job_id, data, submitted_at, run_at, function_name, unique_id, client_pid, priority, bg}).

%% job
%% - we track the client_pid so when the job is finished, if it's a foreground job, we can notify the client of the result.
%% - the unique_id is submitted in the SUBMIT_JOB packet by the client
%% - the job_id is automatically assigned by the server
