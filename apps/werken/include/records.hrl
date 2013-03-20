%%% record definitions

-record(worker, {functions, status, worker_id}).
-record(client, {function_name, data, client_id, result}).
-record(job, {job_id, function_name, data, submitted_at, run_at, unique_id, client_pid, priority, bg}).
