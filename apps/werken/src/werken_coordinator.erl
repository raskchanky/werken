-module(werken_coordinator).
-behavior(gen_server).
-include("records.hrl").

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-record(state, {}).

%% API
start_link() ->
  gen_server:start_link(?MODULE, [], []).

%% gen_server callbacks
init([]) ->
  {ok, #state{}, 0}. % jump to the timeout

handle_info(timeout, State) ->
  register(werken_coordinator, self()),
  werken_storage:init(),
  {noreply, State}.

handle_call({add_job, Job}, _From, State) ->
  werken_storage:add_job(Job),
  spawn(fun() -> wakeup_workers_for_job(Job) end),
  {reply, ok, State};

handle_call({get_job, Pid}, _From, State) when is_pid(Pid) ->
  io:format("werken_coordinator. handle_call/get_job/pid = ~p~n", [Pid]),
  Job = werken_storage:get_job(Pid),
  io:format("werken coordinator. job = ~p~n", [Job]),
  {reply, {ok, Job}, State};

handle_call({get_job, JobHandle}, _From, State) ->
  Job = werken_storage:get_job(JobHandle),
  {reply, {ok, Job}, State};

handle_call({delete_job, JobHandle}, _From, State) ->
  werken_storage:delete_job(JobHandle),
  {reply, ok, State};

handle_call({add_client, Client}, _From, State) ->
  werken_storage:add_client(Client),
  {reply, ok, State};

handle_call({add_worker, Worker}, _From, State) ->
  io:format("about to add worker ~p~n", [Worker]),
  werken_storage:add_worker(Worker),
  AllWorkers = ets:tab2list(workers),
  io:format("workers table = ~p~n", [AllWorkers]),
  io:format("about to return from adding a worker~n"),
  {reply, ok, State};

handle_call(Msg, _From, State) ->
  {reply, {ok, Msg}, State}.

handle_cast({delete_connection, Pid}, State) ->
  werken_storage:delete_client(Pid),
  werken_storage:delete_worker(Pid),
  {noreply, State};

handle_cast({remove_function_from_worker, all, Pid}, State) ->
  {noreply, State};

handle_cast({remove_function_from_worker, FunctionName, Pid}, State) ->
  {noreply, State};

handle_cast({respond_to_client, Pid, Packet}, State) ->
  io:format("werken_coordinator. respond_to_client. Pid = ~p, Packet = ~p~n", [Pid, Packet]),
  {noreply, State};

handle_cast(stop, State) ->
  {stop, normal, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

% private
wakeup_workers_for_job(Job) ->
  io:format("wakeup_workers_for_job, Job = ~p~n", [Job]),
  Pids = werken_storage:get_worker_pids_for_function_name(Job#job.function_name),
  io:format("wakeup_workers_for_job, Pids = ~p~n", [Pids]),
  wakeup_workers(Pids).

wakeup_workers([]) ->
  io:format("wakeup_workers, all out of workers. bye bye~n"),
  ok;

wakeup_workers([Pid|Rest]) ->
  io:format("wakeup_workers, Pid = ~p, Rest = ~p~n", [Pid, Rest]),
  Record = werken_storage:get_worker_status(Pid),
  io:format("wakeup_workers, Record = ~p~n", [Record]),
  case Record#worker_status.status of
    asleep ->
      gen_server:call(Pid, wakeup_worker);
    _ ->
      ok
  end,
  wakeup_workers(Rest).
