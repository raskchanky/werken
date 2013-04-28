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
  {reply, ok, State};

handle_call({get_job, Pid}, _From, State) when is_pid(Pid) ->
  io:format("werken_coordinator. handle_call/get_job/pid = ~p~n", [Pid]),
  Job = werken_storage:get_job(Pid),
  io:format("job = ~p~n", [Job]),
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

handle_call(dump_workers, _From, State) ->
  Workers = ets:tab2list(workers),
  {reply, {ok, Workers}, State};

handle_call(list_workers, _From, State) ->
  Workers = werken_storage:list_workers(),
  {reply, {ok, Workers}, State};

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
  {noreply, State};

handle_cast(stop, State) ->
  {stop, normal, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.
