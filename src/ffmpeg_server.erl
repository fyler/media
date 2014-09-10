%%%-------------------------------------------------------------------
%%% @author ilia
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. июн 2014 18:03
%%%-------------------------------------------------------------------
-module(ffmpeg_server).
-author("ilia").

-behaviour(gen_server).
-include("log.hrl").
-include("ffmpeg_worker.hrl").
%% API
-export([start_link/0, start_link_worker/1]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-record(state, {refs,
                workers = [],
                waiting = queue:new(),
                count = 0,
                max_count}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
  {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([]) ->
  self() ! start,
  {ok, #state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
  {reply, Reply :: term(), NewState :: #state{}} |
  {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_call({worker, init, Ref, WorkerOpt}, {Pid, _}, #state{refs = Refs} = State) ->
  ets:insert(Refs, {Ref, {Pid, WorkerOpt}}),
  {reply, ok, State};

handle_call({worker, start, Ref}, {Pid, _}, #state{refs = Refs, workers = Workers, waiting = Waiting, count = N} = State) ->
  NewN = case ets:member(Refs, Ref) of
          true ->
            N;
           false ->
            N + 1
         end,
  ets:insert(Refs, {Ref, {Pid, #{}}}),
  case queue:out(Waiting) of
    {{value, From}, NewWaiting} ->
      gen_server:reply(From, {ok, Pid}),
      {reply, ok, State#state{waiting = NewWaiting, count = NewN}};
    {empty, Waiting} ->
      NewWorkers = insert_worker(Workers, #{}, Pid),
      {reply, ok, State#state{workers = NewWorkers, count = NewN}}
  end;

handle_call({worker, finish, Ref}, _From, #state{refs = Refs, count = Count, max_count = MaxCount} = State) when Count > MaxCount ->
  ets:delete(Refs, Ref),
  {reply, ok, State#state{count = Count - 1}};

handle_call({worker, finish, Ref}, _From, #state{refs = Refs, workers = Workers} = State) ->
  [{Ref, {Pid, WorkerOpt}}] = ets:lookup(Refs, Ref),
  {reply, ok, State#state{workers = insert_worker(Workers, WorkerOpt, Pid)}};

handle_call({checkout, _WorkerOpt}, From, #state{workers = [], waiting = Waiting} = State) ->
  prepopulate(1),
  {noreply, State#state{waiting = queue:in(From, Waiting)}};

handle_call({checkout, WorkerOpt}, _From, #state{workers = Workers} = State) ->
  {Pid, OtherWorkers} = get_worker(Workers, WorkerOpt),
  {reply, {ok, Pid}, State#state{workers = OtherWorkers}};

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(_Request, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_info(start, State) ->
  N = ulitos_var:get_var(media, ffmpeg_workers_count, 3),
  Refs = ets:new(refs, [set, private]),
  prepopulate(N),
  {noreply, State#state{refs = Refs, max_count = N}};

handle_info(_Info, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, _State) ->
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
  {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

start_link_worker(WorkerOpt) ->
  Options = set_default(WorkerOpt),
  {ok, Pid} = gen_server:call(ffmpeg_server, {checkout, Options}),
  link(Pid),
  ffmpeg_worker:set_options(Pid, self(), Options),
  {ok, Pid}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

prepopulate(0) ->
  ok;

prepopulate(N) ->
  Ref = make_ref(),
  ffmpeg_sup:start_worker(Ref),
  prepopulate(N - 1).

insert_worker(Workers, Worker, Pid) ->
  [{Worker, [Pid|proplists:get_value(Worker, Workers, [])]}|proplists:delete(Worker, Workers)].

get_worker(Workers, Worker) ->
  case proplists:get_value(Worker, Workers, []) of
    [] ->
      get_worker(Workers, element(1, hd(Workers)));
    [Pid] ->
      {Pid, proplists:delete(Worker, Workers)};
    [Pid|OtherWorkers] ->
      {Pid, [{Worker, OtherWorkers}|proplists:delete(Worker, Workers)]}
  end.

set_default(#{audio := Audio, video := Video}) ->
  #{audio => merge(Audio), video => merge(Video)};

set_default(#{audio := Audio}) ->
  #{audio => merge(Audio)};

set_default(#{video := Video}) ->
  #{video => merge(Video)}.

merge(#{output := Output, input := #{config := _Config} = Input}) ->
  #{output => maps:merge(maps:without(config, Input), Output), input => Input};

merge(#{output := Output, input := Input}) ->
  #{output => maps:merge(Input, Output), input => Input}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

insert_worker_test() ->
  Workers1 = insert_worker([], #{}, 1),
  Workers2 = insert_worker(Workers1, #{}, 2),
  Workers3 = insert_worker(Workers1, #{a => 1}, 3),
  ?assertEqual([{#{}, [2, 1]}], Workers2),
  ?assertEqual([{#{a => 1}, [3]}, {#{}, [1]}], Workers3).

-endif.
