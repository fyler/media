%%% @author ilia

-module(media_server).
-author("ilia").
-behaviour(gen_server).
-include("log.hrl").
%% API
-export([start_link/0,
         get_converter/3]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).
-record(state, {orders = #{}, clients = #{}}).

%%%===================================================================
%%% API
%%%===================================================================

-spec(start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec(init(Args :: term()) ->
  {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([]) ->
  {ok, #state{}}.

-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
  {reply, Reply :: term(), NewState :: #state{}} |
  {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_call({get_converter, Input, Output, Options}, {Pid, _Ref}, #state{orders = Orders, clients = Clients} = State) ->
  {ok, WorkerPid} = writer_sup:start_worker(Input, Output, Options),
  erlang:monitor(process, WorkerPid),
  NewClients = case maps:get(Pid, Clients, []) of
                [] ->
                  Ref = erlang:monitor(process, Pid),
                  maps:put(Pid, {Ref, [WorkerPid]}, Clients);
                {Ref, Workers} ->
                  maps:put(Pid, {Ref, [WorkerPid | Workers]}, Clients)
               end,
  {reply, ok, State#state{orders = maps:put(WorkerPid, {Pid, Options}, Orders), clients = NewClients}};

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

-spec(handle_cast(Request :: term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(_Request, State) ->
  {noreply, State}.

-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_info({'DOWN', _, process, Pid, Reason}, #state{orders = Orders, clients = Clients} = State) ->
  case maps:find(Pid, Orders) of
    {ok, {ClientPid, Options}} ->
      Message = case Reason of
                  normal ->
                    {task_complete, Options};
                  _ ->
                    {task_failed, Reason, Options}
                end,
      case maps:find(ClientPid, Clients) of
        {ok, {Ref, [Pid]}} ->
          erlang:demonitor(Ref),
          ClientPid ! Message,
          {noreply, State#state{orders = maps:remove(Pid, Orders), clients = maps:remove(ClientPid, Clients)}};
        {ok, {Ref, Workers}} ->
          ?I(finish),
          ClientPid ! Message,
          {noreply, State#state{orders = maps:remove(Pid, Orders), clients = maps:put(ClientPid, {Ref, lists:delete(Pid, Workers)}, Clients)}};
        error ->
          {noreply, State#state{orders = maps:remove(Pid, Orders)}}
      end;
    error ->
      case maps:find(Pid, Clients) of
        {ok, {_, Workers}} ->
          [writer_sup:stop_worker(WorkerPid) || WorkerPid <- Workers],
          NewOrders = lists:foldl(fun (WorkerPid, AllOrders) -> maps:remove(WorkerPid, AllOrders) end, Orders, Workers),
          {noreply, State#state{clients = maps:remove(Pid, Clients), orders = NewOrders}};
        error ->
          {noreply, State}
      end
  end;

handle_info(_Info, State) ->
  {noreply, State}.

-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, _State) ->
  ok.

-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
  {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

get_converter(Input, Output, Options) ->
  gen_server:call(media_server, {get_converter, Input, Output, Options}).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(setup(F), {setup, fun setup_/0, fun cleanup_/1, F}).

setup_() ->
  meck:new(writer_sup, [passthrough]),
  meck:expect(writer_sup, start_worker, fun(_,_,_) -> {ok, spawn(fun () -> timer:sleep(30000) end)} end),
  meck:expect(writer_sup, stop_worker, fun(_) -> ok end),
  media:start().

cleanup_(_) ->
  meck:unload(writer_sup),
  application:stop(lager),
  media:stop().

get_converter_test_() ->
  [
    {"Add one order", ?setup(fun get_converter_t_/1)},
    {"Add more than one order", ?setup(fun get_more_converters_t_/1)},
    {"Delete orders", ?setup(fun client_down_t_/1)},
    {"Worker failed", ?setup(fun worker_failed_t_/1)}
  ].

get_converter_t_(_) ->
  Self = self(),
  {reply, ok, #state{orders = Orders, clients = Clients}} = handle_call({get_converter, flv_reader, [hls_writer], #{}}, {Self, 1}, #state{}),
  [
    ?_assertEqual(1, maps:size(Orders)),
    ?_assertMatch({ok, {_, [_]}}, maps:find(Self, Clients))
  ].

get_more_converters_t_(_) ->
  Self = self(),
  #state{orders = Orders, clients = Clients} = lists:foldl(fun(X, AccIn) -> {reply, ok, AccOut} = handle_call({get_converter, flv_reader, [hls_writer], #{}}, {Self, X}, AccIn), AccOut end, #state{}, lists:seq(1, 10)),
  [
    ?_assertEqual(10, maps:size(Orders)),
    ?_assertMatch(1, maps:size(Clients))
  ].

client_down_t_(_) ->
  Self = self(),
  {noreply, #state{clients = Clients, orders = Orders}} = handle_info({'DOWN', 1, process, Self, normal}, #state{orders = #{1 => {Self, 1}, 2 => {Self, 2}, 3 => {Self, 3}, 4 => {Self, 4}}, clients = maps:put(Self, {make_ref(), [1, 2, 3, 4]}, #{})}),
  [
    ?_assertEqual(#{}, Clients),
    ?_assertEqual(#{}, Orders)
  ].

worker_failed_t_(_) ->
  Self = self(),
  Ref = make_ref(),
  {noreply, #state{clients = Clients1, orders = Orders1}} = handle_info({'DOWN', 1, process, 1, normal}, #state{orders = #{1 => {Self, 1}, 2 => {Self, 2}, 3 => {Self, 3}, 4 => {Self, 4}}, clients = maps:put(Self, {Ref, [1, 2, 3, 4]}, #{})}),
  {noreply, #state{clients = Clients2, orders = Orders2}} = handle_info({'DOWN', 1, process, 1, normal}, #state{orders = #{1 => {Self, 1}}, clients = maps:put(Self, {Ref, [1]}, #{})}),
  [
    ?_assertEqual(maps:put(Self, {Ref, [2, 3, 4]}, #{}), Clients1),
    ?_assertEqual(#{2 => {Self, 2}, 3 => {Self, 3}, 4 => {Self, 4}}, Orders1),
    ?_assertEqual(#{}, Clients2),
    ?_assertEqual(#{}, Orders2)
  ].

-endif.


