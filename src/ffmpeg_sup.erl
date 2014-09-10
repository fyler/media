%%%-------------------------------------------------------------------
%%% @author ilia
%%% @doc
%%%
%%% @end
%%% Created : 27. июн 2014 17:42
%%%-------------------------------------------------------------------
-module(ffmpeg_sup).
-author("ilia").

-behaviour(supervisor).

%% API
-export([start_worker/1, stop_worker/1]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

start_worker(Ref) ->
  supervisor:start_child(ffmpeg_worker_sup, [Ref]).

stop_worker(Pid) ->
  supervisor:terminate_child(ffmpeg_worker_sup, Pid).

init([worker]) ->
  {ok, {{simple_one_for_one, 5, 10}, [
    {undefined, {ffmpeg_worker, start_link, []},
      transient, 2000, worker, [ffmpeg_worker]}
  ]}};

init([]) ->
  Children = [
    {ffmpeg_worker_sup,
      {supervisor,start_link,[{local, ffmpeg_worker_sup}, ?MODULE, [worker]]},
      permanent,
      infinity,
      supervisor,
      []
    },
    ?CHILD(ffmpeg_server, worker)],
  {ok, {{one_for_all, 5, 10}, Children}}.


