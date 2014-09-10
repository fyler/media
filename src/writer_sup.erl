%%%-------------------------------------------------------------------
%%% @author ilia
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 27. июн 2014 17:46
%%%-------------------------------------------------------------------
-module(writer_sup).
-author("ilia").

-behaviour(supervisor).

%% API
-export([]).

%% Supervisor callbacks
-export([init/1, start_worker/3, stop_worker/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

start_worker(Input, Output, Options) ->
  supervisor:start_child(writer_worker_sup, [Input, Output, Options]).

stop_worker(Pid) ->
  supervisor:terminate_child(ffmpeg_worker_sup, Pid).

init([worker]) ->
  {ok, {{simple_one_for_one, 5, 10}, [
    {undefined, {media_writer, start_link, []},
      temporary, 2000, worker, [media_writer]}
  ]}};

init([]) ->
  Children = [
    {writer_worker_sup,
      {supervisor,start_link,[{local, writer_worker_sup}, ?MODULE, [worker]]},
      permanent,
      infinity,
      supervisor,
      []
    },
    ?CHILD(media_server, worker)],
  {ok, {{one_for_one, 5, 10}, Children}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
