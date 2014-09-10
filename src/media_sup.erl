-module(media_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
  supervisor:start_link(?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
  Children = [
    {ffmpeg_sup,
      {supervisor,start_link,[ffmpeg_sup, []]},
      permanent,
      infinity,
      supervisor,
      []
    },
    {writer_sup,
      {supervisor,start_link,[writer_sup, []]},
      permanent,
      infinity,
      supervisor,
      []
    }
  ],
  {ok, {{one_for_one, 5, 10}, Children}}.

