-module(media_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
  lager:info("Starting application: Media"),
  ulitos:load_config(media, "media.conf"),
  media_sup:start_link().

stop(_State) ->
  ok.
