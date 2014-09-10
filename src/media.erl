%% Copyright
-module(media).
-author("ilia").

%% API
-export([start/0, stop/0]).

start() ->
  ulitos_app:ensure_started(media).

stop() ->
  application:stop(media),
  application:unload(media).
