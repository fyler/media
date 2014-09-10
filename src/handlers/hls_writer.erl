%%%-------------------------------------------------------------------
%%% @author ilia
%%% @doc
%%%   Module for writing VOD HLS.
%%% @end
%%%-------------------------------------------------------------------
-module(hls_writer).

%%-include("log.hrl").
-include_lib("erlmedia/include/video_frame.hrl").
-include_lib("mpegts/include/mpegts.hrl").

-define(DELTA, 200).
-define(D(X), lager:debug("~p:~p ~p",[?MODULE, ?LINE, X])).
-record(hls_frame, {
  data :: binary(),
  pts :: non_neg_integer(),
  dts :: non_neg_integer(),
  content,
  flavor
}).

-record(hls_state, {streamer,
                    duration,
                    playlist,
                    dir,
                    frames = [] :: [#hls_frame{}],
                    last_keyframe = undefined :: undefined | non_neg_integer(),
                    start = 0 :: non_neg_integer(),
                    count = 0 :: non_neg_integer()
}).

%% API
-export([init_file/1, write_frame/2]).

init_file(Options) when is_map(Options) ->
  Dir = maps:get(dir, Options),
  Duration = maps:get(duration, Options, 20000),
  PlaylistName = Dir ++ "/" ++ maps:get(playlist, Options, "playlist.m3u8"),
  Video = maps:get(video, Options, h264),
  Audio = maps:get(audio, Options, aac),
  Playlist = lists:flatten(io_lib:format("#EXTM3U~n#EXT-X-PLAYLIST-TYPE:VOD~n#EXT-X-TARGETDURATION:~p~n#EXT-X-VERSION:3~n", [Duration div 1000])),
  filelib:ensure_dir(Dir ++ "/"),
  Streamer = mpegts:init(),
  {ok, PlaylistFile} = file:open(PlaylistName, [write]),
  file:write(PlaylistFile, Playlist),
  {ok, #hls_state{streamer = Streamer#streamer{audio_codec = Audio, video_codec = Video}, duration = Duration, playlist = PlaylistFile, dir = Dir}}.

write_frame(#video_frame{flavor = config} = Frame, #hls_state{streamer = Streamer} = State) ->
  {NewStreamer, undefined} = encode_frame(Streamer, Frame),
  {ok, State#hls_state{streamer = NewStreamer}};

write_frame(#video_frame{} = Frame, #hls_state{streamer = Streamer} = State) ->
  case encode_frame(Streamer, Frame) of
    {NewStreamer, undefined} ->
      {ok, State#hls_state{streamer = NewStreamer}};
    {NewStreamer, HlsFrame} ->
      {ok, add_frame(HlsFrame, State#hls_state{streamer = NewStreamer})}
  end;

write_frame(eof, #hls_state{frames = [#hls_frame{dts = Dts}| _] = Frames, dir = Dir, playlist = Playlist, start = Start, count = N} = State) ->
  write(Dir, N, Frames, Playlist, Dts - Start),
  finish_playlist(Playlist),
  {ok, State}.

add_frame(#hls_frame{flavor = keyframe, dts = Dts} = Frame, #hls_state{frames = []} = State) ->
  State#hls_state{frames = [Frame], last_keyframe = Dts, start = Dts};

add_frame(#hls_frame{flavor = keyframe, dts = Dts} = Frame, #hls_state{dir = Dir, playlist = Playlist, frames = Frames, last_keyframe = undefined, start = Start, count = N} = State) ->
  write(Dir, N, Frames, Playlist, Dts - Start),
  State#hls_state{frames = [Frame], last_keyframe = Dts, start = Dts, count = N + 1};

add_frame(#hls_frame{flavor = keyframe, dts = Dts} = Frame, #hls_state{dir = Dir, playlist = Playlist, duration = Duration, frames = Frames, last_keyframe = LastDts, start = Start, count = N} = State) when Start + Duration + ?DELTA < Dts ->
  Pred = fun(#hls_frame{dts = Dts1}) when Dts1 > LastDts -> true;
    (#hls_frame{dts = Dts1, flavor = keyframe}) when Dts1 == LastDts -> true;
    (#hls_frame{}) -> false
  end,
  {NewFrames, FramesToSegment} = lists:splitwith(Pred, Frames),
  write(Dir, N, FramesToSegment, Playlist, LastDts - Start),
  State#hls_state{frames = [Frame|NewFrames], last_keyframe = Dts, start = LastDts, count = N + 1};

add_frame(#hls_frame{flavor = keyframe, dts = Dts} = Frame, #hls_state{frames = Frames} = State) ->
  State#hls_state{frames = [Frame|Frames], last_keyframe = Dts};

add_frame(#hls_frame{content = video}, #hls_state{last_keyframe = undefined} = State) ->
  State;

add_frame(#hls_frame{dts = Dts} = Frame, #hls_state{frames = []} = State) ->
  State#hls_state{frames = [Frame], start = Dts};

add_frame(#hls_frame{dts = Dts} = Frame, #hls_state{dir = Dir, playlist = Playlist, duration = Duration, frames = Frames, last_keyframe = undefined, start = Start, count = N} = State) when Start + Duration + ?DELTA < Dts ->
  Pred = fun(#hls_frame{dts = Dts1}) when Dts1 >= Dts -> true;
    (#hls_frame{}) -> false
  end,
  {NewFrames, FramesToSegment} = lists:splitwith(Pred, Frames),
  write(Dir, N, FramesToSegment, Playlist, Dts - Start),
  State#hls_state{frames = [Frame|NewFrames], start = Dts, count = N + 1};

add_frame(#hls_frame{} = Frame, #hls_state{frames = Frames} = State) ->
  State#hls_state{frames = [Frame|Frames]}.

encode_frame(Streamer, #video_frame{content = Content, flavor = Flavor, pts = Pts, dts = Dts} = Frame) ->
  {NewStreamer, Data} = mpegts:encode(Streamer, Frame),
  NewFrame = case Data of
               <<>> ->
                 undefined;
               [] ->
                 undefined;
               _Else ->
                 #hls_frame{content = Content, data = Data, flavor = Flavor, pts = Pts, dts = Dts}
             end,
  {NewStreamer, NewFrame}.

write(Dir, N, Frames, Playlist, Duration) ->
  FileName = Dir ++ "/stream_" ++ integer_to_list(N) ++ ".ts",
  {ok, File} = file:open(FileName, [write, binary]),
  file:write(File, list_to_binary(lists:reverse([Data || #hls_frame{data = Data} <- Frames]))),
  file:close(File),
  file:write(Playlist, iolist_to_binary(lists:flatten(io_lib:format("#EXTINF:~.3f,~nstream_~p.ts~n", [Duration / 1000, N])))).

finish_playlist(Playlist) ->
  file:write(Playlist, iolist_to_binary("#EXT-X-ENDLIST")),
  file:close(Playlist).


