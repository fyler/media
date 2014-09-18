%%%-------------------------------------------------------------------
%%% @author ilia
%%% @doc
%%%   Module for writing VOD HLS.
%%% @end
%%%-------------------------------------------------------------------
-module(hls_writer).

-include("log.hrl").
-include_lib("erlmedia/include/video_frame.hrl").
-include_lib("mpegts/include/mpegts.hrl").

-define(DELTA, 200).
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
                    last_tables = undefined :: undefined | non_neg_integer(),
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

write_frame(#video_frame{flavor = metadata}, #hls_state{} = State) ->
  {ok, State};

write_frame(#video_frame{flavor = config} = Frame, #hls_state{streamer = Streamer} = State) ->
  {NewStreamer, undefined} = encode_frame(Streamer, Frame),
  {ok, State#hls_state{streamer = NewStreamer}};

write_frame(#video_frame{} = Frame, #hls_state{} = State) ->
  {ok, add_frame(Frame, State)};

write_frame(eof, #hls_state{frames = [#hls_frame{dts = Dts}| _] = Frames, dir = Dir, playlist = Playlist, start = Start, count = N} = State) ->
  write(Dir, N, Frames, Playlist, Dts - Start),
  finish_playlist(Playlist),
  {ok, State}.

add_frame(#video_frame{flavor = keyframe, dts = Dts} = Frame, #hls_state{streamer = Streamer, frames = []} = State) ->
  {NewStreamer, EncFrame} = encode_frame(Streamer, Frame),
  State#hls_state{streamer = NewStreamer, frames = [EncFrame], last_keyframe = Dts, start = Dts};

add_frame(#video_frame{flavor = keyframe, dts = Dts} = Frame, #hls_state{streamer = Streamer, start = Start} = State) when Dts - Start < ?DELTA ->
  {NewStreamer, EncFrame} = encode_frame(Streamer#streamer{pat_counter = 0, pmt_counter = 0}, Frame),
  State#hls_state{streamer = NewStreamer, frames = [EncFrame], last_keyframe = Dts, start = Dts};

add_frame(#video_frame{flavor = keyframe, dts = Dts} = Frame, #hls_state{streamer = Streamer, dir = Dir, playlist = Playlist, frames = Frames, last_keyframe = undefined, start = Start, count = N} = State) ->
  {NewStreamer, EncFrame} = encode_frame(Streamer, Frame),
  write(Dir, N, Frames, Playlist, Dts - Start),
  State#hls_state{streamer = NewStreamer, frames = [EncFrame], last_keyframe = Dts, start = Dts, count = N + 1};

add_frame(#video_frame{flavor = keyframe, dts = Dts} = Frame, #hls_state{streamer = Streamer, dir = Dir, playlist = Playlist, duration = Duration, frames = Frames, last_keyframe = LastDts, start = Start, count = N} = State) when Start + Duration - ?DELTA < Dts ->
  {NewStreamer, EncFrame} = encode_frame(Streamer, Frame),
  Pred = fun(#hls_frame{dts = Dts1}) when Dts1 > LastDts -> true;
    (#hls_frame{dts = Dts1, flavor = keyframe}) when Dts1 == LastDts -> true;
    (#hls_frame{}) -> false
  end,
  {NewFrames, FramesToSegment} = lists:splitwith(Pred, Frames),
  write(Dir, N, FramesToSegment, Playlist, LastDts - Start),
  State#hls_state{streamer = NewStreamer, frames = [EncFrame|NewFrames], last_keyframe = Dts, start = LastDts, count = N + 1};

add_frame(#video_frame{flavor = keyframe, dts = Dts} = Frame, #hls_state{streamer = Streamer, frames = Frames} = State) ->
  {NewStreamer, EncFrame} = encode_frame(Streamer, Frame),
  State#hls_state{streamer = NewStreamer, frames = [EncFrame|Frames], last_keyframe = Dts};

add_frame(#video_frame{content = video}, #hls_state{last_keyframe = undefined} = State) ->
  State;

add_frame(#video_frame{dts = Dts} = Frame, #hls_state{streamer = Streamer, frames = []} = State) ->
  {NewStreamer, EncFrame} = encode_frame(Streamer#streamer{sent_pat = false}, Frame),
  State#hls_state{streamer = NewStreamer, frames = [EncFrame], start = Dts};

add_frame(#video_frame{dts = Dts} = Frame, #hls_state{streamer = Streamer, dir = Dir, playlist = Playlist, duration = Duration, frames = Frames, last_keyframe = undefined, start = Start, count = N} = State) when Start + Duration =< Dts ->
  {NewStreamer, EncFrame} = encode_frame(Streamer#streamer{sent_pat = false}, Frame),
  write(Dir, N, Frames, Playlist, Dts - Start),
  State#hls_state{streamer = NewStreamer, frames = [EncFrame], start = Dts, count = N + 1};

add_frame(#video_frame{dts = Dts} = Frame, #hls_state{streamer = Streamer, frames = Frames, start = Start, duration = Duration} = State) when Start + Duration + ?DELTA < Dts ->
  {NewStreamer, EncFrame} = encode_frame(Streamer#streamer{sent_pat = false}, Frame),
  State#hls_state{streamer = NewStreamer, frames = [EncFrame | Frames], last_tables = Dts};

add_frame(#video_frame{dts = Dts} = Frame, #hls_state{streamer = Streamer, dir = Dir, playlist = Playlist, duration = Duration, frames = Frames, last_keyframe = LastDts, start = Start, count = N} = State) when Start + 3 * Duration / 2 < Dts andalso Start < LastDts andalso Dts - LastDts < Duration ->
  {NewStreamer, EncFrame} = encode_frame(Streamer, Frame),
  Pred = fun(#hls_frame{dts = Dts1}) when Dts1 > LastDts -> true;
    (#hls_frame{dts = Dts1, flavor = keyframe}) when Dts1 == LastDts -> true;
    (#hls_frame{}) -> false
  end,
  {NewFrames, FramesToSegment} = lists:splitwith(Pred, Frames),
  write(Dir, N, FramesToSegment, Playlist, LastDts - Start),
  State#hls_state{streamer = NewStreamer, frames = [EncFrame|NewFrames], start = LastDts, count = N + 1};

add_frame(#video_frame{dts = Dts} = Frame, #hls_state{streamer = Streamer, dir = Dir, playlist = Playlist, duration = Duration, frames = Frames, last_keyframe = LastDts, start = Start, count = N} = State) when Start + 3 * Duration / 2 < Dts andalso Start < LastDts andalso Dts - LastDts >= Duration ->
  {NewStreamer, EncFrame} = encode_frame(Streamer#streamer{sent_pat = false}, Frame),
  Pred = fun(#hls_frame{dts = Dts1}) when Dts1 > LastDts -> true;
    (#hls_frame{dts = Dts1, flavor = keyframe}) when Dts1 == LastDts -> true;
    (#hls_frame{}) -> false
  end,
  {NewFrames, FramesToSegment} = lists:splitwith(Pred, Frames),
  write(Dir, N, FramesToSegment, Playlist, LastDts - Start),
  State#hls_state{streamer = NewStreamer, frames = [EncFrame|NewFrames], start = LastDts, last_tables = Dts, count = N + 1};

add_frame(#video_frame{dts = Dts} = Frame, #hls_state{streamer = Streamer, dir = Dir, playlist = Playlist, duration = Duration, frames = Frames, last_tables = LastTables, start = Start, count = N} = State) when Start + 3 * Duration / 2 < Dts ->
  {NewStreamer, EncFrame} = encode_frame(Streamer, Frame),
  Pred = fun(#hls_frame{dts = Dts1}) when Dts1 > LastTables -> true;
    (#hls_frame{dts = Dts1, flavor = keyframe}) when Dts1 == LastTables -> true;
    (#hls_frame{}) -> false
  end,
  {NewFrames, FramesToSegment} = lists:splitwith(Pred, Frames),
  write(Dir, N, FramesToSegment, Playlist, LastTables - Start),
  State#hls_state{streamer = NewStreamer, frames = [EncFrame|NewFrames], start = LastTables, last_keyframe = undefined, count = N + 1};

add_frame(#video_frame{} = Frame, #hls_state{streamer = Streamer, frames = Frames} = State) ->
  {NewStreamer, EncFrame} = encode_frame(Streamer, Frame),
  State#hls_state{streamer = NewStreamer, frames = [EncFrame|Frames]}.

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


