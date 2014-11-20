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
  content :: frame_content(),
  flavor :: frame_flavor(),
  point :: true | false
}).

-record(hls_state, {
  streamer,
  duration :: non_neg_integer(),
  playlist :: file:fd(),
  dir :: string(),
  pid :: pid(),
  header :: string(),
  tracks_queue = queue:new() :: queue:queue(),
  max = 3 :: non_neg_integer(),
  frames = [] :: [#hls_frame{}],
  last_keyframe = undefined :: undefined | non_neg_integer(),
  last_tables = undefined :: undefined | non_neg_integer(),
  start = 0 :: non_neg_integer(),
  count = 0 :: non_neg_integer(),
  actions = [] :: [atom()]
}).

%% API
-export([init_file/1, write_frame/2]).

init_file(Options) ->
  State1 = init_vod(Options, #hls_state{}),
  State = init_live(Options, State1),
  {ok, State}.

init_live(#{pid := Pid} = Options, #hls_state{actions = Actions} = State) ->
  Duration = maps:get(duration, Options, 20000),
  Video = maps:get(video, Options, h264),
  Audio = maps:get(audio, Options, aac),
  Header = io_lib:format("#EXTM3U~n#EXT-X-TARGETDURATION:~p~n#EXT-X-VERSION:3~n", [Duration div 1000]),
  Streamer = mpegts:init(),
  State#hls_state{streamer = Streamer#streamer{audio_codec = Audio, video_codec = Video}, duration = Duration, header = Header, pid = Pid, actions = [store | Actions]};

init_live(_Options, State) ->
  State.

init_vod(#{dir := Dir} = Options, #hls_state{actions = Actions} = State) ->
  Duration = maps:get(duration, Options, 20000),
  PlaylistName = Dir ++ "/" ++ maps:get(playlist, Options, "playlist.m3u8"),
  Video = maps:get(video, Options, h264),
  Audio = maps:get(audio, Options, aac),
  Playlist = lists:flatten(io_lib:format("#EXTM3U~n#EXT-X-PLAYLIST-TYPE:VOD~n#EXT-X-TARGETDURATION:~p~n#EXT-X-VERSION:3~n", [Duration div 1000])),
  filelib:ensure_dir(Dir ++ "/"),
  Streamer = mpegts:init(),
  {ok, PlaylistFile} = file:open(PlaylistName, [write]),
  file:write(PlaylistFile, Playlist),
  State#hls_state{streamer = Streamer#streamer{audio_codec = Audio, video_codec = Video}, duration = Duration, playlist = PlaylistFile, dir = Dir, actions = [write | Actions]};

init_vod(_Options, State) ->
  State.

write_frame(#video_frame{flavor = metadata}, #hls_state{} = State) ->
  {ok, State};

write_frame(#video_frame{flavor = config} = Frame, #hls_state{streamer = Streamer} = State) ->
  {NewStreamer, undefined} = encode_frame(Streamer, Frame),
  {ok, State#hls_state{streamer = NewStreamer}};

write_frame(#video_frame{} = Frame, #hls_state{} = State) ->
  {ok, add_frame(Frame, State)};

write_frame(eof, #hls_state{frames = [#hls_frame{dts = Dts}| _] = Frames, start = Start} = State) ->
  action(State, Frames, Dts - Start),
  action(State, eof, 0),
  {ok, State}.

add_frame(#video_frame{flavor = keyframe, dts = Dts} = Frame, #hls_state{streamer = Streamer, frames = []} = State) ->
  {NewStreamer, EncFrame} = encode_frame(Streamer, Frame),
  State#hls_state{streamer = NewStreamer, frames = [EncFrame], last_keyframe = Dts, last_tables = undefined, start = Dts};

add_frame(#video_frame{flavor = keyframe, dts = Dts} = Frame, #hls_state{streamer = Streamer, last_keyframe = undefined, start = Start, count = 0} = State) when Dts - Start < ?DELTA ->
  {NewStreamer, EncFrame} = encode_frame(Streamer#streamer{pat_counter = 0, pmt_counter = 0}, Frame),
  State#hls_state{streamer = NewStreamer, frames = [EncFrame], last_keyframe = Dts, last_tables = undefined, start = Dts};

add_frame(#video_frame{flavor = keyframe, dts = Dts} = Frame, #hls_state{streamer = Streamer, frames = Frames, last_keyframe = undefined, start = Start, count = N} = State) ->
  {NewStreamer, EncFrame} = encode_frame(Streamer, Frame),
  %%write(Dir, N, Frames, Playlist, Dts - Start),
  NewState = action(State, Frames, Dts - Start),
  NewState#hls_state{streamer = NewStreamer, frames = [EncFrame], last_keyframe = Dts, last_tables = undefined, start = Dts, count = N + 1};

add_frame(#video_frame{flavor = keyframe, dts = Dts} = Frame, #hls_state{streamer = Streamer, duration = Duration, frames = Frames, last_keyframe = LastDts, start = Start, count = N} = State) when Start + Duration - ?DELTA < Dts ->
  {NewStreamer, EncFrame} = encode_frame(Streamer, Frame),
  {NewFrames, FramesToSegment} = split_frames(Frames, LastDts),
  %%write(Dir, N, FramesToSegment, Playlist, LastDts - Start),
  NewState = action(State, FramesToSegment, LastDts - Start),
  NewState#hls_state{streamer = NewStreamer, frames = [EncFrame|NewFrames], last_keyframe = Dts, last_tables = undefined, start = LastDts, count = N + 1};

add_frame(#video_frame{flavor = keyframe, dts = Dts} = Frame, #hls_state{streamer = Streamer, frames = Frames} = State) ->
  {NewStreamer, EncFrame} = encode_frame(Streamer, Frame),
  State#hls_state{streamer = NewStreamer, frames = [EncFrame|Frames], last_keyframe = Dts, last_tables = undefined};

add_frame(#video_frame{content = video}, #hls_state{last_keyframe = undefined} = State) ->
  State;

add_frame(#video_frame{content = video} = Frame, #hls_state{streamer = Streamer, frames = Frames} = State) ->
  {NewStreamer, EncFrame} = encode_frame(Streamer, Frame),
  State#hls_state{streamer = NewStreamer, frames = [EncFrame|Frames]};

add_frame(#video_frame{dts = Dts} = Frame, #hls_state{streamer = Streamer, frames = []} = State) ->
  {NewStreamer, EncFrame} = encode_frame(Streamer#streamer{sent_pat = false}, Frame),
  State#hls_state{streamer = NewStreamer, frames = [EncFrame], start = Dts};

add_frame(#video_frame{dts = Dts} = Frame, #hls_state{streamer = Streamer, frames = Frames, duration = Duration, last_keyframe = undefined, last_tables = undefined, start = Start} = State) when Start + Duration =< Dts ->
  {NewStreamer, EncFrame} = encode_frame(Streamer#streamer{sent_pat = false}, Frame),
  State#hls_state{streamer = NewStreamer, frames = [EncFrame | Frames], last_tables = Dts};

add_frame(#video_frame{dts = Dts} = Frame, #hls_state{streamer = Streamer, frames = Frames, last_tables = undefined, start = Start, duration = Duration} = State) when Start + Duration + ?DELTA < Dts ->
  {NewStreamer, EncFrame} = encode_frame(Streamer#streamer{sent_pat = false}, Frame),
  State#hls_state{streamer = NewStreamer, frames = [EncFrame | Frames], last_tables = Dts};

add_frame(#video_frame{dts = Dts} = Frame, #hls_state{streamer = Streamer, duration = Duration, frames = Frames, last_keyframe = undefined, last_tables = LastTables, start = Start, count = N} = State) when Start + Duration + ?DELTA < Dts ->
  {NewStreamer, EncFrame} = encode_frame(Streamer, Frame),
  {NewFrames, FramesToSegment} = split_frames(Frames, LastTables),
  %%write(Dir, N, FramesToSegment, Playlist, LastTables - Start),
  NewState = action(State, FramesToSegment, LastTables - Start),
  NewState#hls_state{streamer = NewStreamer, frames = [EncFrame|NewFrames], last_tables = undefined, start = LastTables, count = N + 1};

add_frame(#video_frame{dts = Dts} = Frame, #hls_state{streamer = Streamer, duration = Duration, frames = Frames, last_keyframe = LastDts, start = Start, count = N} = State) when Start + 3 * Duration / 2 < Dts andalso Start < LastDts andalso Dts - LastDts < Duration ->
  {NewStreamer, EncFrame} = encode_frame(Streamer, Frame),
  {NewFrames, FramesToSegment} = split_frames(Frames, LastDts),
  %%write(Dir, N, FramesToSegment, Playlist, LastDts - Start),
  NewState = action(State, FramesToSegment, LastDts - Start),
  NewState#hls_state{streamer = NewStreamer, frames = [EncFrame|NewFrames], start = LastDts, count = N + 1};

add_frame(#video_frame{dts = Dts} = Frame, #hls_state{streamer = Streamer, duration = Duration, frames = Frames, last_keyframe = LastDts, start = Start, count = N} = State) when Start + 3 * Duration / 2 < Dts andalso Start < LastDts andalso Dts - LastDts >= Duration ->
  {NewStreamer, EncFrame} = encode_frame(Streamer#streamer{sent_pat = false}, Frame),
  {NewFrames, FramesToSegment} = split_frames(Frames, LastDts),
  %%write(Dir, N, FramesToSegment, Playlist, LastDts - Start),
  NewState = action(State, FramesToSegment, LastDts - Start),
  NewState#hls_state{streamer = NewStreamer, frames = [EncFrame|NewFrames], start = LastDts, last_tables = Dts, count = N + 1};

add_frame(#video_frame{dts = Dts} = Frame, #hls_state{streamer = Streamer, duration = Duration, frames = Frames, last_tables = LastTables, start = Start, count = N} = State) when Start + 3 * Duration / 2 < Dts ->
  {NewStreamer, EncFrame} = encode_frame(Streamer, Frame),
  {NewFrames, FramesToSegment} = split_frames(Frames, LastTables),
  %%write(Dir, N, FramesToSegment, Playlist, LastTables - Start),
  NewState = action(State, FramesToSegment, LastTables - Start),
  NewState#hls_state{streamer = NewStreamer, frames = [EncFrame|NewFrames], start = LastTables, last_keyframe = undefined, last_tables = undefined, count = N + 1};

add_frame(#video_frame{} = Frame, #hls_state{streamer = Streamer, frames = Frames} = State) ->
  {NewStreamer, EncFrame} = encode_frame(Streamer, Frame),
  State#hls_state{streamer = NewStreamer, frames = [EncFrame|Frames]}.

encode_frame(#streamer{sent_pat = false} = Streamer, #video_frame{content = Content, flavor = Flavor, pts = Pts, dts = Dts} = Frame) ->
  {NewStreamer, Data} = mpegts:encode(Streamer, Frame),
  NewFrame = case Data of
               <<>> ->
                 undefined;
               [] ->
                 undefined;
               _Else ->
                 #hls_frame{content = Content, data = Data, flavor = Flavor, pts = Pts, dts = Dts, point = true}
             end,
  {NewStreamer, NewFrame};

encode_frame(Streamer, #video_frame{content = video, flavor = keyframe, pts = Pts, dts = Dts} = Frame) ->
  {NewStreamer, Data} = mpegts:encode(Streamer, Frame),
  NewFrame = case Data of
               <<>> ->
                 undefined;
               [] ->
                 undefined;
               _Else ->
                 #hls_frame{content = video, data = Data, flavor = keyframe, pts = Pts, dts = Dts, point = true}
             end,
  {NewStreamer, NewFrame};

encode_frame(Streamer, #video_frame{content = Content, flavor = Flavor, pts = Pts, dts = Dts} = Frame) ->
  {NewStreamer, Data} = mpegts:encode(Streamer, Frame),
  NewFrame = case Data of
               <<>> ->
                 undefined;
               [] ->
                 undefined;
               _Else ->
                 #hls_frame{content = Content, data = Data, flavor = Flavor, pts = Pts, dts = Dts, point = false}
             end,
  {NewStreamer, NewFrame}.

action(#hls_state{actions = Actions} = State, Frames, Duration) ->
  action(State, Frames, Duration, Actions).

action(State, Frames, Duration, [write | Actions]) ->
  action(write(State, Frames, Duration), Frames, Duration, Actions);

action(State, Frames, Duration, [store | Actions]) ->
  action(store(State, Frames, Duration), Frames, Duration, Actions);

action(State, _Frames, _Duration, []) ->
  State.

write(#hls_state{playlist = Playlist} = State, eof,  _Duration) ->
  file:write(Playlist, iolist_to_binary("#EXT-X-ENDLIST")),
  file:close(Playlist),
  State;

write(#hls_state{dir = Dir, count = N, playlist = Playlist} = State, Frames,  Duration) ->
  FileName = Dir ++ "/stream_" ++ integer_to_list(N) ++ ".ts",
  {ok, File} = file:open(FileName, [write, binary]),
  file:write(File, list_to_binary(lists:reverse([Data || #hls_frame{data = Data} <- Frames]))),
  file:close(File),
  file:write(Playlist, iolist_to_binary(lists:flatten(io_lib:format("#EXTINF:~.3f,~nstream_~p.ts~n", [Duration / 1000, N])))),
  State.

store(#hls_state{pid = Pid, max = Max, header = Header, tracks_queue = Tracks, count = Number} = State, eof, _Duration) ->
  N =
    if
      Number >= Max -> Number - Max + 2;
      true -> 1
    end,
  Playlist = iolist_to_binary([Header, io_lib:format("#EXT-X-MEDIA-SEQUENCE:~p~n", [N]) | queue:to_list(Tracks)]),
  Pid ! {Playlist, eof},
  State;

store(#hls_state{pid = Pid, count = Number, max = Max, header = Header, tracks_queue = Tracks} = State, Frames, Duration) ->
  N =
    if
      Number >= Max ->
        NewTracks = queue:drop(queue:in(io_lib:format("#EXTINF:~.3f,~nstream_~p.ts~n", [Duration, Number]), Tracks)),
        Number - Max + 2;
      true ->
        NewTracks = queue:in(io_lib:format("#EXTINF:~.3f,~nstream_~p.ts~n", [Duration, Number]), Tracks),
        1
    end,
  Playlist = iolist_to_binary([Header, io_lib:format("#EXT-X-MEDIA-SEQUENCE:~p~n", [N]) | queue:to_list(NewTracks)]),
  Pid ! {Playlist, list_to_binary(lists:reverse([Data || #hls_frame{data = Data} <- Frames]))},
  State#hls_state{tracks_queue = NewTracks}.

split_frames(Frames, Dts) ->
  split_frames(Frames, Dts, []).

split_frames([#hls_frame{dts = Dts1} = Frame | Frames], Dts, Acc) when Dts1 > Dts ->
  split_frames(Frames, Dts, [Frame | Acc]);

split_frames([#hls_frame{dts = Dts1, point = false} = Frame | Frames], Dts, Acc) when Dts1 == Dts ->
  split_frames(Frames, Dts, [Frame | Acc]);

split_frames([#hls_frame{dts = Dts1, point = true} = Frame | Frames], Dts, Acc) when Dts1 == Dts ->
  {lists:reverse([Frame | Acc]), Frames}.



