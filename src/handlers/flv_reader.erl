%%% @author     Max Lapshin <max@maxidoors.ru> [http://erlyvideo.org]
%%% @copyright  2009 Max Lapshin
%%% @doc        FLV reader for erlyvideo
%%% @reference  See <a href="http://erlyvideo.org/" target="_top">http://erlyvideo.org</a> for more information
%%% @end
%%%
%%% This file is part of erlyvideo.
%%% 
%%% erlyvideo is free software: you can redistribute it and/or modify
%%% it under the terms of the GNU General Public License as published by
%%% the Free Software Foundation, either version 3 of the License, or
%%% (at your option) any later version.
%%%
%%% erlyvideo is distributed in the hope that it will be useful,
%%% but WITHOUT ANY WARRANTY; without even the implied warranty of
%%% MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
%%% GNU General Public License for more details.
%%%
%%% You should have received a copy of the GNU General Public License
%%% along with erlyvideo.  If not, see <http://www.gnu.org/licenses/>.
%%%
%%%---------------------------------------------------------------------------------------
-module(flv_reader).
-author('Max Lapshin <max@maxidoors.ru>').
-include_lib("erlmedia/include/video_frame.hrl").
-include_lib("erlmedia/include/media_info.hrl").
-include_lib("erlmedia/include/flv.hrl").
-include_lib("erlmedia/include/aac.hrl").
-include_lib("erlmedia/include/mp3.hrl").
-include_lib("stdlib/include/ms_transform.hrl").
-include("log.hrl").

-behaviour(gen_format).
-export([init/2, read_frame/2, media_info/1, properties/1, seek/3, can_open_file/1, write_frame/2]).
-export([keyframes/1]).

-record(flv_media, {
  reader,
  frames,
  header,
  metadata = [],
  metadata_offset,
  duration,
  height,
  width,
  bitrate,
  audio_config,
  audio_codec,
  video_config,
  video_codec
}).


can_open_file(Name) when is_binary(Name) ->
  can_open_file(binary_to_list(Name));

can_open_file(Name) ->
  filename:extension(Name) == ".flv".


write_frame(_Device, _Frame) -> 
  erlang:error(unsupported).




%%--------------------------------------------------------------------
%% @spec ({IoModule::atom(), IoDev::iodev()}, Options) -> {ok, State} | {error,Reason::atom()}
%% @doc Read flv file and load its frames in memory ETS
%% @end 
%%--------------------------------------------------------------------
init({_Module,_Device} = Reader, Options) ->
  Media1 = #flv_media{reader = Reader, frames = ets:new(frames, [ordered_set, private])},
  ReadMeta = proplists:get_value(find_metadata, Options, true),
  case flv:read_header(Reader) of
    {#flv_header{} = Header, Offset} when ReadMeta ->
      read_frame_list(Media1#flv_media{header = Header}, Offset, -1);
    {#flv_header{} = Header, _} ->
      {ok, Media1#flv_media{header = Header}};
    eof -> 
      {error, unexpected_eof};
    {error, Reason} -> {error, Reason}           
  end.

first(Media) ->
  first(Media, flv:data_offset(), 0).

first(#flv_media{audio_config = A}, Id, DTS) when A =/= undefined ->
  {audio_config, Id, DTS};

first(#flv_media{video_config = V}, Id, DTS) when V =/= undefined ->
  {video_config, Id, DTS};

first(_, Id, _DTS) ->
  Id.


properties(#flv_media{metadata = Meta}) -> Meta.


media_info(#flv_media{width = Width, height = Height, duration = Duration, bitrate = Bitrate} = FLV) ->
  VideoStreams = case FLV#flv_media.video_codec of
    undefined -> [];
    _ -> [#stream_info{
      content = video,
      stream_id = 1,
      bitrate = Bitrate,
      codec = FLV#flv_media.video_codec,
      config = case FLV#flv_media.video_config of
        #video_frame{body = VideoBody} -> VideoBody;
        _ -> undefined
      end,
      params = #video_params{width = Width, height = Height}
    }]
  end,
  AudioStreams = case FLV#flv_media.audio_codec of
    undefined -> [];
    _ -> [#stream_info{
      content = audio,
      stream_id = 2,
      codec = FLV#flv_media.audio_codec,
      config = case FLV#flv_media.audio_config of
        #video_frame{body = AudioBody} -> AudioBody;
        _ -> undefined
      end,
      params = #audio_params{}
    }]
  end,
  
  #media_info{
    flow_type = file,
    audio = AudioStreams,
    video = VideoStreams,
    metadata = [],
    duration = Duration
  }.

max_duration(#flv_media{duration = D1}, D2) when D2 > D1 -> D2;
max_duration(#flv_media{duration = D1}, _D2) -> D1.

read_frame_list(#flv_media{} = MediaInfo, _Offset, 0) ->
  {ok, MediaInfo};

read_frame_list(#flv_media{reader = Reader, frames = FrameTable, metadata = Metadata} = MediaInfo, Offset, Limit) ->
  % We need to bypass PreviousTagSize and read header.
	case flv:read_frame(Reader, Offset)	of
	  #video_frame{content = metadata, body = Meta, next_id = NextOffset} ->
			case parse_metadata(MediaInfo, Meta) of
			  {MediaInfo1, true} ->	
          % ?D({"Found metadata, looking 10 frames ahead"}),
			    read_frame_list(MediaInfo1#flv_media{metadata_offset = Offset}, NextOffset, 10);
			  {MediaInfo1, false} ->
			    ?D("Found metadata, but it is not a tagging. Seek will be disabled~n"),
			    read_frame_list(MediaInfo1, NextOffset, 10)
			end;
		#video_frame{content = video, flavor = config, codec = Codec, next_id = NextOffset} = V ->
      % ?D({"Save flash video_config"}),
			read_frame_list(MediaInfo#flv_media{video_config = V, video_codec = Codec}, NextOffset, Limit - 1);

		#video_frame{content = audio, flavor = config, codec = Codec, next_id = NextOffset} = A ->
      % ?D({"Save flash audio config"}),
			read_frame_list(MediaInfo#flv_media{audio_config = A, audio_codec = Codec}, NextOffset, Limit - 1);
		  
  	#video_frame{content = video, flavor = keyframe, codec = Codec, dts = DTS, next_id = NextOffset} ->
  	  ets:insert(FrameTable, {DTS, Offset}),
			read_frame_list(MediaInfo#flv_media{duration = max_duration(MediaInfo, DTS), video_codec = Codec}, NextOffset, Limit - 1);

		#video_frame{content = audio, codec = Codec, next_id = NextOffset, dts = DTS} ->
			read_frame_list(MediaInfo#flv_media{audio_codec = Codec, duration = max_duration(MediaInfo, DTS)}, NextOffset, Limit - 1);

		#video_frame{next_id = NextOffset, dts = DTS} ->
			read_frame_list(MediaInfo#flv_media{duration = max_duration(MediaInfo, DTS)}, NextOffset, Limit - 1);

    eof ->
      % ?D({duration, MediaInfo#flv_media.duration, Offset}),
      {ok, MediaInfo#flv_media{
        metadata = lists:ukeymerge(1, [{duration, MediaInfo#flv_media.duration}], Metadata)
      }};
    {error, Reason} -> 
      {error, Reason}
  end.

get_int(Key, Meta, Coeff) ->
  case {proplists:get_value(Key, Meta), Coeff} of
    {undefined, _} -> undefined;
    {{object, []}, _} -> undefined;
    {Value, Coeff} when is_number(Coeff) andalso is_number(Value) -> Value*Coeff;
    {Value, {M, F}} -> 
      try M:F(round(Value)) of
        Int -> Int
      catch
        _:_ -> undefined
      end
  end.

b_to_atom(A) when is_atom(A) -> A;
b_to_atom(A) when is_binary(A) -> binary_to_atom(A, latin1).

parse_metadata(MediaInfo, [<<"onMetaData">>, {object, Meta}]) ->
  parse_metadata(MediaInfo, [<<"onMetaData">>, Meta]);

parse_metadata(MediaInfo, [<<"onMetaData">>, Meta]) ->
  Meta1 = [{b_to_atom(K),V} || {K,V} <- Meta],
  
  Meta2 = [{b_to_atom(K),V} || {K,V} <- Meta1, K =/= duration andalso K =/= keyframes andalso K =/= times],

  Duration = get_int(duration, Meta1, 1000),
  MediaInfo1 = MediaInfo#flv_media{
    width = case get_int(width, Meta1, 1) of
      undefined -> undefined;
      ElseW -> round(ElseW)
    end,
    height = case get_int(height, Meta1, 1) of
      undefined -> undefined;
      ElseH -> round(ElseH)
    end,
    duration = Duration,
    bitrate = case proplists:get_value(videodatarate, Meta1) of
      undefined -> undefined;
      BR -> round(BR*1000)
    end,
    metadata = [{duration,Duration}|Meta2]
  },
  case proplists:get_value(keyframes, Meta1) of
    {object, Keyframes} ->
      Offsets = proplists:get_value(filepositions, Keyframes),
      Times = proplists:get_value(times, Keyframes),
      ets:delete_all_objects(MediaInfo1#flv_media.frames),
      {insert_keyframes(MediaInfo1, Offsets, Times), true};
    _ -> {MediaInfo1, false}
  end;

parse_metadata(MediaInfo, Meta) ->
  ?D({"Unknown metadata", Meta}),
  {MediaInfo, false}.


keyframes(#flv_media{frames = Frames}) ->
  ets:tab2list(Frames).

insert_keyframes(MediaInfo, [], _) -> MediaInfo;
insert_keyframes(MediaInfo, _, []) -> MediaInfo;
insert_keyframes(#flv_media{frames = FrameTable} = MediaInfo, [Offset|Offsets], [Time|Times]) ->
  ets:insert(FrameTable, {round(Time*1000), round(Offset)}),
  insert_keyframes(MediaInfo, Offsets, Times).

seek(#flv_media{} = Media, TS, _Options) when TS == 0 orelse TS == undefined ->
  {{audio_config, first(Media), 0}, 0};

seek(#flv_media{frames = undefined} = Media, Timestamp, Options) ->
  erlang:error(flv_file_should_have_frame_table),
  find_frame_in_file(Media, Timestamp, proplists:get_value(seek_mode, Options, keyframe));

seek(#flv_media{frames = FrameTable} = Media, Timestamp, Options) ->
  case proplists:get_value(seek_mode, Options, keyframe) of
    keyframe -> find_keyframe_in_table(FrameTable, Timestamp);
    frame -> find_frame_in_file(Media, Timestamp, frame)
  end.
  
find_keyframe_in_table(FrameTable, Timestamp) ->  
  TimestampInt = round(Timestamp),
  Ids = ets:select(FrameTable, ets:fun2ms(fun({FrameTimestamp, Offset} = _Frame) when FrameTimestamp =< TimestampInt ->
    {Offset, FrameTimestamp}
  end)),
  
  % ?D({zz, ets:tab2list(FrameTable)}),
  
  case lists:reverse(Ids) of
    [{Offset, DTS} | _] -> {{audio_config,Offset,DTS}, DTS};
    _ -> undefined
  end.

find_frame_in_file(Media, Timestamp, SeekMode) ->
  find_frame_in_file(Media, Timestamp, 0, first(Media), first(Media), SeekMode).


find_frame_in_file(Media, Timestamp, PrevTS, PrevOffset, Offset, SeekMode) ->
  case read_frame(Media, Offset) of
    
    #video_frame{dts = DTS} when DTS >= Timestamp andalso SeekMode == frame -> 
      {{audio_config, Offset, DTS}, DTS};
    #video_frame{flavor = keyframe, dts = DTS} when DTS > Timestamp andalso SeekMode == keyframe -> 
      {{audio_config, PrevOffset, PrevTS}, PrevTS};
      
      
    #video_frame{flavor = keyframe, dts = DTS, next_id = Next} -> 
      find_frame_in_file(Media, Timestamp, DTS, Offset, Next, SeekMode);

    #video_frame{dts = DTS, next_id = Next} when SeekMode == frame -> 
      find_frame_in_file(Media, Timestamp, DTS, Offset, Next, SeekMode);
      
    #video_frame{next_id = Next} ->
      find_frame_in_file(Media, Timestamp, PrevTS, PrevOffset, Next, SeekMode);
    eof when PrevTS == undefined -> 
      undefined;
    eof ->  
      {{audio_config, PrevOffset, PrevTS}, PrevTS}
  end.
  
% Reads a tag from IoDev for position Pos.
% @param IoDev
% @param Pos
% @return a valid video_frame record type

read_frame(#flv_media{audio_config = undefined} = MediaInfo, {audio_config, Pos, DTS}) ->
  read_frame(MediaInfo, {video_config, Pos, DTS});

read_frame(#flv_media{audio_config = Frame} = MediaInfo, {audio_config, Pos, DTS}) ->
  Next = case MediaInfo#flv_media.video_config of
    undefined -> 0;
    _ -> {video_config,Pos, DTS}
  end,
  Frame#video_frame{next_id = Next, dts = DTS, pts = DTS};


read_frame(#flv_media{video_config = undefined} = MediaInfo, {video_config, Pos, _DTS}) ->
  read_frame(MediaInfo, Pos);

read_frame(#flv_media{video_config = Frame}, {video_config,Pos, DTS}) ->
  Frame#video_frame{next_id = Pos, dts = DTS, pts = DTS};

read_frame(_, eof) ->
  eof;

read_frame(Media, undefined) ->
  read_frame(Media, first(Media));

read_frame(#flv_media{metadata_offset = Offset, reader = Reader} = Media, Offset) ->
  case flv:read_frame(Reader, Offset) of
    #video_frame{next_id = Next} -> read_frame(Media, Next);
    Else -> Else
  end;

read_frame(#flv_media{reader = Reader} = Media, Offset) ->
  normalize_audio_dts(Media, flv:read_frame(Reader, Offset)).

normalize_audio_dts(_Media, #video_frame{codec = mp3, body = Body, dts = DTS} = Frame) ->
  {ok, #mp3_frame{samples = Samples, sample_rate = SampleRate}, _} = mp3:read(Body),
  Count = (DTS*SampleRate)/(Samples*1000),
  PureDTS = round(Count)*Samples*1000 / SampleRate,
  % ?D({mp3,Count,DTS, PureDTS}),
  Frame#video_frame{dts = PureDTS, pts = PureDTS};

normalize_audio_dts(#flv_media{audio_config = #video_frame{body = Config}}, #video_frame{codec = aac, dts = DTS} = Frame) ->
  #aac_config{sample_rate = SampleRate, samples_per_frame = Samples} = aac:decode_config(Config),
  Count = (DTS*SampleRate)/(Samples*1000),
  PureDTS = round(Count)*Samples*1000 / SampleRate,
  % ?D({aac,Count,DTS, PureDTS}),
  Frame#video_frame{dts = PureDTS, pts = PureDTS};

normalize_audio_dts(_Media, Frame) ->
  % ?D({Frame#video_frame.codec, Frame#video_frame.dts}),
  Frame.

