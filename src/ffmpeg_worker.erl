-module(ffmpeg_worker).

-include_lib("erlmedia/include/video_frame.hrl").
-include("ffmpeg_worker.hrl").
-include("log.hrl").

-behaviour(gen_server).

%% API
-export([start_link/1, set_options/3, transcode/2, finish/1, flush/1]).

%% gen_server
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

%% API
start_link(Ref) ->
  gen_server:start_link(?MODULE, Ref, []).

start_worker() ->
  start_worker(filename:join(code:lib_dir(media, priv), "erffmpeg")).

start_worker(Path) ->
  Port = erlang:open_port({spawn_executable, Path}, [{packet,4}, {arg0, "erffmpeg"}, binary,exit_status]),
  {program, Port}.

send({program, Port}, Term) ->
  erlang:port_command(Port, erlang:term_to_binary(Term)).

send_frame(Port, #video_frame{content = audio} = Frame) ->
  send(Port, Frame).

%% gen_server callbacks

init(Ref) ->
  self() ! start,
  {ok, #ffmpeg_worker{ref = Ref}}.

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast({set_options, Owner, WorkerOpt}, #ffmpeg_worker{worker_opt = WorkerOpt} = State) ->
  {noreply, State#ffmpeg_worker{owner = Owner}};

handle_cast({set_options, Owner, #{audio := #{input := InputAudio, output := OutputAudio}, video := #{input := InputVideo, output := OutputVideo}} = WorkerOpt}, #ffmpeg_worker{ref = Ref, port = Port} = State) ->
  send(Port, {clean}),
  gen_server:call(ffmpeg_server, {worker, init, Ref, WorkerOpt}),
  AudioInput = audio_input(InputAudio),
  AudioOutput = audio_output(OutputAudio),
  VideoInput = video_input(InputVideo),
  VideoOutput = video_output(OutputVideo),
  send(Port, AudioInput),
  send(Port, VideoInput),
  send(Port, AudioOutput),
  send(Port, VideoOutput),
  {noreply, State#ffmpeg_worker{owner = Owner, audio_input = AudioInput, audio_output = AudioOutput, video_input = VideoInput, video_output = VideoOutput, worker_opt = WorkerOpt, audio_config = undefined}};

handle_cast({set_options, Owner, #{audio := #{input := InputAudio, output := OutputAudio}} = WorkerOpt}, #ffmpeg_worker{ref = Ref, port = Port} = State) ->
  send(Port, {clean}),
  gen_server:call(ffmpeg_server, {worker, init, Ref, WorkerOpt}),
  AudioInput = audio_input(InputAudio),
  AudioOutput = audio_output(OutputAudio),
  send(Port, AudioInput),
  send(Port, AudioOutput),
  {noreply, State#ffmpeg_worker{owner = Owner, audio_input = AudioInput, audio_output = AudioOutput, worker_opt = WorkerOpt, audio_config = undefined}};

handle_cast({set_options, Owner, #{video := #{input := InputVideo, output := OutputVideo}} = WorkerOpt}, #ffmpeg_worker{ref = Ref, port = Port} = State) ->
  send(Port, {clean}),
  gen_server:call(ffmpeg_server, {worker, init, Ref, WorkerOpt}),
  VideoInput = video_input(InputVideo),
  VideoOutput = video_output(OutputVideo),
  send(Port, VideoInput),
  send(Port, VideoOutput),
  {noreply, State#ffmpeg_worker{owner = Owner, video_input = VideoInput, video_output = VideoOutput, worker_opt = WorkerOpt}};

handle_cast({transcode, #video_frame{content = audio} = Frame}, #ffmpeg_worker{port = Port, send_audio_config = false, audio_config = undefined} = State) ->
  send_frame(Port, Frame),
  {noreply, State};

handle_cast({transcode, #video_frame{content = audio} = Frame}, #ffmpeg_worker{port = Port, owner = Owner, send_audio_config = false, audio_config = Config} = State) ->
  response_to_owner({ffmpeg, Config}, Owner),
  send_frame(Port, Frame),
  {noreply, State#ffmpeg_worker{send_audio_config = true}};

handle_cast({transcode, #video_frame{} = Frame}, #ffmpeg_worker{port = Port} = State) ->
  send_frame(Port, Frame),
  {noreply, State};

handle_cast(finish, #ffmpeg_worker{port = Port} = State) ->
  send(Port, {finish}),
  {noreply, State};

handle_cast(flush, #ffmpeg_worker{port = Port} = State) ->
  send(Port, {flush}),
  {noreply, State};

handle_cast(_Request, State) ->
  {noreply, State}.

handle_info(start, #ffmpeg_worker{ref = Ref} = State) ->
  Port = start_worker(),
  gen_server:call(ffmpeg_server, {worker, start, Ref}),
  {noreply, State#ffmpeg_worker{port = Port}};

handle_info({Port, Data}, #ffmpeg_worker{ref = Ref, port = {program, Port}, owner = Owner} = State) ->
  case handle_data(Data) of
    #video_frame{content = audio, flavor = config} = Frame ->
      {noreply, State#ffmpeg_worker{audio_config = Frame}};
    #video_frame{content = audio} = Frame ->
      response_to_owner({ffmpeg, Frame}, Owner),
      {noreply, State};
    #video_frame{content = video} = Frame ->
      response_to_owner({ffmpeg, Frame}, Owner),
      {noreply, State};
    flush ->
      response_to_owner({ffmpeg, flush}, Owner),
      {noreply, State};
    finish ->
      response_to_owner({ffmpeg, finish}, Owner),
      unlink(Owner),
      gen_server:call(ffmpeg_server, {worker, finish, Ref}),
      {noreply, State#ffmpeg_worker{owner = undefined, send_audio_config = false}};
    Else ->
      ?D(Else),
      {stop, ffmpeg_error, State}
  end;

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

set_options(Pid, Owner, WorkerOpt) ->
  gen_server:cast(Pid, {set_options, Owner, WorkerOpt}).

transcode(Pid, Frame) ->
  gen_server:cast(Pid, {transcode, Frame}).

flush(Pid) ->
  gen_server:cast(Pid, flush).

finish(Pid) ->
  gen_server:cast(Pid, finish).

response_to_owner(Term, Owner) ->
  gen_server:cast(Owner, Term).

handle_data({exit_status, 0}) ->
  closed;

handle_data({exit_status, Code}) ->
  {exit, Code};

handle_data({data, Data}) ->
  erlang:binary_to_term(Data);

handle_data(Data) ->
  {ok, Data}.

audio_input(#{codec := Codec, sample_rate := SampleRate, channels := Channels, config := Config}) ->
  #init_input{content = audio, codec = ev_to_av(Codec), options = [{sample_rate, ev_to_av(SampleRate)}, {channels, ev_to_av(Channels)}], config = Config};

audio_input(#{codec := Codec, sample_rate := SampleRate, channels := Channels}) ->
  #init_input{content = audio, codec = ev_to_av(Codec), options = [{sample_rate, ev_to_av(SampleRate)}, {channels, ev_to_av(Channels)}]}.

audio_output(#{codec := Codec, sample_rate := SampleRate, channels := Channels}) ->
  #init_output{content = audio, codec = ev_to_av(Codec), track_id = 1, options = [{sample_rate, ev_to_av(SampleRate)}, {channels, ev_to_av(Channels)}]}.

video_input(#{codec := Codec, config := Config}) ->
  #init_input{content = video, codec = ev_to_av(Codec), config = Config}.

video_output(#{codec := Codec}) ->
  #init_input{content = audio, codec = ev_to_av(Codec)}.


ev_to_av(h264) -> libx264;
ev_to_av(aac) -> libfdk_aac;
ev_to_av(speex) -> libspeex;
ev_to_av(mp3) -> libmp3lame;
ev_to_av(rate5) -> 5512;
ev_to_av(rate11) -> 11025;
ev_to_av(rate22) -> 22050;
ev_to_av(rate44) -> 44100;
ev_to_av(bit8) -> 8000;
ev_to_av(bit16) -> 16000;
ev_to_av(mono) -> 1;
ev_to_av(stereo) -> 2;
ev_to_av(Smth) -> Smth.