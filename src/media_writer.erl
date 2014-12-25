%%%-------------------------------------------------------------------
%%% @author ilia
%%% @copyright (C) 2014, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 01. июл 2014 16:52
%%%-------------------------------------------------------------------
-module(media_writer).
-author("ilia").

-behaviour(gen_server).
-include("log.hrl").
-include_lib("erlmedia/include/video_frame.hrl").
%% API
-export([start_link/3]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-record(state, {reader,
                reader_state,
                output_modules,
                states,
                ffmpeg,
                ffmpeg_content,
                audio_buffer = queue:new(),
                video_buffer = queue:new(),
                buffer_size,
                frame_id = undefined}).

%%%===================================================================
%%% API
%%%===================================================================

-spec(start_link(Input :: atom(), Output :: [atom()], Options :: map()) ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Input, Output, Options) ->
  gen_server:start_link(?MODULE, [Input, Output, Options], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Reader, Output, Options]) ->
  self() ! {start, Options},
  {ok, #state{reader = Reader, output_modules = Output}}.

-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
  {reply, Reply :: term(), NewState :: #state{}} |
  {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

-spec(handle_cast(Request :: term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).

handle_cast(#video_frame{next_id = Id} = Frame, #state{output_modules = Output, states = States, ffmpeg = undefined} = State) ->
  {noreply, State#state{states = write_frame(Frame, Output, States), frame_id = Id}};

handle_cast(#video_frame{content = audio, next_id = Id} = Frame, #state{ffmpeg = FFmpeg, ffmpeg_content = #{audio := true}} = State) ->
  ffmpeg_worker:transcode(FFmpeg, Frame),
  {noreply, State#state{frame_id = Id}};

handle_cast(#video_frame{content = video, next_id = Id} = Frame, #state{ffmpeg = FFmpeg, ffmpeg_content = #{video := true}} = State) ->
  ffmpeg_worker:transcode(FFmpeg, Frame),
  {noreply, State#state{frame_id = Id}};

handle_cast(#video_frame{next_id = Id} = Frame, #state{} = State) ->
  handle_cast({ffmpeg, Frame}, State#state{frame_id = Id});

handle_cast({ffmpeg, #video_frame{content = audio} = Frame}, #state{output_modules = Output, states = States, audio_buffer = Buffer, video_buffer = VideoBuffer} = State) ->
  AudioBuffer = queue:in(Frame, Buffer),
  case queue:is_empty(VideoBuffer) of
    true ->
      {noreply, State#state{audio_buffer = AudioBuffer}};
    false ->
      #video_frame{dts = Dts1} = AFrame = queue:get(AudioBuffer),
      #video_frame{dts = Dts2} = VFrame = queue:get(VideoBuffer),
      if
        Dts1 < Dts2 ->
          {noreply, State#state{states = write_frame(AFrame, Output, States), audio_buffer = queue:drop(AudioBuffer)}};
        true ->
          {noreply, State#state{states = write_frame(VFrame, Output, States), audio_buffer = AudioBuffer, video_buffer = queue:drop(VideoBuffer)}}
      end
  end;

handle_cast({ffmpeg, #video_frame{content = video} = Frame}, #state{output_modules = Output, states = States, audio_buffer = AudioBuffer, video_buffer = Buffer} = State) ->
  VideoBuffer = queue:in(Frame, Buffer),
  case queue:is_empty(AudioBuffer) of
    true ->
      {noreply, State#state{video_buffer = VideoBuffer}};
    false ->
      #video_frame{dts = Dts1} = VFrame = queue:get(VideoBuffer),
      #video_frame{dts = Dts2} = AFrame = queue:get(AudioBuffer),
      if
        Dts1 =< Dts2 ->
          {noreply, State#state{states = write_frame(VFrame, Output, States), video_buffer = queue:drop(VideoBuffer)}};
        true ->
          {noreply, State#state{states = write_frame(AFrame, Output, States), video_buffer = VideoBuffer, audio_buffer = queue:drop(AudioBuffer)}}
      end
  end;

handle_cast({ffmpeg, flush}, #state{reader = live} = State) ->
  {noreply, State}; 

handle_cast({ffmpeg, flush}, #state{reader = Reader, reader_state = Media, buffer_size = N, frame_id = Id} = State) ->
  Self = self(),
  spawn_link(fun() -> read_frames(Reader, Media, Id, N, Self) end),
  {noreply, State};

handle_cast({ffmpeg, finish}, #state{output_modules = Modules, states = States, audio_buffer =  AudioBuffer, video_buffer = VideoBuffer} = State) ->
  FramesMerge = fun(#video_frame{dts = Dts1}, #video_frame{dts = Dts2}) when Dts1 > Dts2 -> true;
    (#video_frame{dts = Dts1, content = video}, #video_frame{dts = Dts1}) -> true;
    (#video_frame{}, #video_frame{}) -> false
  end,
  Frames = lists:merge(FramesMerge, lists:reverse(queue:to_list(VideoBuffer)), lists:reverse(queue:to_list(AudioBuffer))),
  NewStates = write_frame(Frames, Modules, States),
  {stop, normal, State#state{states = write_frame(eof, Modules, NewStates)}};

handle_cast({reader, ok}, #state{reader = live, ffmpeg = undefined} = State) ->
  {noreply, State};

handle_cast({reader, ok}, #state{reader = Reader, reader_state = Media, ffmpeg = undefined, buffer_size = N, frame_id = Id} = State) ->
  Self = self(),
  spawn_link(fun() -> read_frames(Reader, Media, Id, N, Self) end),
  {noreply, State};

handle_cast({reader, ok}, #state{ffmpeg = FFmpeg} = State) ->
  ffmpeg_worker:flush(FFmpeg),
  {noreply, State};

handle_cast({reader, eof}, #state{output_modules = Modules, states = States, ffmpeg = undefined} = State) ->
  {stop, normal, State#state{states = write_frame(eof, Modules, States)}};

handle_cast({reader, eof}, #state{ffmpeg = FFmpeg} = State) ->
  ffmpeg_worker:finish(FFmpeg),
  {noreply, State};

handle_cast(_Request, State) ->
  {noreply, State}.

-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_info({start, Options}, #state{reader = undefined, output_modules = Output} = State) ->
  States = init_state(Output, Options),
  {Content, Pid} = is_need_ffmpeg(Options),
  {noreply, State#state{states = States, ffmpeg = Pid, ffmpeg_content = Content}};

handle_info({start, Options}, #state{reader = live, output_modules = Output} = State) ->
  States = init_state(Output, Options),
  {Content, Pid} = is_need_ffmpeg(Options),
  {noreply, State#state{states = States, ffmpeg = Pid, ffmpeg_content = Content}};

handle_info({start, #{filename := FileName} = Options}, #state{reader = Reader, output_modules = Output} = State) ->
  States = init_state(Output, Options),
  {ok, File} = file:open(FileName, [read, binary]),
  {ok, Media} = Reader:init({file, File}, [{find_metadata, false}]),
  N = ulitos_app:get_var(media, reader_buffer, 50),
  Self = self(),
  spawn_link(fun() -> read_frames(Reader, Media, undefined, N, Self) end),
  {Content, Pid} = is_need_ffmpeg(Options),
  {noreply, State#state{reader_state = Media, states = States, ffmpeg = Pid, ffmpeg_content = Content, buffer_size = N}};

handle_info(_Info, State) ->
  {noreply, State}.

-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, _State) ->
  ok.

-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
  {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
init_state(Output, Options) ->
  init_state(Output, Options, []).

init_state([Module | Output], Options, Acc) ->
  {ok, State} = case maps:get(Module, Options, []) of
            [] ->
              Module:init_file();
            Opt ->
              case get_filename(Opt) of
                undefined ->
                  Module:init_file(Opt);
                FileName ->
                  Module:init_file(FileName, Opt)
              end
          end,
  init_state(Output, Options, [State | Acc]);

init_state([], _Options, Acc) ->
  lists:reverse(Acc).

read_frames(_Reader, _Media, _Id, 0, Owner) ->
  gen_server:cast(Owner, {reader, ok});

read_frames(Reader, Media, Id, N, Owner) ->
  case Reader:read_frame(Media, Id) of
    #video_frame{next_id = Next} = F ->
      gen_server:cast(Owner, F),
      read_frames(Reader, Media, Next, N - 1, Owner);
    eof ->
      gen_server:cast(Owner, {reader, eof})
  end.

is_need_ffmpeg(#{audio := #{output := _} = Audio, video := #{output := _} = Video}) ->
  {ok, Pid} = ffmpeg_server:start_link_worker(#{audio => Audio, video => Video}),
  {#{audio => true, video => true}, Pid};

is_need_ffmpeg(#{audio := #{output := _} = Audio}) ->
  {ok, Pid} = ffmpeg_server:start_link_worker(#{audio => Audio}),
  {#{audio => true}, Pid};

is_need_ffmpeg(#{video := #{output := _} = Video}) ->
  {ok, Pid} = ffmpeg_server:start_link_worker(#{video => Video}),
  {#{video => true}, Pid};

is_need_ffmpeg(#{}) ->
  {#{}, undefined}.

write_frame(Frames, Modules, States) when is_list(Frames) ->
  lists:foldr(fun(Frame, StatesAcc) -> write_frame(Frame, Modules, StatesAcc) end, States, Frames);

write_frame(Frame, Modules, States) ->
  write_frame(Frame, Modules, States, []).

write_frame(Frame, [Module | Modules], [State|States], Acc) ->
  {ok, NewState} = Module:write_frame(Frame, State),
  write_frame(Frame, Modules, States, [NewState | Acc]);

write_frame(_Frame, [], [], Acc) ->
  lists:reverse(Acc).

get_filename(Options) when is_map(Options) ->
  maps:get(filename, Options, undefined);

get_filename(Options) when is_list(Options) ->
  proplists:get_value(filename, Options, undefined).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

init_file_test() ->
  meck:new(aaa, [non_strict]),
  meck:new(bbb, [non_strict]),
  meck:new(ccc, [non_strict]),
  meck:expect(aaa, init_file, fun() -> {ok, a} end),
  meck:expect(bbb, init_file, fun(#{1 := B}) -> {ok, B} end),
  meck:expect(ccc, init_file, fun([{2, C}]) -> {ok, C} end),
  ?assertMatch([_, _, _], init_state([aaa, bbb, ccc], #{bbb => #{1 => b}, ccc => [{2, c}]})),
  meck:unload(aaa),
  meck:unload(bbb),
  meck:unload(ccc).

write_frame_test() ->
  meck:new(aA, [non_strict]),
  meck:new(bB, [non_strict]),
  meck:expect(aA, write_frame, fun(A, State) -> {ok, [A | State]} end),
  meck:expect(bB, write_frame, fun(A, State) -> {ok, [A + 1 | State]} end),
  ?assertEqual([[4, 3, 2, 1], [5, 4, 3, 2]], write_frame([4, 3, 2, 1], [aA, bB], [[], []])),
  meck:unload(aA),
  meck:unload(bB).

-endif.



