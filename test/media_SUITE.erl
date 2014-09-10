-module(media_SUITE).
-include_lib("common_test/include/ct.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([convert_test1/1, convert_test2/1, convert_test3/1]).

all() -> [convert_test1, convert_test2, convert_test3].

init_per_suite(Config) ->
  media:start(),
  Config.

end_per_suite(_Config) ->
  application:stop(lager),
  media:stop().

convert_test1(Config) ->
  Dir = ?config(data_dir, Config),
  Options = #{filename => Dir ++ "/stream_1.flv", hls_writer => #{dir => Dir ++ "/hls/test1"}, audio => #{input => #{codec => speex, channels => 1, sample_rate => 16000}, output => #{codec => aac, channels => 2}}},
  media_convert:flv_to_hls(Options),
  receive
    {task_complete, Options} -> ok
  after 300000 ->
    throw(timeout)
  end.

convert_test2(Config) ->
  Dir = ?config(data_dir, Config),
  Options = #{audio => #{input => #{codec => speex, channels => 1, sample_rate => 16000}, output => #{codec => aac, channels => 2, sample_rate => 44100}}, filename => Dir ++ "/stream_3.flv", hls_writer => #{dir => Dir ++ "/hls/111", duration => 20000, audio => aac}, flv_writer => [{filename, Dir ++ "/111.flv"}]},
  media_server:get_converter(flv_reader, [hls_writer, flv_writer], Options),
  receive
    {task_complete, Options} -> ok
  after 300000 ->
    throw(timeout)
  end.

convert_test3(Config) ->
  Dir = ?config(data_dir, Config),
  Options = #{filename => Dir ++ "../no_file", hls_writer => #{dir => Dir ++ "../hls/no_file"}},
  media_server:get_converter(flv_reader, [hls_writer], Options),
  receive
    {task_failed, _Reason, Options} -> ok
  after 300000 ->
    throw(timeout)
  end.