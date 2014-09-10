
-record(init_output, {
  content = undefined,
  codec = undefined,
  track_id = undefined,
  options = []
}).

-record(init_input, {
  content = undefined,
  codec = undefined,
  options = [],
  config = <<>> :: binary()
}).

-record(ffmpeg_worker, {
  ref,
  owner = undefined,
  worker_opt,
  port,
  audio_input = undefined,
  audio_output,
  video_input = undefined,
  video_output,
  audio_config = undefined,
  send_audio_config = false
}).



