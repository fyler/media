// @author     Max Lapshin <max@maxidoors.ru> [http://erlyvideo.org]
// @copyright  2010-2012 Max Lapshin
// @doc        multibitrate packetizer
// @reference  See <a href="http://erlyvideo.org" target="_top">http://erlyvideo.org</a> for more information
// @end
//
//

#include <ei.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include <libavcodec/avcodec.h>
#include <libswresample/swresample.h>
#include <libavutil/opt.h>
#include <libavutil/audio_fifo.h>
#include "reader.h"
#include "compat.h"
#include "audio_transcode.h"


typedef struct Track {
  AVCodec *codec;
  AVCodecContext *ctx;
  int track_id;
  enum AVMediaType content;
  enum AVCodecID codec_id;
  int width;
  int height;
} Track;

Track input_audio;
Track input_video;
Track output_audio;
SwrContext *resample_context = NULL;
AVAudioFifo *fifo = NULL;
int64_t audio_pts = 1e10;
Track output_video;

extern int out_fd;
extern int in_fd;

void loop();
void pong(void);
void debug_loop(int argc, char *argv[], void (*loop)(void));
void reply_atom(char *a);
void reply_avframe(AVPacket *pkt, AVCodecContext *ctx);
void error(const char *fmt, ...);
ssize_t read1(int fd, void *buf, ssize_t len);
FILE *logg;

int main(int argc, char *argv[]) 
{
  avcodec_register_all();
  av_log_set_level(AV_LOG_ERROR);

  bzero(&input_audio, sizeof(Track));
  bzero(&input_video, sizeof(Track));
  bzero(&output_audio, sizeof(Track));
  bzero(&output_video, sizeof(Track));
  
  loop();
  
  return 0;
}

void flush() {
     AVPacket output_packet;
     init_packet(&output_packet);
     int got_packet;
     int nb_samples;
     const int output_frame_size = output_audio.ctx->frame_size;
     int ret = 0;
     while ((ret = convert_and_store(NULL, fifo, input_audio.ctx, output_audio.ctx, resample_context)) > 0);
     if (ret < 0) {
        error("failed to flush swr buffer");
     }
     while (av_audio_fifo_size(fifo) >= output_frame_size) {
         init_packet(&output_packet);
         if (load_and_encode(fifo, output_audio.ctx, &output_packet, &got_packet, &nb_samples) < 0)
                        error("failed to encode audio");
         if(got_packet) {
            output_packet.dts = output_packet.pts = audio_pts;
            audio_pts += output_packet.duration;
            reply_avframe(&output_packet, output_audio.ctx);
         }
         av_free_packet(&output_packet);
     }
     do {
        init_packet(&output_packet);
        if(encode_audio_frame(NULL, output_audio.ctx, &output_packet, &got_packet, &nb_samples))
            error("failed to encode audio");
        if(got_packet) {
            output_packet.dts = output_packet.pts = audio_pts;
            audio_pts += output_packet.duration;
            reply_avframe(&output_packet, output_audio.ctx);
        }
        av_free_packet(&output_packet);
     } while (got_packet);
}

void clean() {
    if (fifo) {
        av_audio_fifo_free(fifo);
    }
    if (resample_context) {
        swr_free(&resample_context);
    }
    if (output_audio.ctx) {
        avcodec_free_context(&output_audio.ctx);
        av_free(output_audio.ctx);
        output_audio.codec = NULL;
        audio_pts = 1e10;
    }
    if (input_audio.ctx) {
        avcodec_free_context(&input_audio.ctx);
        av_free(input_audio.ctx);
        input_audio.codec = NULL;
    }
}

void reset() {
    if (fifo) {
        av_audio_fifo_reset(fifo);
    }
    if (output_audio.ctx) {
        avcodec_flush_buffers(output_audio.ctx);
    }
    if (input_audio.ctx) {
        audio_pts = 1e10;
        avcodec_flush_buffers(input_audio.ctx);
    }
}

void loop() {
  int64_t dts_shift = AV_NOPTS_VALUE;

  uint32_t buf_size = 10240;
  char *buf = (char *)malloc(buf_size);
  while(1) {
    uint32_t len;
    int idx = 0;
    int read_bytes = 0;
    if((read_bytes = read1(in_fd, &len, 4)) != 4) {
      if(read_bytes == 0) {
        _exit(0);
      }
      error("Can't read input length: %d", read_bytes);
    }
    len = ntohl(len);
    if(len > buf_size) {
      buf_size = len;
      free(buf);
      buf = (char *)malloc(buf_size);
    }

    if((read_bytes = read1(in_fd, buf, len)) != len) error("Can't read %d bytes from input: %d", len, read_bytes);
    int version = 0;
    ei_decode_version(buf, &idx, &version);
    int command_idx = idx;

    int arity = 0;
    if(ei_decode_tuple_header(buf, &idx, &arity) == -1) error("must pass tuple");


    int t = 0;
    int size = 0;
    ei_get_type(buf, &idx, &t, &size);
    if(t != ERL_ATOM_EXT) error("first element must be atom");
    char command[MAXATOMLEN+1];
    ei_decode_atom(buf, &idx, command); arity--;


    if(!strcmp(command, "ping")) {
      pong();
      continue;
    }
    if(!strcmp(command, "exit")) {
      return;
    }
    if (!strcmp(command, "flush")) {
        if(output_audio.ctx) {
            flush();
        }
        reply_atom("flush");
        continue;
    }
    if (!strcmp(command, "finish")) {
        if(output_audio.ctx) {
             flush();
        }
        reset();
        reply_atom("finish");
        continue;
    }
    if (!strcmp(command, "clean")) {
        clean();
        continue;
    }
    if(!strcmp(command, "init_input")) {
      if(arity != 4) error("Must provide 3 arguments to init_input command");
      char content[1024];
      char codec[1024];
      if(ei_decode_atom(buf, &idx, content) == -1) error("Must provide content as an atom");
      if(ei_decode_atom(buf, &idx, codec) == -1) error("Must provide codec as an atom");

      Track *tr = NULL;
      if(!strcmp(content, "video")) {
        tr = &input_video;
      } else if(!strcmp(content, "audio")) {
        tr = &input_audio;
      } else {
        error("Unknown media content: '%s'", content);
      }
      if(tr->codec) error("Double initialization of media '%s'", content);
      tr->codec = avcodec_find_decoder_by_name(codec);
      tr->ctx = avcodec_alloc_context3(tr->codec);
      if(!tr->codec || !tr->ctx)
        error("Unknown %s decoder '%s'", content, codec);

      int options_count = 0;
      if(ei_decode_list_header(buf, &idx, &options_count) < 0) error("options must be a proplist");
      while(options_count > 0) {
        int arity1 = 0;

        int q,s;
        ei_get_type(buf, &idx, &q, &s);
        if(q == ERL_NIL_EXT) {
            ei_skip_term(buf, &idx);
            break;
        }

        if(ei_decode_tuple_header(buf, &idx, &arity1) < 0) error("options must be a proper proplist");
        if(arity1 != 2) error("tuples in options proplist must be arity 2");

        char key[MAXATOMLEN];
        if(ei_decode_atom(buf, &idx, key) == 0) {
            if(!strcmp(key, "sample_rate")) {
                long sr = 0;
                if(ei_decode_long(buf, &idx, &sr) < 0) error("sample_rate must be integer");
                tr->ctx->sample_rate = sr;
                continue;
            }

            if(!strcmp(key, "channels")) {
                long ch = 0;
                if(ei_decode_long(buf, &idx, &ch) < 0) error("channels must be integer");
                tr->ctx->channels = ch;
                tr->ctx->channel_layout = av_get_default_channel_layout(ch);
                continue;
            }

            fprintf(stderr, "Unknown key: '%s'\r\n", key);
            ei_skip_term(buf, &idx);
            continue;
        } else {
                error("Invalid options proplist");
        }
      }
      tr->ctx->time_base = (AVRational){1, 90000};

      int decoder_config_len = 0;
      ei_get_type(buf, &idx, &t, &decoder_config_len);
      if(t != ERL_BINARY_EXT) error("decoder config must be a binary");
      uint8_t *decoder_config = av_mallocz(decoder_config_len + FF_INPUT_BUFFER_PADDING_SIZE);
      long bin_len = 0;
      ei_decode_binary(buf, &idx, decoder_config, &bin_len);

      tr->ctx->extradata_size = decoder_config_len;
      tr->ctx->extradata = decoder_config;
      if(avcodec_open2(tr->ctx, tr->codec, NULL) < 0)
        error("failed to allocate %s decoder", content);
      if(tr->ctx->sample_fmt == AV_SAMPLE_FMT_NONE)
        tr->ctx->sample_fmt = AV_SAMPLE_FMT_S16;
      continue;
    }

    if(!strcmp(command, "init_output")) {
      if(arity != 4) error("Must provide 4 arguments to init_output command");
      char content[1024];
      char codec[1024];
      if(ei_decode_atom(buf, &idx, content) == -1) error("Must provide content as an atom");
      if(ei_decode_atom(buf, &idx, codec) == -1) error("Must provide codec as an atom");

      long track_id = -1;
      if(ei_decode_long(buf, &idx, &track_id) == -1) error("track_id must be integer");
      if(track_id < 1) error("track_id must be more then 1");
      track_id--;

      Track *t = NULL;
      if(!strcmp(content, "audio")) {
        t = &output_audio;
      } else if(!strcmp(content, "video")) {
        t = &output_video;
      } else {
        error("invalid_content '%s'", content);
      }
      t->track_id = track_id;

      t->codec = avcodec_find_encoder_by_name(codec);

      t->ctx = avcodec_alloc_context3(t->codec);
      if(!t->codec || !t->ctx) error("Unknown encoder '%s'", codec);

      AVCodecContext* ctx = t->ctx;
      AVDictionary *opts = NULL;


      int options_count = 0;
      if(ei_decode_list_header(buf, &idx, &options_count) < 0) error("options must be a proplist");
      while(options_count > 0) {
        int arity1 = 0;

        int t,s;
        ei_get_type(buf, &idx, &t, &s);
        if(t == ERL_NIL_EXT) {
          ei_skip_term(buf, &idx);
          break;
        }

        if(ei_decode_tuple_header(buf, &idx, &arity1) < 0) error("options must be a proper proplist");
        if(arity1 != 2) error("tuples in options proplist must be arity 2");

        char key[MAXATOMLEN];
        if(ei_decode_atom(buf, &idx, key) == 0) {

          if(!strcmp(key, "width")) {
            long w = 0;
            if(ei_decode_long(buf, &idx, &w) < 0) error("width must be integer");
            ctx->width = w;
            continue;
          }

          if(!strcmp(key, "height")) {
            long h = 0;
            if(ei_decode_long(buf, &idx, &h) < 0) error("height must be integer");
            ctx->height = h;
            continue;
          }

          if(!strcmp(key, "bitrate")) {
            long b = 0;
            if(ei_decode_long(buf, &idx, &b) < 0) error("bitrate must be integer");
            ctx->bit_rate = b;
            continue;
          }

          if(!strcmp(key, "sample_rate")) {
            long sr = 0;
            if(ei_decode_long(buf, &idx, &sr) < 0) error("sample_rate must be integer");
            ctx->sample_rate = sr;
            continue;
          }

          if(!strcmp(key, "channels")) {
            long ch = 0;
            if(ei_decode_long(buf, &idx, &ch) < 0) error("channels must be integer");
            ctx->channels = ch;
            ctx->channel_layout = av_get_default_channel_layout(ch);
            continue;
          }

          fprintf(stderr, "Unknown key: '%s'\r\n", key);
          ei_skip_term(buf, &idx);
          continue;
        } else if(ei_decode_string(buf, &idx, key) == 0) {
          char value[MAXATOMLEN];
          if(ei_decode_string(buf, &idx, value) < 0) error("key-value must be strings");
          av_dict_set(&opts, key, value, 0);
        } else {
          error("Invalid options proplist");
        }
      }

      if(!strcmp(content, "video")) {
        ctx->pix_fmt = AV_PIX_FMT_YUV420P;
      }
      if(!strcmp(content, "audio")) {
        ctx->sample_fmt = t->codec->sample_fmts[0];
        ctx->profile = FF_PROFILE_AAC_LOW;
        if (init_resampler(input_audio.ctx, ctx, &resample_context))
            error("failed to init resampler");
        if (init_fifo(&fifo, ctx))
            error("failed to init fifo");
      }
      ctx->flags |= CODEC_FLAG_GLOBAL_HEADER;
      ctx->time_base = (AVRational){1,90000};
      if(avcodec_open2(ctx, t->codec, &opts) < 0) error("failed to allocate video encoder");

      AVPacket config;
      config.dts = config.pts = 0;
      config.flags = CODEC_FLAG_GLOBAL_HEADER;
      config.data = ctx->extradata;
      config.size = ctx->extradata_size;
      reply_avframe(&config, ctx);
      continue;
    }

    if(!strcmp(command, "video_frame")) {
      idx = command_idx;
      struct video_frame *fr = read_video_frame(buf, &idx);
      AVPacket packet;
      av_new_packet(&packet, fr->body.size);
      memcpy(packet.data, fr->body.data, fr->body.size);
      packet.size = fr->body.size;
      packet.dts = fr->dts*90;
      packet.pts = fr->pts*90;
      if(fr->content == frame_content_audio) {

        /** Define the first pts. */
        audio_pts = FFMIN(packet.pts, audio_pts);

        const int output_frame_size = output_audio.ctx->frame_size;
        int finished = 0;

        while (packet.size > 0)
        {
            if (decode_convert_and_store(fifo, &packet, input_audio.ctx, output_audio.ctx, resample_context, &finished) < 0)
                error("failed to decode audio");
        }
        av_free_packet(&packet);

        /** Packet used for temporary storage. */
        AVPacket output_packet;


        int got_packet;
        int nb_samples;

        while (av_audio_fifo_size(fifo) >= output_frame_size) {
            init_packet(&output_packet);
            if (load_and_encode(fifo, output_audio.ctx, &output_packet, &got_packet, &nb_samples) < 0)
                error("failed to encode audio");
            if(got_packet) {
                output_packet.dts = output_packet.pts = audio_pts;
                audio_pts += output_packet.duration;
                reply_avframe(&output_packet, output_audio.ctx);
            }
            av_free_packet(&output_packet);
        }
        free(fr);
        continue;
      }

      if(fr->content == frame_content_video) {
        if(!input_video.ctx) error("input video uninitialized");
        AVFrame *decoded_frame = av_frame_alloc();
        int could_decode = 0;
        int ret = avcodec_decode_video2(input_video.ctx, decoded_frame, &could_decode, &packet);
        if(ret < 0) {
          error("failed to decode video");
        }
        if(could_decode) {
          decoded_frame->pts = av_frame_get_best_effort_timestamp(decoded_frame);
          int sent_config = 0;

          AVPacket pkt;
          av_init_packet(&pkt);
          pkt.data = NULL;
          pkt.size = 0;

          int could_encode = 0;

          if(avcodec_encode_video2(output_video.ctx, &pkt, decoded_frame, &could_encode) != 0) 
            error("Failed to encode h264");

          if(could_encode) {
            if(dts_shift == AV_NOPTS_VALUE) {
              dts_shift = -pkt.dts;
            }
            pkt.dts += dts_shift;
            reply_avframe(&pkt, output_video.ctx);
          } else if(!sent_config) {
            reply_atom("ok");
          }
          free(fr);
          continue;
        } else {
          reply_atom("ok");
          free(fr);
          continue;
        }
      }

      error("Unknown content");
    }

    // AVCodecContext
    // AVPacket
    // AVFrame



    char *s = (char *)malloc(1024);
    ei_s_print_term(&s, buf, &command_idx);
    error("Unknown command: %s", s);
  }
}


void pong(void) {
  reply_atom("pong");
}

