#include <ei.h>
#include <libavcodec/avcodec.h>
#include <stdarg.h>
#include <unistd.h>
#define _GNU_SOURCE
#include <stdio.h>
#include "reader.h"
#include "compat.h"

int in_fd = 0;
int out_fd = 1;



void write_x(ei_x_buff *x) {
  uint32_t len = htonl(x->buffsz);
  write(out_fd, &len, 4);
  write(out_fd, x->buff, x->buffsz);
}

void reply_atom(char *a) {
  ei_x_buff x;
  ei_x_new_with_version(&x);
  ei_x_encode_atom(&x, a);
  write_x(&x);
  ei_x_free(&x);
}


void reply_avframe(AVPacket *pkt, AVCodecContext *ctx) {
  ei_x_buff x;
  ei_x_new_with_version(&x);
  struct video_frame r;

  r.content = ctx->codec_type == AVMEDIA_TYPE_VIDEO ? frame_content_video :
    ctx->codec_type == AVMEDIA_TYPE_AUDIO ? frame_content_audio : 0;

  r.dts = pkt->dts / 90;
  r.pts = pkt->pts / 90;
  r.stream_id = 0;
  r.codec = ctx->codec_id == AV_CODEC_ID_H264 ? frame_codec_h264 :
    ctx->codec_id == AV_CODEC_ID_AAC ? frame_codec_aac : ctx->codec_id == AV_CODEC_ID_MP3 ? frame_codec_mp3 : 0;

  r.flavor = pkt->flags & CODEC_FLAG_GLOBAL_HEADER ? frame_flavor_config :
    pkt->flags & AV_PKT_FLAG_KEY && ctx->codec_type == AVMEDIA_TYPE_VIDEO ? frame_flavor_keyframe :
    frame_flavor_frame;
  if (r.content == frame_content_audio) {
    r.channels = ctx->channels;
    r.size = av_get_bytes_per_sample(ctx->sample_fmt);
    switch(ctx->sample_rate) {
        case 5512:
            r.rate = frame_sound_rate_rate5; break;
        case 11025:
            r.rate = frame_sound_rate_rate11; break;
        case 22050:
            r.rate = frame_sound_rate_rate22; break;
        case 44100:
            r.rate = frame_sound_rate_rate44; break;
        default: r.rate = ctx->sample_rate;
    }
  }
  r.body.data = pkt->data;
  r.body.size = pkt->size;
  write_video_frame(&x, r);
  write_x(&x);
  ei_x_free(&x);
}

void error(const char *fmt, ...) {
  va_list ap;
  if(in_fd != 0) {
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    fprintf(stderr, "\r\n");
    va_end(ap);
  }

  char msg[256];
  va_start(ap, fmt);
  vsnprintf(msg, 256, fmt, ap);
  va_end(ap);


  ei_x_buff x;
  ei_x_new_with_version(&x);
  ei_x_format_wo_ver(&x, "{~a,~s}","error", msg);
  write_x(&x);
  ei_x_free(&x);
  _exit(15);
}


ssize_t read1(int fd, void *buf, ssize_t len) {
  ssize_t count = 0;
  ssize_t bytes = 0;
  while(len > 0 && (bytes = read(fd, buf, len)) > 0) {
    buf += bytes;
    len -= bytes;
    count += bytes;
  }
  if(bytes == 0 && len > 0) return 0;
  if(bytes == -1 && len > 0) return -1;
  return count;
}
