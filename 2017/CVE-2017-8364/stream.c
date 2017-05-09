/* 
   Copyright (C) Andrew Tridgell 1998
   
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2 of the License, or
   (at your option) any later version.
   
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.
   
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
*/
/* multiplex N streams into a file - the streams are passed
   through bzlib */

#include "rzip.h"
#include "bzlib.h"

#define CTYPE_NONE 3
#define CTYPE_BZIP2 4

typedef uint16 u16;
typedef uint32 u32;

struct stream {
	u32 last_head;
	uchar *buf;
	int buflen;
	int bufp;
	int bzip_level;
};

struct stream_info {
	struct stream *s;
	int num_streams;
	int fd;
	u32 bufsize;
	u32 cur_pos;
	off_t initial_pos;
	u32 total_read;
};

/*
  try to compress a buffer. If compression fails for whatever reason then
  leave uncompressed. Return the compression type in c_type and resulting
  length in c_len
*/
static void compress_buf(struct stream *s, int *c_type, u32 *c_len)
{
	uchar *c_buf;
	unsigned int dlen = s->buflen-1;

	if (s->bzip_level == 0) return;

	c_buf = malloc(dlen);
	if (!c_buf) return;

	if (BZ2_bzBuffToBuffCompress((char*)c_buf, &dlen, (char*)s->buf, s->buflen, 
				     s->bzip_level, 0, s->bzip_level*10) != 
	    BZ_OK) {
		free(c_buf);
		return;
	}

	*c_len = dlen;
	free(s->buf);
	s->buf = c_buf;
	*c_type = CTYPE_BZIP2;
}

/*
  try to decompress a buffer. Return 0 on success and -1 on failure.
*/
static int decompress_buf(struct stream *s, u32 c_len, int c_type)
{
	uchar *c_buf;
	unsigned int dlen = s->buflen;
	int bzerr;

	if (c_type == CTYPE_NONE) return 0;

	c_buf = s->buf;
	s->buf = malloc(dlen);
	if (!s->buf) {
		err_msg("Failed to allocate %d bytes for decompression\n", dlen);
		return -1;
	}

	bzerr = BZ2_bzBuffToBuffDecompress((char*)s->buf, &dlen, (char*)c_buf, c_len, 0, 0);
	if (bzerr != BZ_OK) {
		err_msg("Failed to decompress buffer - bzerr=%d\n", bzerr);
		return -1;
	}

	if (dlen != s->buflen) {
		err_msg("Inconsistent length after decompression. Got %d bytes, expected %d\n", dlen, s->buflen);
		return -1;
	}

	free(c_buf);
	return 0;
}

/* write to a file, return 0 on success and -1 on failure */
static int write_buf(int f, uchar *p, int len)
{
	int ret;
	ret = write(f, p, len);
	if (ret == -1) {
		err_msg("Write of length %d failed - %s\n", len, strerror(errno));
		return -1;
	}
	if (ret != len) {
		err_msg("Partial write!? asked for %d bytes but got %d\n", len, ret);
		return -1;
	}
	return 0;
}

/* write a byte */
static int write_u8(int f, uchar v)
{
	return write_buf(f, &v, 1);
}

/* write a short */
static int write_u16(int f, u16 v)
{
	uchar p[2];
	p[0] = v&0xFF;
	p[1] = (v>>8)&0xFF;
	return write_buf(f, p, 2);
}

/* write a long */
static int write_u32(int f, u32 v)
{
	if (write_u16(f, v&0xFFFF) != 0 ||
	    write_u16(f, (v>>16)) != 0) {
		return -1;
	}
	return 0;
}

static int read_buf(int f, uchar *p, int len)
{
	int ret;
	ret = read(f, p, len);
	if (ret == -1) {
		err_msg("Read of length %d failed - %s\n", len, strerror(errno));
		return -1;
	}
	if (ret != len) {
		err_msg("Partial read!? asked for %d bytes but got %d\n", len, ret);
		return -1;
	}
	return 0;
}

static int read_u8(int f, uchar *v)
{
	return read_buf(f, v, 1);
}

static int read_u16(int f, u16 *v)
{
	uchar p[2];
	if (read_buf(f, p, 2) != 0) {
		return -1;
	}
	*v = (p[1]<<8) | p[0];
	return 0;
}

static int read_u32(int f, u32 *v)
{
	u16 v1, v2;

	if (read_u16(f, &v1) != 0) {
		return -1;
	}
	if (read_u16(f, &v2) != 0) {
		return -1;
	}
	*v = v2;
	*v <<= 16;
	*v |= v1;
	return 0;
}

/* seek to a position within a set of streams - return -1 on failure */
static int seekto(struct stream_info *sinfo, u32 pos)
{
	off_t spos = pos + sinfo->initial_pos;
	if (lseek(sinfo->fd, spos, SEEK_SET) != spos) {
		err_msg("Failed to seek to %d in stream\n", pos);
		return -1;
	}
	return 0;
}

/* open a set of output streams, compressing with the given
   bzip level */
void *open_stream_out(int f, int n, int bzip_level)
{
	int i;
	struct stream_info *sinfo;

	sinfo = malloc(sizeof(*sinfo));
	if (!sinfo) {
		return NULL;
	}

	sinfo->num_streams = n;
	sinfo->cur_pos = 0;
	sinfo->fd = f;
	if (bzip_level == 0) {
		sinfo->bufsize = 100*1024;
	} else {
		sinfo->bufsize = 100*1024*bzip_level;
	}
	sinfo->initial_pos = lseek(f, 0, SEEK_CUR);

	sinfo->s = (struct stream *)calloc(sizeof(sinfo->s[0]), n);
	if (!sinfo->s) {
		free(sinfo);
		return NULL;
	}

	for (i=0;i<n;i++) {
		sinfo->s[i].buf = malloc(sinfo->bufsize);
		if (!sinfo->s[i].buf) goto failed;
		sinfo->s[i].bzip_level = bzip_level;
	}

	/* write the initial headers */
	for (i=0;i<n;i++) {
		sinfo->s[i].last_head = sinfo->cur_pos + 9;
		write_u8(sinfo->fd, CTYPE_NONE);
		write_u32(sinfo->fd, 0);
		write_u32(sinfo->fd, 0);
		write_u32(sinfo->fd, 0);
		sinfo->cur_pos += 13;
	}
	return (void *)sinfo;

failed:
	for (i=0;i<n;i++) {
		if (sinfo->s[i].buf) free(sinfo->s[i].buf);
	}
	free(sinfo);
	return NULL;
}

/* prepare a set of n streams for reading on file descriptor f */
void *open_stream_in(int f, int n)
{
	int i;
	struct stream_info *sinfo;

	sinfo = calloc(sizeof(*sinfo), 1);
	if (!sinfo) {
		return NULL;
	}

	sinfo->num_streams = n;
	sinfo->fd = f;
	sinfo->initial_pos = lseek(f, 0, SEEK_CUR);

	sinfo->s = (struct stream *)calloc(sizeof(sinfo->s[0]), n);
	if (!sinfo->s) {
		free(sinfo);
		return NULL;
	}

	for (i=0;i<n;i++) {
		uchar c;
		u32 v1, v2;

	again:
		if (read_u8(f, &c) != 0) {
			goto failed;
		}
		if (read_u32(f, &v1) != 0) {
			goto failed;
		}
		if (read_u32(f, &v2) != 0) {
			goto failed;
		}
		if (read_u32(f, &sinfo->s[i].last_head) != 0) {
			goto failed;
		}

		if (c == CTYPE_NONE && v1==0 && v2==0 && sinfo->s[i].last_head==0 &&
		    i == 0) {
			err_msg("Enabling stream close workaround\n");
			sinfo->initial_pos += 13;
			goto again;
		}

		sinfo->total_read += 13;

		if (c != CTYPE_NONE) {
			err_msg("Unexpected initial tag %d in streams\n", c);
			goto failed;
		}
		if (v1 != 0) {
			err_msg("Unexpected initial c_len %d in streams %d\n", v1, v2);
			goto failed;
		}
		if (v2 != 0) {
			err_msg("Unexpected initial u_len %d in streams\n", v2);
			goto failed;
		}
	}

	return (void *)sinfo;

failed:
	free(sinfo->s);
	free(sinfo);
	return NULL;
}


/* flush out any data in a stream buffer. Return -1 on failure */
static int flush_buffer(struct stream_info *sinfo, int stream)
{
	int c_type = CTYPE_NONE;
	u32 c_len = sinfo->s[stream].buflen;
	
	if (seekto(sinfo, sinfo->s[stream].last_head) != 0) {
		return -1;
	}
	if (write_u32(sinfo->fd, sinfo->cur_pos) != 0) {
		return -1;
	}

	sinfo->s[stream].last_head = sinfo->cur_pos + 9;
	if (seekto(sinfo, sinfo->cur_pos) != 0) {
		return -1;
	}

	compress_buf(&sinfo->s[stream], &c_type, &c_len);

	if (write_u8(sinfo->fd, c_type) != 0 ||
	    write_u32(sinfo->fd, c_len) != 0 ||
	    write_u32(sinfo->fd, sinfo->s[stream].buflen) != 0 ||
	    write_u32(sinfo->fd, 0) != 0) {
		return -1;
	}
	sinfo->cur_pos += 13;

	if (write_buf(sinfo->fd, sinfo->s[stream].buf, c_len) != 0) {
		return -1;
	}
	sinfo->cur_pos += c_len;

	sinfo->s[stream].buflen = 0;

	free(sinfo->s[stream].buf);
	sinfo->s[stream].buf = malloc(sinfo->bufsize);
	if (!sinfo->s[stream].buf) {
		return -1;
	}
	return 0;
}

/* fill a buffer from a stream - return -1 on failure */
static int fill_buffer(struct stream_info *sinfo, int stream)
{
	uchar c_type;
	u32 u_len, c_len;

	if (seekto(sinfo, sinfo->s[stream].last_head) != 0) {
		return -1;
	}

	if (read_u8(sinfo->fd, &c_type) != 0) {
		return -1;
	}
	if (read_u32(sinfo->fd, &c_len) != 0) {
		return -1;
	}
	if (read_u32(sinfo->fd, &u_len) != 0) {
		return -1;
	}
	if (read_u32(sinfo->fd, &sinfo->s[stream].last_head) != 0) {
		return -1;
	}

	sinfo->total_read += 13;

	if (sinfo->s[stream].buf) {
		free(sinfo->s[stream].buf);
	}
	sinfo->s[stream].buf = malloc(u_len);
	if (!sinfo->s[stream].buf) {
		return -1;
	}
	if (read_buf(sinfo->fd, sinfo->s[stream].buf, c_len) != 0) {
		return -1;
	}

	sinfo->total_read += c_len;

	sinfo->s[stream].buflen = u_len;
	sinfo->s[stream].bufp = 0;

	if (decompress_buf(&sinfo->s[stream], c_len, c_type) != 0) {
		return -1;
	}

	return 0;
}

/* write some data to a stream. Return -1 on failure */
int write_stream(void *ss, int stream, uchar *p, int len)
{
	struct stream_info *sinfo = ss;

	while (len) {
		int n = MIN(sinfo->bufsize - sinfo->s[stream].buflen, len);

		memcpy(sinfo->s[stream].buf+sinfo->s[stream].buflen, p, n);
		sinfo->s[stream].buflen += n;
		p += n;
		len -= n;

		if (sinfo->s[stream].buflen == sinfo->bufsize) {
			if (flush_buffer(sinfo, stream) != 0) {
				return -1;
			}
		}
	}
	return 0;
}

/* read some data from a stream. Return number of bytes read, or -1
   on failure */
int read_stream(void *ss, int stream, uchar *p, int len)
{
	struct stream_info *sinfo = ss;
	int ret=0;

	while (len) {
		int n = MIN(sinfo->s[stream].buflen-sinfo->s[stream].bufp, len);

		if (n > 0) {
			memcpy(p, sinfo->s[stream].buf+sinfo->s[stream].bufp, n);
			sinfo->s[stream].bufp += n;
			p += n;
			len -= n;
			ret += n;
		}

		if (len &&
		    sinfo->s[stream].bufp == sinfo->s[stream].buflen) {
			if (fill_buffer(sinfo, stream) != 0) {
				return -1;
			}
			if (sinfo->s[stream].bufp == sinfo->s[stream].buflen) break;
		}
	}

	return ret;
}

/* flush and close down a stream. return -1 on failure */
int close_stream_out(void *ss)
{
	struct stream_info *sinfo = ss;
	int i;
	for (i=0;i<sinfo->num_streams;i++) {
		if (sinfo->s[i].buflen != 0 &&
		    flush_buffer(sinfo, i) != 0) {
			return -1;
		}
		if (sinfo->s[i].buf) free(sinfo->s[i].buf);
	}
	free(sinfo->s);
	free(sinfo);
	return 0;
}

/* close down an input stream */
int close_stream_in(void *ss)
{
	struct stream_info *sinfo = ss;
	int i;

	if (lseek(sinfo->fd, sinfo->initial_pos + sinfo->total_read, 
		  SEEK_SET) != sinfo->initial_pos + sinfo->total_read) {
		return -1;
	}
	for (i=0;i<sinfo->num_streams;i++) {
		if (sinfo->s[i].buf) free(sinfo->s[i].buf);
	}

	free(sinfo->s);
	free(sinfo);
	return 0;
}
