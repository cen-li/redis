/*
 * Copyright (c) 2009-2012, Pieter Noordhuis <pcnoordhuis at gmail dot com>
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
#include "redis.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/time.h>

#include "hiredis.h"
#include "sds.h"
#include "zmalloc.h"
#include "anet.h"

#define MAX_LONG_LEN 128
#define REDIS_AOF_COMMAND_THRESHOLD 1024
#define REPLAY_DEFAULT_PIPE_CMDS 100

#define CMD_LONG 1 // return immediately on eof
#define ARG_LONG 0 // retry on eof

static int loglevel = REDIS_NOTICE;
/*
 * replayCtx: contain the replay context 
 * @file buffer needed? suppose to be not, we use cached IO, 
 * @if needed we can increase the cache size
 */
typedef struct replayCtx {

    off_t size;             // current file size
    off_t pos;              // current file seek position
    off_t diff;             // current file diff

    long processed_cmd;     // processed commands number
    long skipped_cmd;       // unrecoginized commands number

    FILE *fp;               // aof file fp
    sds filename;           // aof file name
    void *buf;              // buffer for file I/O

    sds ip;                 // target ip
    int port;               // target port

    int pipe_cmds;          // commands sent per time
    redisContext * redis;   // context for replay the commands

    sds filter;             // key filter;
    sds prefix;             // prefix to be replaced
    sds replace_prefix;     // replace prefix to be added for keys

}replayCtx;

/* configuraton for replay */
typedef struct Config {
    int pipe_cmds;
    char * filename;
    char * host;
    char * prefix;
    char * replace_prefix;
    char * filter;
    int verbose;
    int only_check;
} Config;

static char * CMD_SUPPORTED[] = {
    "MSET",     
    "MSETNX",   
    "DEL",      
    "SET",
    "HMSET",
    "EXPIRE",
    "EXPIREAT",
    "PERSIST",
    "PEXPIRE",
    "PEXPIREAT",
    "DUMP",
    "EXISTS",
    "PTTL",
    "RESTORE",
    "TTL",
    "TYPE",
    "APPEND",
    "BITCOUNT",
    "DECR",
    "DECRBY",
    "GET",
    "GETBIT",
    "GETRANGE",
    "GETSET",
    "INCR",
    "INCRBY",
    "INCRBYFLOAT",
    "MGET",
    "PSETEX",
    "SETBIT",
    "SETEX",
    "SETNX",
    "SETRANGE",
    "STRLEN",
    "HDEL",
    "HEXISTS",
    "HGET",
    "HGETALL",
    "HINCRBY",
    "HINCRBYFLOAT",
    "HKEYS",
    "HLEN",
    "HMGET",
    "HSET",
    "HSETNX",
    "HVALS",
    "LINDEX",
    "LINSERT",
    "LLEN",
    "LPOP",
    "LPUSH",
    "LPUSHX",
    "LRANGE",
    "LREM",
    "LSET",
    "LTRIM",
    "RPOP",
    "RPUSH",
    "RPUSHX",
    "SADD",
    "SCARD",
    "SISMEMBER",
    "SMEMBERS",
    "SPOP",
    "SRANDMEMBER",
    "SREM",
    "ZADD",
    "ZCARD",
    "ZCOUNT",
    "ZINCRBY",
    "ZRANGE",
    "ZRANGEBYSCORE",
    "ZRANK",
    "ZREM",
    "ZREMRANGEBYRANK",
    "ZREMRANGEBYSCORE",
    "ZREVRANGE",
    "ZREVRANGEBYSCORE",
    "ZREVRANK",
    "ZSCORE",
};

/*
 * util section
 *
 */

/* Return the UNIX time in microseconds */
long long ustime(void) {
    struct timeval tv;
    long long ust;

    gettimeofday(&tv, NULL);
    ust = ((long long)tv.tv_sec)*1000000;
    ust += tv.tv_usec;
    return ust;
}

void redisLog(int level, const char *fmt, ...) {
    va_list ap;
    char msg[1024];

    if ((level&0xff) < loglevel) return;

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    const char *c = ".-*#";
    char buf[64];

    int off;
    struct timeval tv;

    gettimeofday(&tv,NULL);
    off = strftime(buf,sizeof(buf),"%d %b %H:%M:%S.",localtime(&tv.tv_sec));
    snprintf(buf+off,sizeof(buf)-off,"%03d",(int)tv.tv_usec/1000);
    printf("[%d] %s %c %s\n",(int)getpid(),buf,c[level],msg);
    fflush(stdout);

} 

int consumeNewline(char *buf) {
    if (strncmp(buf,"\r\n",2) != 0) {
        return REDIS_ERR;
    }
    return REDIS_OK;
}

void safeFreeArgvs(int argc, sds *argvs) {
    if(argvs == NULL) return;

    for (int i = 0; i < argc; i++) {
        sdsfree(argvs[i]);
    }

    zfree(argvs);
}

void refreshAppendOnlyFileStat(replayCtx *repctx)
{
    struct redis_stat sb;     
    FILE *fp = repctx->fp;
    if (redis_fstat(fileno(fp),&sb) == -1) {
        //redisLog(REDIS_WARNING, "failed to get stat of file %s", repctx->filename);
        redisLog(REDIS_NOTICE, "failed to get stat of file %s\n", repctx->filename);
        return;
    }
    repctx->size = sb.st_size;
    repctx->pos = ftello(fp);
    repctx->diff = repctx->size - repctx->pos;
}

int safeReadLong(replayCtx *repctx, char prefix, long *target, int return_immediately)
{
    char buf[MAX_LONG_LEN], *eptr;
    FILE *fp = repctx->fp;
    long len = 0, remain = MAX_LONG_LEN, bytes = 0;
    int retry = 1;

    eptr = buf;

    memset(buf, '\0', sizeof(buf));

    do {
        if(fgets(eptr, remain, fp) == NULL) {
            /* return 0 if eof is read at the 1st try, and this is an expected way */
            if(retry == 1 && return_immediately) {
                *target = 0;
                return REDIS_OK;
            } else {
                ++retry;
                usleep(1000000);
                continue;
            }
        }

        if(buf[0] != prefix) {
            return REDIS_ERR;
        }

        len = strtol(buf + 1, &eptr, 10);

        if(consumeNewline(eptr) == REDIS_OK) {
            *target = len;
            return REDIS_OK;
        }

        bytes = (long) (eptr - buf);
        remain -= bytes;
        ++retry;
    } while(remain > 0);

    return REDIS_ERR;
}

int safeReadString(replayCtx *repctx, sds * target, int key)
{
    sds argsds;
    long len, remain, bytes;
    char *buf = NULL, *eptr;
    long prelen = 0, replen = 0;
    int retry = 1;

    int should = key && repctx->prefix != NULL && repctx->replace_prefix != NULL;

    if( should ) {
        prelen = sdslen(repctx->prefix);
        replen = sdslen(repctx->replace_prefix);
    }

    if(safeReadLong(repctx, '$', &len, ARG_LONG) == REDIS_ERR) {
        redisLog(REDIS_WARNING, "failed to read argument length, bad format file");
        goto error;
    }

    buf = (char *)zmalloc(len + 2);
    if(buf == NULL) {
        redisLog(REDIS_WARNING, "failed to zmalloc memory for command argument");
        goto error;
    }

    eptr = buf;
    remain = len + 2;
    do {
        /* do the file position check */
        if(retry % 2 == 0) { 
            usleep(1000000);
        }

        bytes = fread(eptr, 1, remain, repctx->fp);
        remain -= bytes;
        eptr = eptr + bytes;
        ++retry;
    }while(remain > 0);

    eptr = buf + len;
    if(consumeNewline(eptr) == REDIS_ERR) {
        redisLog(REDIS_WARNING, "no new line subffix found");
        goto error;
    }

    long real = len;

    if(should) {
        if(!strncmp(repctx->prefix, buf, sdslen(repctx->prefix))){
            real = len + replen - prelen;
        } else {
            should = 0;
        }
    }

    buf[len] = '\0';
    argsds = sdsnewlen(NULL, real);

    if( should ) {
        sdscpy(argsds, repctx->replace_prefix);
        sdscat(argsds, buf + prelen);
    } else {
        sdscpy(argsds, buf);
    }

    *target = argsds;
    zfree(buf);
    return REDIS_OK;

error:
    *target = NULL;

    if(buf != NULL) {
        zfree(buf);
    }

    return REDIS_ERR;
}

int supported(sds cmd)
{
    int numcommands = sizeof(CMD_SUPPORTED) / sizeof(char *);

    for (int i = 0; i < numcommands; i++) {
        if(!strcasecmp(CMD_SUPPORTED[i], cmd)) {
            return 1;
        }
    }

    return 0;
}

int getRedisCommand(replayCtx *ctx, int *argc, sds **argv){
    long len = 0, j, step = 1;
    int normal = 0, key = 0;
    sds *tmp;

    if(safeReadLong(ctx, '*', &len, CMD_LONG) != REDIS_OK) {
        goto error;
    }

    if (len == 0) {
        *argv = NULL;
        *argc = 0;
        return REDIS_OK;
    }

    *argc = len;
    tmp = (sds *)zmalloc(len * sizeof(sds));
    if(tmp == NULL) {
        goto error;
    }

    if(safeReadString(ctx, &tmp[0], 0) != REDIS_OK) {
        goto error;
    }

    if (!strcasecmp(tmp[0], "SELECT")) {
        normal = 0;
        step = 0;
    } else if (!strcasecmp(tmp[0], "MSET")) {
        normal = 0;
        step = 2;
    } else if (!strcasecmp(tmp[0], "MSETNX")) {
        normal = 0;
        step = 2;
    } else if (!strcasecmp(tmp[0], "DEL")) {
        normal = 0;
        step = 1;
    } else {
        normal = 1;
        step = 0;
    }

    for (j = 1; j < len; j++) {
        if (normal) {
            if( j == 1) {
                key = 1;
            } else {
                key = 0;
            }
        } else {
            if( step == 0 ) {
                key = 0;
            } else if ( (j + 1) % step == 0 ) {
                key = 1;
            } else {
                key = 0;
            }
        }
        if(safeReadString(ctx, &tmp[j], key) != REDIS_OK) {
            goto error;
        }
    }

    *argv = tmp;
    return REDIS_OK;

error:
    safeFreeArgvs(len, *argv);
    *argv = NULL;
    return REDIS_ERR;
}

void cmdToString(int argc, const char **argv) {
    for(int i = 0; i < argc; i++) {
        printf("%s ", argv[i]);
    }
    printf("\n");
}

int getPipeReply(replayCtx *ctx)
{
    redisContext *c = ctx->redis;
    void *reply = NULL;
    int ret = REDIS_OK;

    ret = redisGetReply(c, &reply);
    if(c->err & (REDIS_ERR_IO | REDIS_ERR_EOF)) {
        redisLog(REDIS_WARNING, "connection error, failed to get reply");
    } else if(c->err) {
        redisLog(REDIS_WARNING, "command execution error");
    }
    if(reply != NULL) {
        freeReplyObject(reply);
    }
    return ret;
}

void checkAppendOnlyFile(replayCtx *ctx)
{
    sds *argv = NULL;
    int argc;
    long loops = 0;

    while(1) {
        if(getRedisCommand(ctx, &argc, &argv) != REDIS_OK) {
            redisLog(REDIS_WARNING, "wrong format command at position %ld", (long)ctx->pos);
            goto end;
        }

        if(argv == NULL) {
            redisLog(REDIS_WARNING, "reach end of file");
            goto end;
        }

        if (!supported(argv[0])) {     
            sds cmd = sdsjoin(argv, argc, " ");
            redisLog(REDIS_NOTICE, "unsupported cmd found: %s", cmd);
            sdsfree(cmd); 
            ctx->skipped_cmd += 1; 
        } else {
            ctx->processed_cmd += 1;
        }

        loops += 1;
        safeFreeArgvs(argc, argv);     

        refreshAppendOnlyFileStat(ctx);
        if( loops >= 10000 ) {
             redisLog(REDIS_NOTICE, "progress: [processed:%ld] [skipped:%ld] [filesize:%ld] [postion:%ld] [diff:%ld]", \
                ctx->processed_cmd, ctx->skipped_cmd, (long)ctx->size, (long)ctx->pos, (long)ctx->diff);
             loops = 0;
        }
    }

end:
    refreshAppendOnlyFileStat(ctx);
    redisLog(REDIS_NOTICE, "progress: [processed:%ld] [skipped:%ld] [filesize:%ld] [postion:%ld] [diff:%ld]", \
        ctx->processed_cmd, ctx->skipped_cmd, (long)ctx->size, (long)ctx->pos, (long)ctx->diff);
    return;
}

void processAppendOnlyFile(replayCtx *ctx)
{

    unsigned long loops = 0;
    int j, argc;
    int cmds = 0;
    sds * argv = NULL;

    //long long getcommand = 0;
    //long long runcommand = 0;
    //long long aftercommand = 0;

    redisLog(REDIS_NOTICE, "start to process aof file: %s", ctx->filename);
    while(1) {
        cmds = 0;
        while(cmds < ctx->pipe_cmds) {
            if(getRedisCommand(ctx, &argc, &argv) != REDIS_OK) {
                redisLog(REDIS_WARNING, "wrong format command at position %ld", (long)ctx->pos);
                //printf("wrong format command at %x\n", ctx->pos);
                goto error;
            }

            if(argv == NULL) {
                usleep(1000000);
                break;
            }

            if(argc > 1 && ctx->filter != NULL) {
                if(!strncasecmp(ctx->filter, argv[1], sdslen(ctx->filter))) {
                    ctx->skipped_cmd += 1;
                    safeFreeArgvs(argc, argv);
                    continue;
                }
            }

            if (!supported(argv[0])) {
                sds cmd = sdsjoin(argv, argc, " ");
                redisLog(REDIS_NOTICE, "unsupported cmd found: %s", cmd);
                sdsfree(cmd);
                ctx->skipped_cmd += 1;
            } else if (!strcasecmp(argv[0], "MSET") || !strcasecmp(argv[0], "MSETNX")) {
                char ** subargv = NULL;
                size_t * subargvlen = NULL;
                for(j = 1; j < argc; j++) {
                    subargv = (char **)zmalloc(3 * sizeof(char *));
                    if (subargv == NULL) {
                        goto error;
                    }

                    subargvlen = zmalloc(3 * sizeof(size_t));
                    if(subargvlen == NULL) {
                        zfree(subargv);
                        goto error;
                    }

                    subargv[0] = argv[0] + 1;
                    subargv[1] = argv[j++];
                    subargv[2] = argv[j];
                    subargvlen[0] = strlen(subargv[0]);
                    subargvlen[1] = strlen(subargv[1]);
                    subargvlen[2] = strlen(subargv[2]);

                    if(redisAppendCommandArgv(ctx->redis, 3, (const char **)subargv, subargvlen) != REDIS_OK) {
                        redisLog(REDIS_WARNING, "failed to append sub command to redis output buffer");
                        goto error;
                    }

                    zfree(subargvlen);
                    zfree(subargv);
                    ++cmds;
                }
            } else {
                size_t *argvlen = zmalloc(argc * sizeof(size_t));
                if(argvlen == NULL) {
                    goto error;
                }

                for(j = 0; j < argc; j++) {
                    argvlen[j] = sdslen(argv[j]);
                }

                if(redisAppendCommandArgv(ctx->redis, argc, (const char **)argv, argvlen) != REDIS_OK) {
                    redisLog(REDIS_WARNING, "failed to append command to redis output buffer");
                    goto error;
                }
                zfree(argvlen);
                argvlen = NULL;
                ++cmds;
            }
            safeFreeArgvs(argc, argv);
        }

        redisLog(REDIS_DEBUG, "%d commands sent in this loop", cmds);

        for(int i = 0; i < cmds; i++) {
            if(getPipeReply(ctx) != REDIS_OK) {
                goto error;
            }
        }

        loops += (cmds + 1);
        ctx->processed_cmd += cmds;

        refreshAppendOnlyFileStat(ctx);
        if( (loops >= 10000) || (ctx->diff < REDIS_AOF_COMMAND_THRESHOLD) ) {
            //redisLog(REDIS_NOTICE, "process: %d:%d", ctx->pos, ctx->size);
            redisLog(REDIS_NOTICE, "progress: [processed:%ld] [skipped:%ld] [filesize:%ld] [postion:%ld] [diff:%ld]", \
                ctx->processed_cmd, ctx->skipped_cmd, (long)ctx->size, (long)ctx->pos, (long)ctx->diff);
            loops = 0;
        }
    }

error:
    return;

}

int pingRedis(replayCtx *ctx)
{
    redisContext *rc = ctx->redis;
    redisCommand(rc, "INFO");
    if(rc->err) {
        return REDIS_ERR;
    }
    return REDIS_OK;
}

void freeReplayCtx(replayCtx *repctx)
{

    if(repctx->fp != NULL) {
        fclose(repctx->fp);
    }

    if(repctx->buf != NULL) {
        zfree(repctx->buf);
    }
    if(repctx->filename != NULL) {
        sdsfree(repctx->filename);
    }

    if(repctx->ip != NULL) {
        sdsfree(repctx->ip);
    }

    if(repctx->filter != NULL) {
        sdsfree(repctx->filter);
    }

    if(repctx->prefix != NULL) {
        sdsfree(repctx->prefix);
    }

    if(repctx->replace_prefix != NULL) {
        sdsfree(repctx->replace_prefix);
    }

    if(repctx->redis != NULL) {
        redisFree(repctx->redis);
    }
}

replayCtx *initReplayCtx(Config * config)
{

    replayCtx *repctx = (replayCtx*)zmalloc(sizeof(replayCtx));
    if (NULL == repctx) {
        //redisLog(REDIS_WARNING, "Failed to malloc replayCtx");
        redisLog(REDIS_WARNING, "Failed to malloc replayCtx");
        return NULL;
    }

    repctx->filename = sdsnew(config->filename);
    repctx->fp = fopen(repctx->filename, "r+");
    if (repctx->fp == NULL) {
        //redisLog(REDIS_WARNING, "Cannot open file: %s\n", filename);
        redisLog(REDIS_WARNING, "Cannot open file: %s", config->filename);
        goto error;
    }

    /* set large buffer for file IO */
    repctx->buf = (void *)zmalloc(1024 * 1024);
    setbuf(repctx->fp, repctx->buf);

    char *p = config->host;
    char *s = strchr(p, ':');
    *s = '\0';
    repctx->ip = sdsnew(p);
    repctx->port = atoi(s+1);

    repctx->pos = 0;
    repctx->processed_cmd = 0;
    repctx->skipped_cmd = 0;

    repctx->pipe_cmds = config->pipe_cmds;

    repctx->prefix = NULL;
    repctx->replace_prefix = NULL;
    if (config->prefix != NULL && config->replace_prefix != NULL) {
        repctx->prefix = sdsnew(config->prefix);
        repctx->replace_prefix = sdsnew(config->replace_prefix);
    }

    repctx->filter = NULL;
    if (config->filter != NULL) {
        repctx->filter = sdsnew(config->filter);
    }

    refreshAppendOnlyFileStat(repctx);

    repctx->redis = redisConnect(repctx->ip, repctx->port);
    if (repctx->redis->err) {
        //redisLog(REDIS_WARNING, "Cannot stat file: %s\n", filename);
        redisLog(REDIS_WARNING, "failed to connecto to %s:%d\n", repctx->ip, repctx->port);
        goto error;
    }
    return repctx;

error:
    freeReplayCtx(repctx);
    return NULL;

}

void parseArgs(int argc, char **argv, Config *config)
{

    config->filename = NULL;
    config->host = NULL;
    config->filter = NULL;
    config->prefix = NULL;
    config->replace_prefix = NULL;
    config->pipe_cmds = REPLAY_DEFAULT_PIPE_CMDS;
    config->verbose = 0;
    config->only_check = 0;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--file") == 0) {
            config->filename = argv[i+1];
            ++i;
        }

        if (strcmp(argv[i], "--dest") == 0) {
            config->host = argv[i+1];
            ++i;
        }

        if (strcmp(argv[i], "--prefix") == 0) {
            config->prefix = argv[i+1];
            ++i;
        }

        if (strcmp(argv[i], "--replace_prefix") == 0) {
            config->replace_prefix = argv[i+1];
            ++i;
        }

        if (strcmp(argv[i], "--pipe_cmds") == 0) {
            config->pipe_cmds = atoi(argv[i+1]);
            ++i;
        }
        if (strcmp(argv[i], "--filter") == 0) {
            config->filter = argv[i+1];
            ++i;
        }
        if (strcmp(argv[i], "--verbose") == 0 || strcmp(argv[i], "-v") == 0) {
            config->verbose = 1;
        }
        if (strcmp(argv[i], "--only_check") == 0) {
            config->only_check = 1;
        }
    }
}

int main(int argc, char **argv) {

    replayCtx * repctx = NULL;
    if (argc < 4) {
        printf("Usage: %s --file <file.aof> --dest <ip:port> [--filter <filter>]        \
             [--prefix <prefix>] [--replace_prefix <replace>] [--pipe_cmds <cmds>]      \
             [--only_check] [-v|--verbose]\n", argv[0]);
        exit(1);
    } 

    Config * config = (Config *)zmalloc(sizeof(Config));
    if (config == NULL) {
        exit(1);
    }

    parseArgs(argc, argv, config);
    if (config->filename == NULL || config->host == NULL) {
        exit(1);
    }

    if(config->verbose) {
        loglevel = REDIS_VERBOSE;
    }

    repctx = initReplayCtx(config);
    if(repctx == NULL) {
        redisLog(REDIS_WARNING, "failed to init replay context");
        exit(1);
    }

    int only_check = config->only_check;
    zfree(config);

    if( only_check ) {
        checkAppendOnlyFile(repctx);
    } else {
        processAppendOnlyFile(repctx);
    }

    return 0;
}
