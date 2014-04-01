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
#define REDIS_AOF_COMMAND_THRESHOLD 100000
#define REPLAY_DEFAULT_PIPE_CMDS 100

#define CMD_PREFIX '*'  // command arguments number prefix
#define ARG_PREFIX '$'  // command arguments 

#define NORMAL_START 0
#define PIPELINE_START 1
#define COMMAND_START 2

#define LOOP_MAX_CMDS 100000 // max commands to be scanned in a loop

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
    long unsupported_cmd;   // unsupported commands number
    long scanned_cmd;       // scanned commands number

    FILE *fp;               // aof file fp
    sds filename;           // aof file name
    void *buf;              // buffer for file I/O

    sds ip;                 // target ip
    int port;               // target port

    int pipe_cmds;          // commands sent per time
    redisContext * redis;   // context for replay the commands

    int nfilter;            // number of filter
    sds *filter;            // key filter;

    int nrule;              // number of the replace rule
    sds *prefix;            // prefix to be replaced
    sds *replace_prefix;    // replace prefix to be added for keys

    off_t pipeline_spos;    // position where a pipeline start
    off_t command_spos;     // postion where a command start

    sds lastlen;            // cached last length

}replayCtx;

/* configuraton for replay */
typedef struct Config {
    int pipe_cmds;
    char * filename;
    char * host;
    char * rule;
    char * filter;
    char * logfile;
    int verbose;
    int only_check;
} Config;

Config conf;

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

    FILE *fp = stdout;

    gettimeofday(&tv,NULL);
    off = strftime(buf,sizeof(buf),"%d %b %H:%M:%S.",localtime(&tv.tv_sec));
    snprintf(buf+off,sizeof(buf)-off,"%03d",(int)tv.tv_usec/1000);
    fprintf(fp, "[%d] %s %c %s\n",(int)getpid(),buf,c[level],msg);
    fflush(fp);

    if(conf.logfile != NULL) {
        fp = fopen(conf.logfile, "a");
        fprintf(fp, "[%d] %s %c %s\n",(int)getpid(),buf,c[level],msg);
        fflush(fp);
        fclose(fp);
    }
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

int matchAnyPrefix(int nprefix, sds *prefix, char *buf, int *index)
{
    int nul = -1;
    for(int i = 0; i < nprefix; i++) {
        //redisLog(REDIS_WARNING, "matching prefix: %s", prefix[i]);
        if(prefix[i] == NULL) {
            nul = i;
        } else if(strncmp(prefix[i], buf, sdslen(prefix[i])) == 0) {
            *index = i;
            return REDIS_OK;
        }
    }

    if(nul != -1) {
        *index = nul;
        return REDIS_OK;
    }
    return REDIS_ERR;
}

void refreshAppendOnlyFileStat(replayCtx *ctx, int type)
{
    struct redis_stat sb;     
    FILE *fp = ctx->fp;
    if (redis_fstat(fileno(fp),&sb) == -1) {
        redisLog(REDIS_NOTICE, "failed to get stat of file %s", ctx->filename);
        return;
    }
    ctx->size = sb.st_size;
    ctx->pos = ftello(fp);
    ctx->diff = ctx->size - ctx->pos;

    if(type == PIPELINE_START) {
        ctx->pipeline_spos = ctx->pos;
    }

    if(type == COMMAND_START) {
        ctx->command_spos = ctx->pos;
    }

}

/*
 * 1. read the 1st byte, if got eof, and prefix = '*', return
 * 2. read until \r\n shown up
 * 3. check if format is valid
 */
int safeReadLong(replayCtx *ctx, char prefix, long *target)
{
    char buf[MAX_LONG_LEN], *eptr, *nptr;
    FILE *fp = ctx->fp;
    long len = 0, byte = 0;

    int rflag = 0, nflag = 0, retry = 1;

    eptr = buf;
    nptr = NULL;
    memset(buf, '\0', sizeof(buf));

    do {
        byte = fread(eptr, 1, 1, fp);
        if (byte == 1) {
            if(*eptr == '\n') {
                nflag = 1;
            } else if(*eptr == '\r') {
                rflag = 1;
            } else {
                rflag = 0;
                nflag = 0;
            }
            ++eptr;
        } else if (byte == 0) {
            /* if we are reading the cmd argument number, and got eof, just return */
            if(prefix == CMD_PREFIX && retry == 1) {
                *target = 0;
                return REDIS_OK;
            }
            usleep(100000);
        } else {
            *target = 0;
            return REDIS_ERR;
        }
        ++retry;
    }while(rflag != 1 || nflag != 1);

    len = strtol(buf + 1, &nptr, 10);
    sdscpylen(ctx->lastlen, buf, (long)(nptr - buf));
    redisLog(REDIS_VERBOSE, "read number string: %s", ctx->lastlen);

    if((nptr + 2) != eptr) {
        *target = 0;
        return REDIS_ERR;
    }

    *target = len;
    return REDIS_OK;
}

/*
 * 1. get the argument length LEN
 * 2. read next LEN + 2 bytes 
 * 3. check new line subfix
 * 4. replace prefix
 * 5. return the line
 */
int safeReadString(replayCtx *ctx, sds * target, int key)
{
    sds argsds = NULL;          
    long len = 0;                       /* argument length */
    long remain = 0;                    /* remaining bytes to be read */
    long bytes = 0;                     /* bytes read every round */
    long prelen = 0, replen = 0;        /* prefix length, replace prefix length */
    char *buf = NULL, *eptr = NULL;     /* buf for the j*/

    int retry = 0;
    int should = key && ctx->prefix != NULL && ctx->replace_prefix != NULL;

    if(safeReadLong(ctx, '$', &len) == REDIS_ERR) {
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
        bytes = fread(eptr, 1, remain, ctx->fp);
        remain -= bytes;
        eptr = eptr + bytes;
        if(++retry > 1) usleep(1000000);
    }while(remain > 0);

    eptr = buf + len;
    if(consumeNewline(eptr) == REDIS_ERR) {
        redisLog(REDIS_WARNING, "no new line subffix found, length string is: %s", ctx->lastlen);
        goto error;
    }
    buf[len] = '\0';

    /* replace the prefix for key field */
    int index = -1;

    if( should && matchAnyPrefix(ctx->nrule, ctx->prefix, buf, &index) == REDIS_OK) {
        sds oldprefix = ctx->prefix[index];
        sds newprefix = ctx->replace_prefix[index];

        prelen = (oldprefix == NULL ? 0 : sdslen(oldprefix));
        replen = (newprefix == NULL ? 0 : sdslen(newprefix));
        int newlen = len + replen - prelen;

        argsds = sdsnewlen(NULL, newlen);

        if( newprefix != NULL ) {
            sdscpylen(argsds, newprefix, replen);
        }
        sdscatlen(argsds, buf + prelen, len - prelen );
    } else {
        argsds = sdsnewlen(NULL, len);

        sdscpylen(argsds, buf, len);
    }

    *target = argsds;
    zfree(buf);
    return REDIS_OK;
        
error:
    *target = NULL;
    zfree(buf);

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
    long len = 0, j=0;
    int plan = 0, key = 0;
    sds *tmp;

    if(safeReadLong(ctx, '*', &len) != REDIS_OK) {
        redisLog(REDIS_WARNING, "failed to get cmd argument number");
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
        redisLog(REDIS_WARNING, "failed to get command string, argument index: 0"); 
        goto error;
    }

    if (!strcasecmp(tmp[0], "SELECT")) {
        plan = 1;
    } else if (!strcasecmp(tmp[0], "MSET") || !strcasecmp(tmp[0], "MSETNX")) {
        plan = 2;
    } else if (!strcasecmp(tmp[0], "DEL")) {
        plan = 3;
    }

    for (j = 1; j < len; j++) {
        key = 0;
        switch(plan) {
            case 0:
                if(j == 1) key = 1;
                break;
            case 1:
                key = 0;
                break;
            case 2:
                if( j % 2 == 1 ) key = 1;
                break;
            case 3:
                key = 1;
                break;
        }

        if(safeReadString(ctx, &tmp[j], key) != REDIS_OK) {
            redisLog(REDIS_WARNING, "failed to get command argument, argument index: %ld", j);
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

/*
 * 1 => skip this command
 * 0 => this command needs to be processed
 */
int skipThisCommand(replayCtx *ctx, int argc, sds *argv)
{
    int nkeys = 0;
    sds *keys = NULL;

    if(!supported(argv[0])) {
        ctx->unsupported_cmd += 1;
        sds cmd = sdsjoin(argv, argc, " ");
        redisLog(REDIS_NOTICE, "unsupported cmd found: %s", cmd);
        sdsfree(cmd);    
        return 1;
    }

    if(ctx->nfilter == 0 && ctx->nrule == 0) {
        return 0;
    }

    if(ctx->nfilter != 0) {
        nkeys = ctx->nfilter;
        keys = ctx->filter;
    } else if(ctx->nrule != 0) {
        nkeys = ctx->nrule;
        keys = ctx->replace_prefix;
    }

    int index = -1;
    if(argc > 1 && keys != NULL) {
        if(matchAnyPrefix(nkeys, keys, argv[1], &index) != REDIS_OK) {
            return 1;
        }
    }

    return 0;
} 

int processACommand(replayCtx *ctx, int argc, sds *argv, int *commands)
{
    int cmds = 0; /* how many independent command were processed */
    int j = 0;

    if (!strcasecmp(argv[0], "MSET") || !strcasecmp(argv[0], "MSETNX")) {
        int cmdlen = sdslen(argv[0]) - 1;
        char ** subargv = NULL;
        size_t *subargvlen = NULL;
        subargv = (char **)zmalloc(3 * sizeof(char *));
        subargvlen = (size_t *)zmalloc(3 * sizeof(size_t));

        if(subargv == NULL || subargvlen == NULL) {
            redisLog(REDIS_WARNING, "failed to zmalloc subargv & subargvlen");
            goto error;
        }

        for(j = 1; j < argc; j++) {
            subargv[0] = argv[0] + 1;
            subargv[1] = argv[j++];
            subargv[2] = argv[j];
            subargvlen[0] = cmdlen;
            subargvlen[1] = sdslen(subargv[1]);
            subargvlen[2] = sdslen(subargv[2]);

            if(redisAppendCommandArgv(ctx->redis, 3, (const char **)subargv, subargvlen) != REDIS_OK) {
                redisLog(REDIS_WARNING, "failed to append sub command to redis output buffer");
                goto error;
            }

            ++cmds;
        }
        zfree(subargv);
        zfree(subargvlen);
    } else {
        size_t *argvlen = zmalloc(argc * sizeof(size_t));
        if(argvlen == NULL) {
            redisLog(REDIS_WARNING, "failed to zmalloc argvlen");
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

    *commands += cmds;

    return REDIS_OK;

error:
    return REDIS_ERR;
}

/*
 * 1. get pipe_cmds commands, and put it to output buffer
 * 2. get pipe_cmds commands, and check the return value
 * 3. if any error occurs, record the start position of pipeline and command
 */
void processAppendOnlyFile(replayCtx *ctx)
{
    unsigned long loops = 0;
    int argc = 0;
    int rcmds = 0;      /* rcmds records the processed cmds including sub commands */
    int scmds = 0;      /* scmds records the processed cmds in aof file */ 
    int ncmds = 0;      /* ncmds records the scanned cmds */
    sds * argv = NULL;

    redisLog(REDIS_NOTICE, "start to process aof file: %s", ctx->filename);
    while(1) {
        
        rcmds = 0;
        scmds = 0;
        ncmds = 0;
        refreshAppendOnlyFileStat(ctx, PIPELINE_START);
        while(rcmds < ctx->pipe_cmds) {
            //refreshAppendOnlyFileStat(ctx, COMMAND_START);
            if(getRedisCommand(ctx, &argc, &argv) != REDIS_OK) {
                redisLog(REDIS_WARNING, "wrong format command");
                goto error;
            } else if (argv == NULL) {
                usleep(1000000);
                break;
            }

            ctx->scanned_cmd += 1;
            ncmds += 1;

            if(skipThisCommand(ctx, argc, argv)) {
                ctx->skipped_cmd += 1;
                safeFreeArgvs(argc, argv);
                continue;
            }

            if(processACommand(ctx, argc, argv, &rcmds) != REDIS_OK) {
                redisLog(REDIS_WARNING, "failed to process commands");
                goto error;
            }

            scmds += 1;
            safeFreeArgvs(argc, argv);

            if(ncmds > LOOP_MAX_CMDS) {
                break;
            }
        }

        redisLog(REDIS_VERBOSE, "%d commands sent in this loop", rcmds);

        for(int i = 0; i < rcmds; i++) {
            if(getPipeReply(ctx) != REDIS_OK) {
                redisLog(REDIS_WARNING, "failed to process commands");
                goto error;
            }
        }

        loops += (rcmds + 1);
        ctx->processed_cmd += scmds;

        refreshAppendOnlyFileStat(ctx, NORMAL_START);
        if( (loops >= 10000) || (ctx->diff < REDIS_AOF_COMMAND_THRESHOLD) || ncmds > LOOP_MAX_CMDS ) {
            redisLog(REDIS_NOTICE, "progress: [scanned:%ld] [processed:%ld] [skipped:%ld] [unsupported:%ld] [filesize:%ld] [postion:%ld] [diff:%ld]", \
                ctx->scanned_cmd, ctx->processed_cmd, ctx->skipped_cmd, ctx->unsupported_cmd, (long)ctx->size, (long)ctx->pos, (long)ctx->diff);
            loops = 0;
        }
    }

error:
    safeFreeArgvs(argc, argv);
    redisLog(REDIS_NOTICE, "append only file replay failed at: [pipeline_start_pos:%ld] [command_start_pos:%ld] [file_pos:%ld]", \
            ctx->pipeline_spos, ctx->command_spos, ctx->pos); 
    return;

}

void checkAppendOnlyFile(replayCtx *ctx)
{
    sds *argv = NULL;
    int argc;
    long loops = 0;

    while(1) {
        refreshAppendOnlyFileStat(ctx, COMMAND_START);
        if(getRedisCommand(ctx, &argc, &argv) != REDIS_OK) {
            redisLog(REDIS_WARNING, "wrong format command at position %ld", (long)ctx->command_spos);
            goto error;
        }

        if(argv == NULL) {
            redisLog(REDIS_WARNING, "reach end of file");
            goto end;
        }

        if (!supported(argv[0])) {     
            sds cmd = sdsjoin(argv, argc, " ");
            redisLog(REDIS_NOTICE, "unsupported cmd found: %s", cmd);
            sdsfree(cmd); 
            ctx->unsupported_cmd += 1; 
        } else {
            ctx->processed_cmd += 1;
        }

        loops += 1;
        safeFreeArgvs(argc, argv);     

        if( loops >= 10000 ) {
             redisLog(REDIS_NOTICE, "progress: [processed:%ld] [unsupported:%ld] [filesize:%ld] [postion:%ld] [diff:%ld]", \
                ctx->processed_cmd, ctx->unsupported_cmd, (long)ctx->size, (long)ctx->pos, (long)ctx->diff);
             loops = 0;
        }
    }

error:
    redisLog(REDIS_NOTICE, "progress failed: [processed:%ld] [skipped:%ld] [filesize:%ld] [postion:%ld] [diff:%ld]", \
        ctx->processed_cmd, ctx->skipped_cmd, (long)ctx->size, (long)ctx->pos, (long)ctx->diff);
    return;


end:
    redisLog(REDIS_NOTICE, "progress finished: [processed:%ld] [skipped:%ld] [filesize:%ld] [postion:%ld] [diff:%ld]", \
        ctx->processed_cmd, ctx->skipped_cmd, (long)ctx->size, (long)ctx->pos, (long)ctx->diff);
    return;
}

void freeReplayCtx(replayCtx *ctx)
{
    if(ctx->fp != NULL) {
        fclose(ctx->fp);
    }

    zfree(ctx->buf);
    sdsfree(ctx->filename);
    sdsfree(ctx->ip);
    sdsfree(ctx->lastlen);
    redisFree(ctx->redis);

    for(int i = 0; i < ctx->nfilter; i++) sdsfree(ctx->filter[i]);
    for(int i = 0; i < ctx->nrule; i++) {
        sdsfree(ctx->prefix[i]);
        sdsfree(ctx->replace_prefix[i]);
    }
}

int _get_num(char *ptr, char c)
{
    if(ptr == NULL) return 0;
    
    int num = 0;
    char *tmp = ptr;
    while( *tmp != '\0' ) {
        if(*tmp == c) ++num;
        ++tmp;
    }
    return num;
}

int initFilter(replayCtx *ctx, char *filter)
{
    if(filter == NULL) {
        ctx->nfilter = 0;
        ctx->filter = NULL;
        return REDIS_OK;
    }

    int num = _get_num(filter, ',');

    ctx->nfilter = num + 1;
    ctx->filter = (sds *)zmalloc((num + 1) * sizeof(sds));
    if(ctx->filter == NULL) {
        return REDIS_ERR; 
    }

    memset(ctx->filter, 0, num * sizeof(sds));

    num = 0;
    char *ptr = filter;
    char *pre = ptr;
    do{
        if(*ptr == ',' || *ptr == '\0') {
            if(ptr == pre) return REDIS_ERR;
            ctx->filter[num] = sdsnewlen(pre, (size_t)(ptr - pre));
            ++num;
            pre = ptr + 1;
        }

        if(*ptr == '\0') break;

        ++ptr;
    } while(1);

    return REDIS_OK;
}


int initRule(replayCtx *ctx, char *rule) 
{

    if (rule == NULL) {
        ctx->nrule = 0;
        ctx->prefix = NULL;
        ctx->replace_prefix = NULL;
        return REDIS_OK;
    }

    int num = _get_num(rule, ',');

    ctx->nrule = num + 1;
    ctx->prefix = (sds *)zmalloc((num + 1) * sizeof(sds));
    ctx->replace_prefix = (sds *)zmalloc((num + 1) * sizeof(sds));

    if(ctx->prefix == NULL || ctx->replace_prefix == NULL) {
        return REDIS_ERR;
    }

    memset(ctx->prefix, 0, num * sizeof(sds));
    memset(ctx->replace_prefix, 0, num * sizeof(sds));

    num = 0;
    char *ptr = rule;
    char *pre = ptr;
    char *tmp = NULL;
    do {
        if( *ptr == ',' || *ptr == '\0' ) {
            tmp = strstr(pre, "/");
            if( tmp == NULL ) {
                return REDIS_ERR;
            }

            if(tmp == pre) {
                ctx->prefix[num] = NULL;
            } else {
                ctx->prefix[num] = sdsnewlen(pre, (size_t)(tmp - pre));
            }

            tmp = tmp + 1;
            if(tmp == ptr) {
                return REDIS_ERR;
            }

            ctx->replace_prefix[num] = sdsnewlen(tmp, (size_t)(ptr - tmp));
            pre = ptr + 1;
            ++num;
        }

        if(*ptr == '\0') break;
        ++ptr;
    }while(1);

    int nul = 0;
    for(int i = 0; i < ctx->nrule; i++) {
        if(ctx->prefix[i] == NULL) {
            ++nul;
        }
    }
    if(nul > 1) {
        redisLog(REDIS_WARNING, "%d prefix are NULL", nul);
        return REDIS_ERR;
    }

    return REDIS_OK;
}


replayCtx *initReplayCtx(Config * config)
{

    replayCtx *ctx = (replayCtx*)zmalloc(sizeof(replayCtx));
    if (NULL == ctx) {
        //redisLog(REDIS_WARNING, "Failed to malloc replayCtx");
        redisLog(REDIS_WARNING, "Failed to malloc replayCtx");
        return NULL;
    }

    memset(ctx, 0, sizeof(replayCtx));

    ctx->filename = sdsnew(config->filename);
    ctx->fp = fopen(ctx->filename, "r+");
    if (ctx->fp == NULL) {
        //redisLog(REDIS_WARNING, "Cannot open file: %s\n", filename);
        redisLog(REDIS_WARNING, "Cannot open file: %s", config->filename);
        goto error;
    }

    /* set large buffer for file IO */
    ctx->buf = (void *)zmalloc(1024 * 1024);
    setbuf(ctx->fp, ctx->buf);

    if(config->host != NULL) {
        /* ip && port of destination */
        char *p = config->host;
        char *s = strchr(p, ':');
        *s = '\0';
        ctx->ip = sdsnew(p);
        ctx->port = atoi(s+1);
        ctx->redis = redisConnect(ctx->ip, ctx->port);
        if (ctx->redis->err) {
            redisLog(REDIS_WARNING, "failed to connect to %s:%d", ctx->ip, ctx->port);
            goto error;
        }
    }

    ctx->processed_cmd = 0;
    ctx->skipped_cmd = 0;
    ctx->unsupported_cmd = 0;
    ctx->scanned_cmd = 0;

    ctx->pipeline_spos = 0;
    ctx->command_spos = 0;
    ctx->lastlen = sdsnewlen(NULL, MAX_LONG_LEN);

    ctx->pipe_cmds = config->pipe_cmds;

    if(initFilter(ctx, config->filter) != REDIS_OK) goto error;
    if(initRule(ctx, config->rule) != REDIS_OK) goto error;

    refreshAppendOnlyFileStat(ctx, NORMAL_START);

    return ctx;

error:
    freeReplayCtx(ctx);
    return NULL;

}

void parseArgs(int argc, char **argv, Config *config)
{
    redisLog(REDIS_WARNING, "parsing arguments");

    config->pipe_cmds = REPLAY_DEFAULT_PIPE_CMDS;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--file") == 0) {
            config->filename = argv[i+1];
            ++i;
        } else if (strcmp(argv[i], "--dest") == 0) {
            config->host = argv[i+1];
            ++i;
        } else if (strcmp(argv[i], "--rule") == 0) {
            config->rule = argv[i+1];
            ++i;
        } else if (strcmp(argv[i], "--pipe_cmds") == 0) {
            config->pipe_cmds = atoi(argv[i+1]);
            ++i;
        } else if (strcmp(argv[i], "--filter") == 0) {
            config->filter = argv[i+1];
            ++i;
        } else if (strcmp(argv[i], "--verbose") == 0 || strcmp(argv[i], "-v") == 0) {
            config->verbose = 1;
        } else if (strcmp(argv[i], "--only_check") == 0) {
            config->only_check = 1;
        } else if (strcmp(argv[i], "--logfile") == 0) {
            config->logfile = argv[i+1];
            ++i;
        }
    }
}

void usage(char *argv) {
    fprintf(stderr, "Usage: %s\n", argv);
    fprintf(stderr, "\t\t--file <file.aof>: aof file\n");
    fprintf(stderr, "\t\t--dest <ip:port>: the target ip & port\n");
    fprintf(stderr, "\t\t[--filter <filter>|--rule <rule>]: key prefix filter or replacement rule, oldkey1/newkey1,oldkey2/newkey2\n");
    fprintf(stderr, "\t\t[--pipe_cmds <cmds>]: cmds number for pipeline\n");
    fprintf(stderr, "\t\t[--only_check]: only do check\n");
    fprintf(stderr, "\t\t[--logfile <filename>]: logfile\n");
    fprintf(stderr, "\t\t[-v|--verbose]\n");
    exit(1);
}

int main(int argc, char **argv) {

    replayCtx * ctx = NULL;
    if (argc < 4) {
        usage(argv[0]);
    } 

    parseArgs(argc, argv, &conf);
    if (conf.filename == NULL) {
        usage(argv[0]);
    }

    if(conf.host == NULL && conf.only_check != 1) {
        usage(argv[0]);
    }

    if(conf.filter != NULL && conf.rule != NULL) {
        usage(argv[0]);
    }

    if(conf.verbose) {
        loglevel = REDIS_VERBOSE;
    }

    ctx = initReplayCtx(&conf);
    if(ctx == NULL) {
        redisLog(REDIS_WARNING, "failed to init replay context");
        exit(1);
    }

    if(conf.only_check) {
        checkAppendOnlyFile(ctx);
    } else {
        processAppendOnlyFile(ctx);
    }

    return 0;
}
