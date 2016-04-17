#ifndef __INDEX_H__
#define __INDEX_H__

#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include "varint.h"

typedef u_int32_t t_docId;
typedef u_int32_t t_offset;
 

typedef struct {
    t_docId docId;
    t_offset offset;
} SkipEntry;

typedef struct {
    u_int len;
    SkipEntry *entries;
} SkipIndex;

SkipEntry *SkipIndex_Find(SkipIndex *idx, t_docId docId, u_int *offset);


typedef struct {
    t_docId docId;
    u_int16_t len; 
    u_int16_t freq;
    u_char flags;
    VarintVector *offsets; 
} IndexHit;

typedef struct  {
    t_offset size;
} IndexHeader;

typedef struct {
    IndexHeader *header;
    u_char *data;
    t_offset datalen;
    u_char *pos;
    t_docId lastId;
    
    SkipIndex skipIdx;
    u_int skipIdxPos;
    
} IndexReader; 



typedef struct {
    u_char *buf;
    size_t cap;
    u_char *pos;
    
    t_docId lastId;
    u_int32_t ndocs;
    SkipIndex skipIdx;
} IndexWriter;


#define INDEXREAD_EOF 0
#define INDEXREAD_OK 1
#define INDEXREAD_NOTFOUND 2

#define L_DEBUG 1
#define L_INFO 2
#define L_WARN 4
#define L_ERROR 8


#define LOGGING_LEVEL  0 
//L_DEBUG | L_INFO



#define LG_MSG(...) fprintf(stdout, __VA_ARGS__)
#define LG_DEBUG(...) if (LOGGING_LEVEL & L_DEBUG) { LG_MSG("[DEBUG %s:%d] ", __FILE__ , __LINE__); LG_MSG(__VA_ARGS__); }
#define LG_INFO(...) if (LOGGING_LEVEL & L_INFO) { LG_MSG("[INFO %s:%d] ", __FILE__ , __LINE__); LG_MSG(__VA_ARGS__); }
#define LG_WARN(...) if (LOGGING_LEVEL & L_WARN) { LG_MSG("[WARNING %s:%d] ", __FILE__ , __LINE__); LG_MSG(__VA_ARGS__); }
#define LG_ERROR(...) if (LOGGING_LEVEL & L_ERROR) { LG_MSG("[ERROR %s:%d] ", __FILE__ , __LINE__); LG_MSG(__VA_ARGS__); }


typedef int (*IntersectHandler)(void *ctx, IndexHit*, int);

typedef struct {
    void *ctx;
    int (*Read)(void *ctx, IndexHit *e);
    int (*SkipTo)(void *ctx, u_int32_t docId, IndexHit *hit);
    t_docId (*LastDocId)(void *ctx);
} IndexIterator;
void IndexIterator_Free(IndexIterator *it);

IndexReader *NewIndexReader(void *data, size_t datalen, SkipIndex *si); 
int IR_Read(void *ctx, IndexHit *e);
int IR_Next(void *);
int IR_SkipTo(void *, u_int32_t docId, IndexHit *hit);
t_docId IR_LastDocId(void* ctx);
int IR_Intersect(IndexReader *r, IndexReader *other, IntersectHandler h, void *ctx);
int IR_Intersect2(IndexIterator **argv, int argc, IntersectHandler onIntersect, void *ctx);
void IR_Seek(IndexReader *ir, t_offset offset, t_docId docId);
void IW_MakeSkipIndex(IndexWriter *iw, int step);

IndexIterator *NewIndexTerator(IndexReader *ir);

size_t IW_Close(IndexWriter *w); 
void IW_Write(IndexWriter *w, IndexHit *e); 
size_t IW_Len(IndexWriter *w);
void IW_Free(IndexWriter *w);
IndexWriter *NewIndexWriter(size_t cap);


typedef struct {
    IndexIterator **its;
    int num;
    int pos;
    t_docId minDocId;
} UnionContext;

IndexIterator *NewUnionIterator(IndexIterator **its, int num);

int UI_SkipTo(void *ctx, u_int32_t docId, IndexHit *hit); 
int UI_Next(void *ctx);
int UI_Read(void *ctx, IndexHit *hit);
t_docId UI_LastDocId(void *ctx);
 
#endif