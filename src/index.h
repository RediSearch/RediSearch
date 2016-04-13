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

SkipEntry *SkipIndex_Find(SkipIndex *idx, t_docId docId, u_int offset);


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

IndexReader *NewIndexReader(void *data, size_t datalen, SkipIndex *si); 
int IR_Read(IndexReader *ir, IndexHit *e);
int IR_Next(IndexReader *ir);
int IR_SkipTo(IndexReader *ir, u_int32_t docId, IndexHit *hit);
int IR_Intersect(IndexReader *r, IndexReader *other);
void IR_Seek(IndexReader *ir, t_offset offset, t_docId docId);
void IW_MakeSkipIndex(IndexWriter *iw, int step);

size_t IW_Close(IndexWriter *w); 
void IW_Write(IndexWriter *w, IndexHit *e); 
size_t IW_Len(IndexWriter *w);
void IW_Free(IndexWriter *w);
IndexWriter *NewIndexWriter(size_t cap);


 
#endif