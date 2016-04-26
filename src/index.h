#ifndef __INDEX_H__
#define __INDEX_H__

#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include "types.h"
#include "varint.h"
#include "forward_index.h"
#include "util/logging.h"
#include "doc_table.h"


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
    double totalFreq;
    u_char flags;
    VarintVector **offsetVecs;
    int numOffsetVecs;
    int hasMetadata;
    DocumentMetadata metadata;
} IndexHit;


void IndexHit_Init(IndexHit *h);
IndexHit NewIndexHit();
void IndexHit_Terminate(IndexHit *h);
int IndexHit_LoadMetadata(IndexHit *h, DocTable *dt);


#pragma pack(4)
typedef struct  {
    t_offset size;
    t_docId lastId;
    u_int32_t numDocs;
} IndexHeader;
#pragma pack()


typedef struct {
    Buffer *buf;
    IndexHeader header;
    t_docId lastId;
    SkipIndex *skipIdx;
    u_int skipIdxPos;
    DocTable *docTable;
} IndexReader; 



typedef struct {
    BufferWriter bw;
    
    t_docId lastId;
    u_int32_t ndocs;
    SkipIndex skipIdx;
} IndexWriter;


#define INDEXREAD_EOF 0
#define INDEXREAD_OK 1
#define INDEXREAD_NOTFOUND 2

typedef int (*IntersectHandler)(void *ctx, IndexHit*, int);

typedef struct indexIterator {
    void *ctx;

    int (*Read)(void *ctx, IndexHit *e);
    int (*SkipTo)(void *ctx, u_int32_t docId, IndexHit *hit);
    t_docId (*LastDocId)(void *ctx);
    int (*HasNext)(void *ctx);
    void (*Free)(struct indexIterator *self);
} IndexIterator;


void UnionIterator_Free(IndexIterator *it);
void IntersectIterator_Free(IndexIterator *it);
void ReadIterator_Free(IndexIterator *it);

IndexReader *NewIndexReader(void *data, size_t datalen, SkipIndex *si, DocTable *docTable);
IndexReader *NewIndexReaderBuf(Buffer *buf, SkipIndex *si, DocTable *docTable);
void IR_Free(IndexReader *ir); 
int IR_GenericRead(IndexReader *ir, t_docId *docId, u_int16_t *freq, u_char *flags, VarintVector *offsets);
int IR_Read(void *ctx, IndexHit *e);
int IR_Next(void *ctx);
int IR_HasNext(void *ctx);
int IR_SkipTo(void *ctx, u_int32_t docId, IndexHit *hit);
u_int32_t IR_NumDocs(IndexReader *ir);
t_docId IR_LastDocId(void* ctx);
int IR_Intersect(IndexReader *r, IndexReader *other, IntersectHandler h, void *ctx);
int IR_Intersect2(IndexIterator **argv, int argc, IntersectHandler onIntersect, void *ctx);
void IR_Seek(IndexReader *ir, t_offset offset, t_docId docId);
void IW_MakeSkipIndex(IndexWriter *iw, int step);
int indexReadHeader(Buffer *b, IndexHeader *h);

IndexIterator *NewIndexIterator(IndexReader *ir);

size_t IW_Close(IndexWriter *w); 
void IW_WriteEntry(IndexWriter *w, ForwardIndexEntry *ent);
size_t IW_Len(IndexWriter *w);
void IW_Free(IndexWriter *w);
IndexWriter *NewIndexWriter(size_t cap);
IndexWriter *NewIndexWriterBuf(BufferWriter bw);


typedef struct {
    IndexIterator **its;
    int num;
    int pos;
    t_docId minDocId;
    IndexHit *currentHits;
    DocTable *docTable;
} UnionContext;

IndexIterator *NewUnionIterator(IndexIterator **its, int num, DocTable *t);

int UI_SkipTo(void *ctx, u_int32_t docId, IndexHit *hit); 
int UI_Next(void *ctx);
int UI_Read(void *ctx, IndexHit *hit);
int UI_HasNext(void *ctx);
t_docId UI_LastDocId(void *ctx);


typedef struct {
    IndexIterator **its;
    int num;
    int exact;
    t_docId lastDocId;
    IndexHit *currentHits;
    DocTable *docTable;
} IntersectContext;

IndexIterator *NewIntersecIterator(IndexIterator **its, int num, int exact, DocTable *t);
int II_SkipTo(void *ctx, u_int32_t docId, IndexHit *hit); 
int II_Next(void *ctx);
int II_Read(void *ctx, IndexHit *hit);
int II_HasNext(void *ctx);
t_docId II_LastDocId(void *ctx);



#endif