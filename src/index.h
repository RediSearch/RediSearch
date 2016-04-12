#ifndef __INDEX_H__
#define __INDEX_H__

#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include "varint.h"



typedef struct {
    u_int32_t docId;
    u_int16_t len; 
    u_int16_t freq;
    u_char flags;
    VarintVector *offsets; 
} IndexHit;

typedef struct  {
    u_int32_t size;
} IndexHeader;

typedef struct {
    IndexHeader *header;
    u_char *data;
    size_t datalen;
    u_char *pos;
    u_int32_t lastId;
    
} IndexReader; 

typedef struct {
    u_char *buf;
    size_t cap;
    u_char *pos;
    
    u_int32_t lastId;
    u_int32_t ndocs;
    
} IndexWriter;



#define INDEXREAD_EOF 0
#define INDEXREAD_OK 1

IndexReader *NewIndexReader(void *data, size_t datalen); 
int IR_Read(IndexReader *ir, IndexHit *e);
int IR_Next(IndexReader *ir);

size_t IW_Close(IndexWriter *w); 
void IW_Write(IndexWriter *w, IndexHit *e); 
size_t IW_Len(IndexWriter *w);
IndexWriter *NewIndexWriter(size_t cap);


 
#endif