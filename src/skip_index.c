#include "skip_index.h"

SkipIndex *NewSkipIndex(Buffer *b) {
  SkipIndex *ret = malloc(sizeof(SkipIndex));

  u_int32_t len = 0;
  BufferSeek(b, 0);
  BufferRead(b, &len, sizeof(len));

  ret->entries = (SkipEntry *)b->pos;
  ret->len = len;
  return ret;
}

void SkipIndex_Free(SkipIndex *si) {
  if (si != NULL) {
    free(si);
  }
}

inline int si_isPos(SkipIndex *idx, u_int i, t_docId docId) {
  if (idx->entries[i].docId < docId && (i < idx->len - 1 && idx->entries[i + 1].docId >= docId)) {
    return 1;
  }
  return 0;
}

inline SkipEntry *SkipIndex_Find(SkipIndex *idx, t_docId docId, u_int *offset) {
  if (idx == NULL || idx->len == 0 || docId < idx->entries[0].docId) {
    return NULL;
  }

  if (si_isPos(idx, *offset, docId)) {
    return NULL;  //&idx->entries[*offset];
  }
  if (docId > idx->entries[idx->len - 1].docId) {
    *offset = idx->len - 1;
    return &idx->entries[idx->len - 1];
  }
  u_int top = idx->len, bottom = *offset;
  u_int i = bottom;
  int newi;

  while (bottom < top) {
    // LG_DEBUG("top %d, bottom: %d idx %d, i %d, docId %d\n", top, bottom,
    // idx->entries[i].docId, i, docId );
    if (si_isPos(idx, i, docId)) {
      // LG_DEBUG("IS POS!\n");
      *offset = i;
      return &idx->entries[i];
    }
    // LG_DEBUG("NOT POS!\n");

    if (docId <= idx->entries[i].docId) {
      top = i;
    } else {
      bottom = i;
    }
    newi = (bottom + top) / 2;
    // LG_DEBUG("top %d, bottom: %d, new i: %d\n", top, bottom, newi);
    if (newi == i) {
      break;
    }
    i = newi;
  }
  // if (i == 0) {
  //     return &idx->entries[0];
  // }
  return NULL;
}
