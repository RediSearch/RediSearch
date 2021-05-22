#pragma once

#include <stdlib.h>

///////////////////////////////////////////////////////////////////////////////////////////////

struct BlkAlloc {
  struct Block {
    struct Block *next;
    size_t numUsed;
    size_t capacity;
    char data[0] __attribute__((aligned(16)));
  };

  Block *root;
  Block *last;

  // Available blocks - used when recycling the allocator
  Block *avail;

  BlkAlloc() : root(NULL), last(NULL), avail(NULL) {}
  ~BlkAlloc() { FreeAll(NULL, NULL, 0); }

  // Allocate a new element from the block allocator.
  // A pointer of size elemSize will be returned. 
  // blockSize is the size of the new block to be created (if the current block has no more 
  // room for elemSize).
  // blockSize should be greater than elemSize, and should likely be a multiple thereof.
  //
  // The returned pointer remains valid until FreeAll is called.

  void *Alloc(size_t elemSize, size_t blockSize);

  typedef void (*Cleaner)(void *ptr, void *arg);

  // Free all memory allocated by the allocator.
  // If a cleaner function is called, it will be called for each element.
  // Elements are assumed to be elemSize spaces apart from each other.

  void FreeAll(Cleaner cleaner, void *arg, size_t elemSize);

  // Like FreeAll, except the blocks are recycled and placed inside the 'avail' pool instead.

  void Clear(Cleaner cleaner, void *arg, size_t elemSize);

protected:
  void freeCommon(Cleaner cleaner, void *arg, size_t elemSize, bool reuse);
};

///////////////////////////////////////////////////////////////////////////////////////////////

#if 0

template <class T>
struct DumbBlockPool {

  struct Block {
    struct Block *next;
    size_t used;
    size_t capacity;
    char data[0] __attribute__((aligned(16)));
	
	Block(size_t items) : used(0), capacity(itms), next(NULL) {}

    void reset() } {
      used = 0;
	  next = NULL;
    }

    void* operator new(std::size_t sz) { return rm_calloc(1, sizeof(Block) + sz * sizeof(T)); }
    void operator delete(void *p) { rm_free(p); }
  };

  Block *root;
  Block *last;
  Block *avail;

  DumbBlockPool() : root(NULL), last(NULL), avail(NULL) {}
  DumbBlockPool() { FreeAll(); }

  virtual T *Alloc(size_t items) {
    if (!root) {
      root = last = newBlock(items);
    } else if (last->used == items) {
      Block *block = newBlock(items);
      last->next = block;
      last = block;
    }
    return reinterpret_cast<T*>(last->data + last->used++ * sizeof(T));
  }

  virtual void FreeAll(bool recycle = false) {
    Block *cur = root;
    while (cur) {
      Block *curNext = cur->next;
      if (recycle) {
        cur->next = avail;
        avail = cur;
      } else {
        delete cur;
      }
      cur = curNext;
    }
  
    if (!recycle && avail) {
      cur = avail;
      while (cur) {
        Block *curNext = cur->next;
        delete cur;
        cur = curNext;
      }
    }
  }

protected:
  Block *newBlock(size_t items) {
    Block *block = NULL, *prev = NULL;
    for (Block *cur = avail; cur; cur = cur->next) {
      if (cur->capacity >= items) {
        block = cur;
        if (cur == avail) {
          avail = cur->next;
        } else {
          assert(prev != NULL);
          prev->next = cur->next;
        }
        break;
      }
      prev = cur;
    }
  
    if (!block) {
	  return new Block(items);
    }
  
    block->reset();
    return block;
  }
};

//---------------------------------------------------------------------------------------------

template <class T>
struct BlockPool : DumpBlockPool<T> {
  typedef DumpBlockPool<T> Super;
  
  BlockPool() : DumpBlockPool<T>() {}
  BlockPool() { FreeAll(); }

  virtual T *Alloc(size_t items) {
	T *p = Super::Alloc(items);
    for (size_t i = 0; i < items; ++i) {
	  new (&p[i]) T();
    }
    return p;
  }

  virtual void FreeAll(bool recycle = false) {
    Block *cur = root;
    while (cur) {
      Block *curNext = cur->next;
      if (recycle) {
        cur->next = avail;
        avail = cur;
      } else {
		((T *) cur)->~T();
        delete cur;
      }
      cur = curNext;
    }
  
    if (!recycle && avail) {
      cur = avail;
      while (cur) {
        Block *curNext = cur->next;
		((T *) cur)->~T();
        delete cur;
        cur = curNext;
      }
    }
  }
};

#endif // 0

///////////////////////////////////////////////////////////////////////////////////////////////
