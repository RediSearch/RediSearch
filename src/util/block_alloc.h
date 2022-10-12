#pragma once

#include <stdlib.h>
#include <list>
#include <utility>
#include "rmutil/vector.h"

///////////////////////////////////////////////////////////////////////////////////////////////

// Foo *foo = pool.Alloc(Foo{"sdF", 3})
// Foo *foo = new (pool) Foo{"sdF", 3};

/*
struct BlkAllocObject {
  void *operator new(Pool *, size_t size);
}
struct Foo : BlkAllocObject {
  Foo(char *p, size_t n);
};
*/

//---------------------------------------------------------------------------------------------

struct BlkAllocBase {
  struct Block {
    struct Block *next;
    size_t numUsed;
    size_t capacity;
    char data[0] __attribute__((aligned(16)));
  };

  size_t block_size;
  Block *root;
  Block *last;

  // Available blocks - used when recycling the allocator
  Block* avail;

  BlkAllocBase(size_t blockSize) : block_size(blockSize), root(NULL), last(NULL), avail(NULL) {}
  ~BlkAllocBase();

  virtual void Clear();

protected:
  struct Block *getNewBlock();
};

//---------------------------------------------------------------------------------------------

template <class T>
struct BlkAlloc {
  BlkAlloc(size_t numElem) : num_elem(numElem) {}

  size_t num_elem;
  std::list<Vector<T>> used, avail;

  T *Alloc(T &&obj) {
    if (used.empty() || used.back().size() == used.back().capacity()) {
      used.emplace_back(Vector<T>{});
      used.back().reserve(num_elem);
    }

    used.back().emplace_back(std::move(obj));
    return &used.back().back();
  }

  void Clear() {
    for (auto &v: used) {
      v.clear();
    }
  }
};

//---------------------------------------------------------------------------------------------

struct StringBlkAlloc : BlkAllocBase {
  StringBlkAlloc(size_t blockSize) : BlkAllocBase(blockSize) {}

  // gets non-null terminated string, copies into pool, returns pointer from pool
  char *strncpy(const char *str, size_t len);
  char *strncpy(std::string_view str) { return strncpy(str.data(), str.size()); }
};

///////////////////////////////////////////////////////////////////////////////////////////////

#if 0

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


template <class T>
struct DumbBlockPool {

  struct Block {
    struct Block *next;
    size_t used;
    size_t capacity;
    char data[0] __attribute__((aligned(16)));

	  Block(size_t items) : used(0), capacity(itms), next(NULL) {}

    void reset() {
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
