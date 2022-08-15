
#include "index_result.h"
#include "varint.h"
#include "rmalloc.h"

#include <math.h>
#include <sys/param.h>

///////////////////////////////////////////////////////////////////////////////////////////////

#define __absdelta(x, y) (x > y ? x - y : y - x)

//------------------------------------------------------------------------------`---------------

AggregateResult::AggregateResult(const AggregateResult &src) {
  *this = src;
  isCopy = true;
  // deep copy recursively all children
  for (auto child : src.children) {
    children.push_back(new IndexResult(*child));
  }
}

//------------------------------------------------------------------------------`---------------

void AggregateResult::Print(int depth) const {
  for (int i = 0; i < depth; i++) printf("  ");
  printf("%s => %llu{ \n", type == RSResultType_Intersection ? "Inter" : "Union",
         (unsigned long long)docId);

  for (auto child : children) child->Print(depth + 1);
  for (int i = 0; i < depth; i++) printf("  ");
  printf("},\n");
}

//------------------------------------------------------------------------------`---------------

bool AggregateResult::HasOffsets() const {
  return typeMask != RSResultType_Virtual && typeMask != RSResultType_Numeric;
}

//------------------------------------------------------------------------------`---------------

void AggregateResult::GetMatchedTerms(RSQueryTerm *arr[], size_t cap, size_t &len) {
  if (len == cap) return;
  for (auto child : children) {
    child->GetMatchedTerms(arr, cap, len);
  }
}

//------------------------------------------------------------------------------`---------------

// Reset aggregate result's child vector

void AggregateResult::Reset() {
  IndexResult::Reset();
  children.clear();
  typeMask = (RSResultType) 0;
}

//------------------------------------------------------------------------------`---------------

// Append a child to an aggregate result

void AggregateResult::AddChild(IndexResult *child) {
  // Increase capacity if needed
  children.push_back(child);

  typeMask |= child->type;
  freq += child->freq;
  docId = child->docId;
  fieldMask |= child->fieldMask;
}

//------------------------------------------------------------------------------`---------------

// Test the result offset vectors to see if they fall within a max "slop" or distance between the
// terms. That is the total number of non matched offsets between the terms is no bigger than
// maxSlop. e.g. for an exact match, the slop allowed is 0.

bool AggregateResult::IsWithinRange(int maxSlop, bool inOrder) const {
  // check if calculation is even relevant here...
  int num = children.size();

  if (num <= 1) {
    return true;
  }

  // Fill a list of iterators and the last read positions
  RSOffsetIterators iters(num);
  uint32_t positions[num];
  int n = 0;
  for (int i = 0; i < num; i++) {
    // collect only iterators for nodes that can have offsets
    auto &child = *children[i];
    if (child.HasOffsets()) {
      iters.emplace_back(child.IterateOffsets());
      positions[n] = 0;
      n++;
    }
  }

  // No applicable offset children - just return true
  if (n == 0) {
    return true;
  }

  int rc;
  // cal the relevant algorithm based on ordered/unordered condition
  if (inOrder)
    rc = withinRangeInOrder(iters, positions, n, maxSlop);
  else
    rc = withinRangeUnordered(iters, positions, n, maxSlop);

  return !!rc;
}

//----------------------------------------------------------------------------------------------

// Find the minimal distance between members of the vectos.
// e.g. if V1 is {2,4,8} and V2 is {0,5,12}, the distance is 1 - abs(4-5)
// @param vs a list of vector pointers
// @param num the size of the list

int AggregateResult::MinOffsetDelta() const {
  int dist = 0;
  int num = children.size();

  if (num <= 1) {
    return 1;
  }

  std::unique_ptr<RSOffsetIterator> v1, v2;
  int i = 0;
  while (i < num) {
    // if either
    while (i < num && !children[i]->HasOffsets()) {
      i++;
      continue;
    }
    if (i == num) break;
    v1 = children[i]->IterateOffsets();
    i++;

    while (i < num && !children[i]->HasOffsets()) {
      i++;
      continue;
    }
    if (i == num) {
      dist = dist ? dist : 100;
      break;
    }
    v2 = children[i]->IterateOffsets();

    uint32_t p1 = v1->Next(NULL);
    uint32_t p2 = v2->Next(NULL);
    int cd = __absdelta(p2, p1);
    while (cd > 1 && p1 != RS_OFFSETVECTOR_EOF && p2 != RS_OFFSETVECTOR_EOF) {
      cd = MIN(__absdelta(p2, p1), cd);
      if (p2 > p1) {
        p1 = v1->Next(NULL);
      } else {
        p2 = v2->Next(NULL);
      }
    }

    dist += cd * cd;
  }

  // we return 1 if ditance could not be calculate, to avoid division by zero
  return dist ? sqrt(dist) : children.size() - 1;
}

//---------------------------------------------------------------------------------------------

TermResult::TermResult(const TermResult &src) {
  *this = src;
  isCopy = true;

  // copy the offset vectors
  if (src.offsets.data) {
    offsets.data = rm_malloc(offsets.len);
    memcpy(offsets.data, src.offsets.data, offsets.len);
  }
}

//---------------------------------------------------------------------------------------------

TermResult::~TermResult() {
  if (isCopy) {
    rm_free(offsets.data);
  } else {  // non copy result...
    // we only free up terms for non copy results
    if (term != NULL) {
      delete term;
    }
  }
}

//---------------------------------------------------------------------------------------------

void TermResult::Print(int depth) const {
  for (int i = 0; i < depth; i++) printf("  ");

  printf("Term{%llu: %s},\n", (unsigned long long)docId,
         term ? term->str : "nil");
}

//---------------------------------------------------------------------------------------------

bool TermResult::HasOffsets() const {
  return offsets.len > 0;
}

//---------------------------------------------------------------------------------------------

void TermResult::GetMatchedTerms(RSQueryTerm *arr[], size_t cap, size_t &len) {
  if (len == cap) return;
  if (term) {
    // make sure we have a term string and it's not an expansion
    if (term->str.length() > 0) {
      arr[len++] = term;
    }
  }
}

//---------------------------------------------------------------------------------------------

void NumericResult::Print(int depth) const {
  for (int i = 0; i < depth; i++) printf("  ");
  printf("Numeric{%llu:%f},\n", (unsigned long long)docId, value);
}

//---------------------------------------------------------------------------------------------

void VirtualResult::Print(int depth) const {
  for (int i = 0; i < depth; i++) printf("  ");
  printf("Virtual{%llu},\n", (unsigned long long)docId);
}

//---------------------------------------------------------------------------------------------

RSQueryTerm::RSQueryTerm(const RSToken &tok, int id) : id(id), idf(1.0), flags(tok.flags), str(tok.str) {
}

//---------------------------------------------------------------------------------------------

void IndexResult::Reset() {
  docId = 0;
  fieldMask = 0;
  freq = 0;
}

//---------------------------------------------------------------------------------------------

// Fill an array of max capacity cap with all the matching text terms for the result.
// The number of matching terms is returned.

size_t IndexResult::GetMatchedTerms(RSQueryTerm **arr, size_t cap) {
  size_t arrlen = 0;
  GetMatchedTerms(arr, cap, arrlen);
  return arrlen;
}

//---------------------------------------------------------------------------------------------

bool IndexResult::withinRangeInOrder(RSOffsetIterators &iters, uint32_t *positions, int num,
                                     int maxSlop) {
  while (1) {
    // we start from the beginning, and a span of 0
    int span = 0;
    for (int i = 0; i < num; i++) {
      // take the current position and the position of the previous iterator.
      // For the first iterator we always advance once
      uint32_t pos = i ? positions[i] : iters[i]->Next(NULL);
      uint32_t lastPos = i ? positions[i - 1] : 0;

      // read while we are not in order
      while (pos != RS_OFFSETVECTOR_EOF && pos < lastPos) {
        pos = iters[i]->Next(NULL);
      }

      // we've read through the entire list and it's not in order relative to the last pos
      if (pos == RS_OFFSETVECTOR_EOF) {
        return 0;
      }
      positions[i] = pos;

      // add the diff from the last pos to the total span
      if (i > 0) {
        span += ((int)pos - (int)lastPos - 1);
        // if we are already out of slop - just quit
        if (span > maxSlop) {
          break;
        }
      }
    }

    if (span <= maxSlop) {
      return true;
    }
  }

  return false;
}

//---------------------------------------------------------------------------------------------

static inline uint32_t _arrayMin(uint32_t *arr, int len, uint32_t *pos) {
  int m = arr[0];
  *pos = 0;
  for (int i = 1; i < len; i++) {
    if (arr[i] < m) {
      m = arr[i];
      *pos = i;
    }
  }
  return m;
}

//---------------------------------------------------------------------------------------------

static inline uint32_t _arrayMax(uint32_t *arr, int len, uint32_t *pos) {
  int m = arr[0];
  *pos = 0;
  for (int i = 1; i < len; i++) {
    if (arr[i] >= m) {
      m = arr[i];
      *pos = i;
    }
  }
  return m;
}

//---------------------------------------------------------------------------------------------

// Check the index result for maximal slop, in an unordered fashion.
// The algorithm is simple - we find the first offsets min and max such that max-min<=maxSlop
bool IndexResult::withinRangeUnordered(RSOffsetIterators &iters, uint32_t *positions, int num,
                                       int maxSlop) {
  for (int i = 0; i < num; i++) {
    positions[i] = iters[i]->Next(NULL);
  }
  uint32_t minPos, maxPos, min, max;
  // find the max member
  max = _arrayMax(positions, num, &maxPos);

  while (1) {

    // we start from the beginning, and a span of 0
    min = _arrayMin(positions, num, &minPos);
    if (min != max) {
      // calculate max - min
      int span = (int)max - (int)min - (num - 1);
      // if it matches the condition - just return success
      if (span <= maxSlop) {
        return true;
      }
    }

    // if we are not meeting the conditions - advance the minimal iterator
    positions[minPos] = iters[minPos]->Next(NULL);
    // If the minimal iterator is larger than the max iterator, the minimal iterator is the new
    // maximal iterator.
    if (positions[minPos] != RS_OFFSETVECTOR_EOF && positions[minPos] > max) {
      maxPos = minPos;
      max = positions[maxPos];

    } else if (positions[minPos] == RS_OFFSETVECTOR_EOF) {
      // this means we've reached the end
      break;
    }
  }

  return false;
}

///////////////////////////////////////////////////////////////////////////////////////////////
