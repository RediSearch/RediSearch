
#include "aggregate/reducer.h"

///////////////////////////////////////////////////////////////////////////////////////////////

int RDCRToList::Add(const RLookupRow *srcrow) {
  RSValue *v = srcrow->GetItem(srckey);
  if (!v) {
    return 1;
  }
  
  // for non array values we simply add the value to the list
  if (v->t != RSValue_Array) {
    uint64_t hval = v->Hash(0);
    std::string_view hvalstr{(char *)&hval, sizeof(hval)};
    if (data.values.Find(hvalstr) == TRIEMAP_NOTFOUND) {
      data.values.Add(hvalstr, v->MakePersistent()->IncrRef(), NULL);
    }
  } else {  // For array values we add each distinct element to the list
    uint32_t len = v->ArrayLen();
    for (uint32_t i = 0; i < len; i++) {
      RSValue *av = v->ArrayItem(i);
      uint64_t hval = av->Hash(0);
      std::string_view hvalstr{(char *)&hval, sizeof(hval)};
      if (data.values.Find(hvalstr) == TRIEMAP_NOTFOUND) {
        data.values.Add(hvalstr, av->MakePersistent()->IncrRef(), NULL);
      }
    }
  }
  return 1;
}

//---------------------------------------------------------------------------------------------

RSValue *RDCRToList::Finalize() {
  TrieMapIterator *it = data.values.Iterate("");
  char *c;
  tm_len_t l;
  void *ptr;
  RSValue **arr = rm_calloc(data.values.cardinality, sizeof(RSValue));
  size_t i = 0;
  while (it->Next(&c, &l, &ptr)) {
    if (ptr) {
      arr[i++] = ptr;
    }
  }

  RSValue *ret = RSValue::NewArray(arr, i, RSVAL_ARRAY_ALLOC);
  return ret;
}

//---------------------------------------------------------------------------------------------

RDCRToList::RDCRToList(const ReducerOptions *options) {
  if (!options->GetKey(&srckey)) {
    throw Error("RDCRToList: no key found");
  }

  //@@ reducerId = ?;
}

///////////////////////////////////////////////////////////////////////////////////////////////
