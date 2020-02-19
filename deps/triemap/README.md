# triemap

C implementation of a compact trie lookup map

## Features

* High memory efficiency, fast lookups and insertions
* Deletions with node rejoining
* Prefix lookups with an iterator API
* Random key extraction
* No external dependencies, just one C and one H file

## Basic Example
```c

 TrieMap *tm = NewTrieMap();

 char buf[32];

 for (int i = 0; i < 100; i++) {
    sprintf(buf, "key%d", i);
    TrieMap_Add(tm, buf, strlen(buf), NULL, NULL);
 }
 
  TrieMapIterator *it = TrieMap_Iterate(tm, "key1", 4);
  
  char *str = NULL;
  tm_len_t len = 0;
  void *ptr = NULL;

  /* Prefix Iteration */
  while (TrieMapIterator_Next(it, &str, &len, &ptr)) {
    printf("Found key %.*s\n", (int)len, str);
  }
  
  TrieMapIterator_Free(&it);
  TrieMap_Free(tm, NULL);
  
  ```
  
