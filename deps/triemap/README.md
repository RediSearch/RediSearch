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
    tm->Add(buf, strlen(buf), NULL, NULL);
 }

  TrieMapIterator *it = tm->Iterate("key1", 4);

  char *str = NULL;
  tm_len_t len = 0;
  void *ptr = NULL;

  /* Prefix Iteration */
  while (it->Next(&str, &len, &ptr)) {
    printf("Found key %.*s\n", (int)len, str);
  }

  delete it;
  delete tm;

  ```

