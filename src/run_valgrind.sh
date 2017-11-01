#!/bin/sh

valgrind --tool=memcheck --leak-check=full --show-leak-kinds=definite --suppressions=leakcheck.supp redis-server redis.conf
