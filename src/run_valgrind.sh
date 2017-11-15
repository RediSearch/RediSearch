#!/bin/sh
if [ -z "$REDIS_PATH" ]; then
    REDIS_PATH="redis-server"
fi

valgrind --tool=memcheck --leak-check=full --show-leak-kinds=definite --suppressions=leakcheck.supp $REDIS_PATH redis.conf
