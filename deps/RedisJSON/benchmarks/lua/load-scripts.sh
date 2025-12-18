#!/bin/bash

for i in `ls *.lua`; do 
    echo -n "$i: "; 
    redis-cli SCRIPT LOAD "`cat $i`"; 
done
