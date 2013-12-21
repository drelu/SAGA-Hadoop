#!/bin/bash

rm -rf /tmp/hadoop-`whoami`*
killall -9 java 

for i in `cat $PBS_NODEFILE`; do
    echo "clean ${i}"
    ssh $i rm -rf /tmp/hadoop-`whoami`*
    ssh $i killall -9 java
done
