#!/bin/bash

for j in `seq 20`;do
    for i in 48 96 192 384; do
    rm -rf work/kafka_2.11-0.10.1.0* work/kafka_started
    saga-hadoop --resource=slurm://localhost --queue=normal --walltime=120 --number_cores=$i --project=TG-MCB090174 --framework spark
    sleep 1
    scancel -u tg804093
    
done
done


