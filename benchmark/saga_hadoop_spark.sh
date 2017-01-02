#!/bin/bash

for j in `seq 20`;do
    for i in 48 96 192 384 768; do
    rm -rf work/spark_master work/spark_started 
    saga-hadoop --resource=slurm://localhost --queue=normal --walltime=120 --number_cores=$i --project=TG-MCB090174 --framework spark
    sleep 1
    scancel -u tg804093
    
done
done


