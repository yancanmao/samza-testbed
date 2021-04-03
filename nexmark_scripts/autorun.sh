#!/bin/bash
AVGRATE=30000
for cycle in 150; do # 150; do # 200 300; do
        for range in 20000; do # 20000; do
            for times in {1..1}; do
		base=`expr ${AVGRATE} - ${range}`
                echo $cycle $base $range
                ./runQuery-auto.sh 1 dragon 2 $cycle $base $range
#                ./runQuery-auto.sh 0 flamingo 2 $cycle $base $range
            done
    done
done
