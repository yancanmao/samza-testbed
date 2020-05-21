#!/bin/bash
AVGRATE=30000
for cycle in 300 ; do
        for range in 0; do
            for times in 1; do
		        base=`expr ${AVGRATE} - ${range}`
                echo $cycle $base $range
                ./runQuery-auto.sh 1 dragon 2 $cycle $base $range
            done
    done
done
