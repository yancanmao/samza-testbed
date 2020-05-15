#!/bin/bash
AVGRATE=35000
for cycle in 200 300; do
        for range in 20000; do
            for times in 1; do
		base=`expr ${AVGRATE} - ${range}`
                echo $cycle $base $range
                ./runQuery-auto.sh 1 eagle 2 $cycle $base $range
            done
    done
done
