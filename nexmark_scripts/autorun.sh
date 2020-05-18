#!/bin/bash
AVGRATE=30000
limit=1.0
for cycle in 300 ; do
        for range in 0; do
            for times in 1; do
		base=`expr ${AVGRATE} - ${range}`
                echo $cycle $base $range
                ./runQuery-auto.sh 0 eagle 2 $cycle $base $range $limit
            done
    done
done
