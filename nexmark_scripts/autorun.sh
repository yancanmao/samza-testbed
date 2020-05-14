#!/bin/bash
for cycle in 300 200 150
do
    for base in 5000
    do
        for range in 10000
        do
            for times in {1..5}
            do
                echo $cycle $base $range
                ./runQuery-auto.sh 1 dragon 8 $cycle $base $range
            done
        done
    done
done
