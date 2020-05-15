#!/bin/bash
for cycle in 300; do
    for base in 40000 50000 60000; do
        for range in 0; do
            for times in 1; do
                echo $cycle $base $range
                ./runQuery-auto.sh 1 eagle 2 $cycle $base $range
            done
        done
    done
done
