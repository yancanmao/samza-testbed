#!/bin/bash
AVGRATE=16667
for cycle in 300 ; do
        for range in 4167; do
#            for L in 2000; do
#                for Ls in 500 1000 1500; do
#                    for times in {1..9}; do
#        		        base=`expr ${AVGRATE} - ${range}`
#                        Lc=`expr ${L} - ${Ls}`
#                        echo $cycle $base $range $L $Ls
#                        ./runQuery-auto.sh 1 camel 2 $cycle $base $range $L $Ls $Lc
#                    done
#                done
#           done
            for L in 1000; do
                for Ls in 250 500 750; do
                    for times in {1..9}; do
        		        base=`expr ${AVGRATE} - ${range}`
                        Lc=`expr ${L} - ${Ls}`
                        echo $cycle $base $range $L $Ls
                        ./runQuery-auto.sh 1 camel 2 $cycle $base $range $L $Ls $Lc
                    done
                done
            done 
    done
done
