while IFS="" read -r p || [ -n "$p" ]
do
    echo "$p"
    python2 draw/RateAndWindowDelay.py $p
    python2 draw/ViolationsAndUsageFromGroundTruth.py $p
done < inputDirs.txt
