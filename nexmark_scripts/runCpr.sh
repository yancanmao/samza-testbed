#!/bin/sh

runCmd()
{
    appid="${1}"
    localPath="${2}"
    end=40
    mkdir -p ${localPath}
    for i in $(seq 1 $end);
    do
        container=$(printf %06d $i)
        path="/home/samza/cluster/yarn/logs/userlogs/application_${appid}/container_${appid}_01_${container}/stdout"
        cp $path "${localPath}/${container}.txt"
    done
    return 0
}
    
    runCmd "$@"
    echo "return from runCmd."
