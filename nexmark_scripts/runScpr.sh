#!/bin/sh

trap "exit 0" SIGINT SIGTERM;

runCmd()
{
    appid="${1}"
    localPath="${2}"
    end=3000
    mkdir -p ${localPath}
    for i in $(seq 1 $end); do
        for HOST in dragon eagle; do
            container=$(printf %06d $i)
            path=${HOST}":~/cluster/yarn/logs/userlogs/application_${appid}/container_${appid}_01_${container}/stdout"
            scp ${path} "${localPath}/${container}.txt"
            trap 
        done
    done
    return 0
}
    
    runCmd "$@"
    echo "return from runCmd."
