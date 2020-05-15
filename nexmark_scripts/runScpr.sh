#!/bin/sh


runCmd()
{
    appid="${1}"
    localPath="${2}"
    end=80
    mkdir -p ${localPath}
    for i in $(seq 1 $end); do
        for HOST in eagle flamingo; do
            container=$(printf %06d $i)
            path=${HOST}":/yarn/logs/userlogs/application_${appid}/container_${appid}_01_${container}/stdout"
            scp ${path} "${localPath}/${container}.txt"
        done
    done
    return 0
}
    
    runCmd "$@"
    echo "return from runCmd."
