#!/bin/sh

runCmd()
{
    hosts=`sed -n '/^[^#]/p' ~/experiment_data/hostlist`
    appid="${1}"
    localPath="${2}"
    end=450
    mkdir -p ${localPath}
    for host in $hosts
        do for i in $(seq 1 $end); 
	    do
		container=$(printf %06d $i)
                path="~/experiment_data/${host}/cluster/yarn/logs/userlogs/application_${appid}/container_${appid}_01_${container}/stdout"
		echo "copy from" $path 
                echo HOST $host
                cp $path "${localPath}/${container}.txt"
            done
         done
    return 0
}
    
    runCmd "$@"
    echo "return from runCmd."
