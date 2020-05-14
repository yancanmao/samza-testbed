#!/usr/bin/env bash
APP_DIR="$(dirname $(pwd))"

IS_COMPILE=$1
HOST=$2
APP=$3

function clearEnv() {
#    ~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic auctions
    #~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic persons
    #~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic nexmark-q8-1-join-join-R
    #~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic nexmark-q8-1-join-join-L
    #~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic nexmark-q8-1-partition_by-auction
    #~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic nexmark-q8-1-partition_by-person
    #~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic __samza_coordinator_nexmark-q8_1
    #~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --delete --zookeeper ${HOST}:2181 --topic results
    #python -c 'import time; time.sleep(60)'

    export JAVA_HOME=/home/samza/kit/jdk
    ~/samza-hello-samza/bin/grid stop kafka
    ~/samza-hello-samza/bin/grid stop zookeeper
    rm -r /data/kafka/kafka-logs/
    rm -r /tmp/zookeeper/

    python -c 'import time; time.sleep(20)'

    ~/samza-hello-samza/bin/grid start zookeeper
    ~/samza-hello-samza/bin/grid start kafka

    ~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --zookeeper ${HOST}:2181 --create --topic auctions --partitions 64 --replication-factor 1  --config message.timestamp.type=LogAppendTime
    ~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --zookeeper ${HOST}:2181 --create --topic persons --partitions 64 --replication-factor 1 --config message.timestamp.type=LogAppendTime
}

function configApp() {
    echo $nOE $L $T $l;
    #Modify applicaiton config
    cp ${APP_DIR}/testbed_1.0.0/target/config/nexmark-q${APP}.properties properties.t1
    awk -F"=" 'BEGIN{OFS=FS} $1=="job.container.count"{$2='"$nOE"'}1' properties.t1 > properties.t2
    awk -F"=" 'BEGIN{OFS=FS} $1=="streamswitch.requirement.window"{$2='"$((T*1000))"'}1' properties.t2 > properties.t1
    awk -F"=" 'BEGIN{OFS=FS} $1=="streamswitch.system.l="{$2='"$l"'}1' properties.t1 > properties.t2
    awk -F"=" 'BEGIN{OFS=FS} $1=="streamswitch.requirement.latency"{$2='"$((L*1000))"'}1' properties.t2 > properties.t1
    rm properties.t2
    mv properties.t1 ${APP_DIR}/testbed_1.0.0/target/config/nexmark-q${APP}.properties
    sed -i -- 's/localhost/'${HOST}'/g' ${APP_DIR}/testbed_1.0.0/target/config/nexmark-q${APP}.properties
#    awk -F"=" 'BEGIN{OFS=FS} $1=="job.id"{$2=$2+1}1' ${APP_DIR}/testbed_1.0.0/target/config/nexmark-q${APP}.properties > properties.tmp
#    mv properties.tmp ${APP_DIR}/testbed_1.0.0/target/config/nexmark-q${APP}.properties
}

function configAppStatic() {
    sed -i -- 's/localhost/'${HOST}'/g' ${APP_DIR}/testbed_1.0.0/target/config/nexmark-q${APP}-static.properties
    awk -F"=" 'BEGIN{OFS=FS} $1=="job.id"{$2=$2+1}1' ${APP_DIR}/testbed_1.0.0/target/config/nexmark-q${APP}-static.properties > properties.tmp
    mv properties.tmp ${APP_DIR}/testbed_1.0.0/target/config/nexmark-q${APP}-static.properties
}

function compile() {
    cd ${APP_DIR}/testbed_1.0.0/
    mvn clean package
    cd target
    tar -zvxf *-dist.tar.gz
    cd ${APP_DIR}
}

function uploadHDFS() {
    ~/samza-hello-samza/deploy/yarn/bin/hdfs dfs -rm  hdfs://${HOST}:9000/testbed-myc/*-dist.tar.gz
    ~/samza-hello-samza/deploy/yarn/bin/hdfs dfs -mkdir hdfs://${HOST}:9000/testbed-myc
    ~/samza-hello-samza/deploy/yarn/bin/hdfs dfs -put  ${APP_DIR}/testbed_1.0.0/target/*-dist.tar.gz hdfs://${HOST}:9000/testbed-myc
}

function compileGenerator() {
    cd ${APP_DIR}/kafka_producer/
    mvn clean package
    cd ${APP_DIR}
}

function generateAuction() {
    java -cp ${APP_DIR}/kafka_producer/target/kafka_producer-0.0.1-jar-with-dependencies.jar kafka.Nexmark.KafkaAuctionGenerator \
        -host $BROKER -topic auctions -rate $RATE -cycle $CYCLE -base $BASE &
}

function generateBid() {
    java -cp ${APP_DIR}/kafka_producer/target/kafka_producer-0.0.1-jar-with-dependencies.jar kafka.Nexmark.KafkaBidGenerator \
        -host $BROKER -topic bids -rate $RATE -cycle $CYCLE -base $BASE &
}

function generatePerson() {
    java -cp ${APP_DIR}/kafka_producer/target/kafka_producer-0.0.1-jar-with-dependencies.jar kafka.Nexmark.KafkaPersonGenerator \
        -host $BROKER -topic persons -rate $RATE -cycle $CYCLE -base $BASE &
}

function runApp() {
    OUTPUT=`${APP_DIR}/testbed_1.0.0/target/bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory \
    --config-path=file://${APP_DIR}/testbed_1.0.0/target/config/nexmark-q${APP}.properties | grep 'application_.*$'`
    app=`[[ ${OUTPUT} =~ application_[0-9]*_[0-9]* ]] && echo $BASH_REMATCH`
    appid=${app#application_}
    echo "assigned app id is: $appid"
}

function runAppStatic() {
    OUTPUT=`${APP_DIR}/testbed_1.0.0/target/bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory \
    --config-path=file://${APP_DIR}/testbed_1.0.0/target/config/nexmark-q${APP}-static.properties | grep 'application_.*$'`
    app=`[[ ${OUTPUT} =~ application_[0-9]*_[0-9]* ]] && echo $BASH_REMATCH`
    appid=${app#application_}
    echo "assigned app id is: $appid"
}

function killApp() {
    ~/cluster/yarn/bin/yarn application -kill $app
    kill -9 $(jps | grep Generator | awk '{print $1}')
}

function runExp() {
    clearEnv
    configApp
    runApp
    #configAppStatic
    #runAppStatic

    # wait for app start
    python -c 'import time; time.sleep(10)'

    BROKER=${HOST}:9092
#        RATE=0
#            CYCLE=400
    BASE=$(expr $TOTALRATE - $RATE)

    if [[ ${APP} == 1 ]]
    then
        generateBid
    elif [[ ${APP} == 8 ]]
    then
        generateAuction
        generatePerson
    fi

    # run 120s
    python -c 'import time; time.sleep(800)'
    killApp

    EXP_NAME=${nOE}_L${L}T${T}I${l}_B${BASE}C${CYCLE}R${RATE}_APP${appid}

    localDir="${APP_DIR}/nexmark_scripts/draw/GroundTruth/${EXP_NAME}"
    figDir="${APP_DIR}/nexmark_scripts/draw/figures/${EXP_NAME}"
    mkdir ${figDir}
    bash ${APP_DIR}/nexmark_scripts/runCpr.sh ${appid} ${localDir}

    cd ${APP_DIR}/nexmark_scripts/draw
    python2 RateAndWindowDelay.py ${EXP_NAME}
    python2 ViolationsAndUsageFromGroundTruth.py ${EXP_NAME}
}



if [ ${IS_COMPILE} == 1 ]
then
    compile
    compileGenerator
    uploadHDFS
fi

T=1000
TOTALRATE=5000
nOE=10
CYCLE=400
for L in 1000; do
    for l in 100; do # 1 5 10 20 30 40 50
        for RATE in 2500; do # 500 1000 200 3000 4000 5000
#            for CYCLE in 50 100 200 400 800; do
            runExp
#            done
        done
    done
done