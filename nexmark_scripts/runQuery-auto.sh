#!/usr/bin/env bash
APP_DIR="$(dirname $(pwd))"

IS_COMPILE=$1
HOST=$2
APP=$3
INPUT_CYCLE=$4
INPUT_BASE=$5
INPUT_RATE=$6
# heterogeneous
#delayGood=$7
#delayBad=$8
#ratioGood=$9
#ratioBad=$10

function clearEnv() {
    export JAVA_HOME=/home/samza/kit/jdk
    /home/samza/samza-hello-samza/bin/grid stop kafka
    /home/samza/samza-hello-samza/bin/grid stop zookeeper
    kill -9 $(jps | grep Kafka | awk '{print $1}')
    python -c 'import time; time.sleep(5)'
    rm -r /tmp/kafka-logs/
    rm -r /tmp/zookeeper/
    /home/samza/samza-hello-samza/bin/grid start zookeeper
    /home/samza/samza-hello-samza/bin/grid start kafka
    python -c 'import time; time.sleep(5)'

    /home/samza/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --zookeeper ${HOST}:2181 --create --topic bids --partitions 64 --replication-factor 1  --config message.timestamp.type=LogAppendTime
}

function configApp() {
    sed -i -- 's/localhost/'${HOST}'/g' ${APP_DIR}/testbed_1.0.0/target/config/nexmark-q${APP}.properties
    cp ${APP_DIR}/testbed_1.0.0/target/config/nexmark-q${APP}.properties properties.t1
    awk -F"=" 'BEGIN{OFS=FS} $1=="task.good.delay"{$2='"$delayGood"'}1' properties.t1 > properties.t2
    awk -F"=" 'BEGIN{OFS=FS} $1=="task.bad.delay"{$2='"$delayBad"'}1' properties.t2 > properties.t1
    awk -F"=" 'BEGIN{OFS=FS} $1=="task.good.ratio"{$2='"$ratioGood"'}1' properties.t1 > properties.t2
    awk -F"=" 'BEGIN{OFS=FS} $1=="task.bad.ratio"{$2='"$ratioBad"'}1' properties.t2 > properties.t1
    rm properties.t2
    mv properties.t1 ${APP_DIR}/testbed_1.0.0/target/config/nexmark-q${APP}.properties
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
    ~/cluster/yarn/bin/hdfs dfs -rm  hdfs://${HOST}:9000/testbed-nexmark/*-dist.tar.gz
    ~/cluster/yarn/bin/hdfs dfs -mkdir hdfs://${HOST}:9000/testbed-nexmark
    ~/cluster/yarn/bin/hdfs dfs -put  ${APP_DIR}/testbed_1.0.0/target/*-dist.tar.gz hdfs://${HOST}:9000/testbed-nexmark
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
}

function killGenerator() {
    kill -9 $(jps | grep Generator | awk '{print $1}')
}

if [ ${IS_COMPILE} == 1 ]
then
    compile
    compileGenerator
    uploadHDFS
fi

delayGood=240000
delayBad=480000
ratioGood=1
ratioBad=0

for delayGood in 720000 480000 120000 80000; do #1/3, 1/2, 2, 3
    clearEnv
    configApp
    runApp
    #configAppStatic
    #runAppStatic

    # wait for app start
    python -c 'import time; time.sleep(100)'

    BROKER=${HOST}:9092
    #The rate here will become [BASE * 2, RATE * 4 + BASE * 2]
    CYCLE=$INPUT_CYCLE
    #RATE=3000
    #BASE=3000
    RATE=$INPUT_RATE
    BASE=$INPUT_BASE


    if [[ ${APP} == 1 ]]
    then
        generateBid
    elif [[ ${APP} == 2 ]]
    then
        generateBid
    elif [[ ${APP} == 5 ]]
    then
        generateBid
    elif [[ ${APP} == 8 ]]
    then
        generateAuction
        generatePerson
        #echo "Generate"
    fi

    python -c 'import time; time.sleep(780)'

    # run 120s
    #python -c 'import time; time.sleep(500)'
    killApp
    killGenerator


    EXP_NAME=B${BASE}C${CYCLE}R${RATE}_H${limit}_APP${appid}

    localDir="/home/samza/GroundTruth/nexmark_result/${EXP_NAME}"
    figDir="${APP_DIR}/nexmark_scripts/draw/figures/${EXP_NAME}"
    mkdir ${figDir}
    bash ${APP_DIR}/nexmark_scripts/runScpr.sh ${appid} ${localDir}

    cd ${APP_DIR}/nexmark_scripts/draw
    python2 RateAndWindowDelay.py ${EXP_NAME}
    python2 ViolationsAndUsageFromGroundTruth.py ${EXP_NAME}
done