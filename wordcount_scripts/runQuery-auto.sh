#!/usr/bin/env bash
APP_DIR="$(dirname $(pwd))"

IS_COMPILE=$1
HOST=$2
APP=$3
INPUT_CYCLE=$4
INPUT_BASE=$5
INPUT_RATE=$6

function clearEnv() {
    export JAVA_HOME=/home/samza/kit/jdk
    /home/samza/samza-hello-samza/bin/grid stop kafka
    /home/samza/samza-hello-samza/bin/grid stop zookeeper
    kill -9 $(jps | grep Kafka | awk '{print $1}')
    python -c 'import time; time.sleep(5)'
    rm -r /data/kafka/kafka-logs/
    rm -r /tmp/zookeeper/
    /home/samza/samza-hello-samza/bin/grid start zookeeper
    /home/samza/samza-hello-samza/bin/grid start kafka
    python -c 'import time; time.sleep(5)'

    /home/samza/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --zookeeper ${HOST}:2181 --create --topic sentences --partitions 64 --replication-factor 1  --config message.timestamp.type=LogAppendTime
    /home/samza/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --zookeeper ${HOST}:2181 --create --topic words --partitions 64 --replication-factor 1  --config message.timestamp.type=LogAppendTime
}

function configApp() {
    sed -i -- 's/localhost/'${HOST}'/g' ${APP_DIR}/testbed_1.0.0/target/config/word-count-splitter-ss.properties
    sed -i -- 's/localhost/'${HOST}'/g' ${APP_DIR}/testbed_1.0.0/target/config/word-count-counter-ss.properties
}

function compile() {
    cd ${APP_DIR}/testbed_1.0.0/
    mvn clean package
    cd target
    tar -zvxf *-dist.tar.gz
    cd ${APP_DIR}
}

function uploadHDFS() {
    ~/cluster/yarn/bin/hdfs dfs -rm  hdfs://${HOST}:9000/testbed-wordcount/*-dist.tar.gz
    ~/cluster/yarn/bin/hdfs dfs -mkdir hdfs://${HOST}:9000/testbed-wordcount
    ~/cluster/yarn/bin/hdfs dfs -put  ${APP_DIR}/testbed_1.0.0/target/*-dist.tar.gz hdfs://${HOST}:9000/testbed-wordcount
}

function compileGenerator() {
    cd ${APP_DIR}/kafka_producer/
    mvn clean package
    cd ${APP_DIR}
}

function generate() {
    java -cp ${APP_DIR}/kafka_producer/target/kafka_producer-0.0.1-jar-with-dependencies.jar kafka.WordCount.SentenceGenerator \
        -host $BROKER -topic sentences -rate $RATE -cycle $CYCLE -base $BASE &
}

function runApp() {
    OUTPUT=`${APP_DIR}/testbed_1.0.0/target/bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory \
    --config-path=file://${APP_DIR}/testbed_1.0.0/target/config/word-count-splitter-ss.properties | grep 'application_.*$'`
    splitterapp=`[[ ${OUTPUT} =~ application_[0-9]*_[0-9]* ]] && echo $BASH_REMATCH`
    splitterappid=${splitterapp#application_}
    echo "assigned app id is: $splitterappid"


#   OUTPUT=`${APP_DIR}/testbed_1.0.0/target/bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory \
#    --config-path=file://${APP_DIR}/testbed_1.0.0/target/config/word-count-counter-ss.properties | grep 'application_.*$'`
#    counterapp=`[[ ${OUTPUT} =~ application_[0-9]*_[0-9]* ]] && echo $BASH_REMATCH`
#    counterappid=${counterapp#application_}
#    echo "assigned app id is: $counterappid"
}

function killApp() {
    ~/cluster/yarn/bin/yarn application -kill ${splitterapp}
    ~/cluster/yarn/bin/yarn application -kill ${counterapp}
}

function killGenerator() {
    kill -9 $(jps | grep Generator | awk '{print $1}')
}

if [[ ${IS_COMPILE} == 1 ]]
then
    compile
    compileGenerator
    uploadHDFS
fi

clearEnv
configApp
runApp

# wait for app start
python -c 'import time; time.sleep(100)'


BROKER=${HOST}:9092
#The rate here will become [BASE * 2, RATE * 4 + BASE * 2]
CYCLE=${INPUT_CYCLE}
RATE=${INPUT_RATE}
BASE=${INPUT_BASE}

generate

python -c 'import time; time.sleep(780)'

# run 120s
#python -c 'import time; time.sleep(500)'
killApp
killGenerator


EXP_NAME=B${BASE}C${CYCLE}R${RATE}_Splitter_APP${splitterappid}

localDir="/home/samza/GroundTruth/wordcount_result/${EXP_NAME}"
figDir="${APP_DIR}/wordcount_scripts/draw/figures/${EXP_NAME}"
mkdir ${figDir}
bash ${APP_DIR}/wordcount_scripts/runScpr.sh ${splitterappid} ${localDir}

cd ${APP_DIR}/wordcount_scripts/draw
python2 RateAndWindowDelay.py ${EXP_NAME}
python2 ViolationsAndUsageFromGroundTruth.py ${EXP_NAME}


EXP_NAME=B${BASE}C${CYCLE}R${RATE}_Counter_APP${counterappid}

localDir="/home/samza/GroundTruth/wordcount_result/${EXP_NAME}"
figDir="${APP_DIR}/wordcount_scripts/draw/figures/${EXP_NAME}"
mkdir ${figDir}
bash ${APP_DIR}/wordcount_scripts/runScpr.sh ${counterappid} ${localDir}

cd ${APP_DIR}/wordcount_scripts/draw
python2 RateAndWindowDelay.py ${EXP_NAME}
python2 ViolationsAndUsageFromGroundTruth.py ${EXP_NAME}
