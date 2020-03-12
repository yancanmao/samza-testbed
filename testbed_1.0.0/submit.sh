deletechanelog=$3
iscompile=$2
job=$1

#if [ $deletechanelog == 1 ]
#then
#    ~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic stock-exchange-buy-changelog
#    ~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic stock-exchange-sell-changelog
#    ~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic __samza_coordinator_stock-exchange_1
#    ~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic stock_sb
#    python -c 'import time; time.sleep(60)'
#    ~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic stock_sb --partitions 32 --replication-factor 1 --config message.timestamp.type=LogAppendTime
#
##    awk -F"=" 'BEGIN{OFS=FS} $1=="job.id"{$2=$2+1}1' src/main/config/stock-exchange-ss-camel.properties > properties.tmp
##    mv properties.tmp src/main/config/stock-exchange-ss-camel.properties
#fi

#Check whether they are equal 
if [ $iscompile == 1 ] 
then 
    mvn clean package
fi 
cd target
tar -zvxf *-dist.tar.gz
if [ $job == 1 ]
then

./bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/config/stock-exchange.properties
cd ..

#./consumer.sh results # stock_cj
fi
if [ $job == 2 ]
then 
./bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/config/stock-average-task.properties
cd ..
./consumer.sh stock_price
fi
if [ $job == 3 ]
then 
./bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/config/stock-exchange.properties
./bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/config/stock-average-task.properties
cd ..
./consumer.sh stock_price
fi

if [ $job == 4 ]
then
~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic nexmark-q3-1-join-join-L
~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic nexmark-q3-1-join-join-R
~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic nexmark-q3-1-partition_by-auction
~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic nexmark-q3-1-partition_by-person
~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic __samza_coordinator_nexmark-q3_1
~/samza-hello-samza/deploy/kafka/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic results

./bin/run-app.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/config/nexmark-q3.properties
cd ..
./consumer.sh results # stock_cj
fi
