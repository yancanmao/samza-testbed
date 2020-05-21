export JAVA_HOME=/home/samza/kit/jdk
/home/samza/samza-hello-samza/bin/grid stop kafka
/home/samza/samza-hello-samza/bin/grid stop zookeeper
rm -r /tmp/kafka-logs/
rm -r /tmp/zookeeper/
/home/samza/samza-hello-samza/bin/grid start zookeeper
/home/samza/samza-hello-samza/bin/grid start kafka

