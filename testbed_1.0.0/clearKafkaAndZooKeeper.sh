export JAVA_HOME=/home/samza/kit/jdk
~/samza-hello-samza/bin/grid stop kafka
~/samza-hello-samza/bin/grid stop zookeeper
rm -r /data/kafka/kafka-logs/
rm -r /tmp/zookeeper/
python -c 'import time; time.sleep(10)'
~/samza-hello-samza/bin/grid start zookeeper
~/samza-hello-samza/bin/grid start kafka

