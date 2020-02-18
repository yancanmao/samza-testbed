iscompile=$2
job=$1
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
./consumer.sh stock_cj
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
