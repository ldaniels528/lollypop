namespace `dev.kafka`
//////////////////////////////////////////////////////////////////////////////////////
//      Start Development Environment Demo
// include './app/examples/src/main/lollypop/StartDevEnv.sql'
//////////////////////////////////////////////////////////////////////////////////////

// Start zookeepert
zookeeper = (& ./zookeeper/bin/zkServer.sh start &)

// Start the Kafka brokers
server0 = (& ./kafka/bin/kafka-server-start.sh ./config/server0.properties &)
server1 = (& ./kafka/bin/kafka-server-start.sh ./config/server1.properties &)
