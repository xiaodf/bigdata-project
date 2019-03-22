clush -g rocfka "cd /opt/cloudera/parcels/CDH/lib/rocfka && java -cp kakfa-rocketmq-1.0-SNAPSHOT.jar iie.udps.rockfka.RocketPushToKafka rocket.properties &> rocket.log.$(date +%F)" &
