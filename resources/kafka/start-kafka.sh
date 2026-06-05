#!/bin/bash
KAFKA_CLUSTER_ID="$(/opt/kafka/bin/kafka-storage.sh random-uuid)"
/opt/kafka/bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID \
  -c /opt/kafka/config/server.properties --standalone --ignore-formatted

/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties \
  --override listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
  --override advertised.listeners=PLAINTEXT://kafka:9092 \
  --override controller.listener.names=CONTROLLER \
  --override listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT \
  --override process.roles=broker,controller \
  --override node.id=1 \
  --override controller.quorum.voters=1@kafka:9093
