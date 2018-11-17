#!/usr/bin/env bash

cd ~/kafka_2.11-2.0.0
bin/zookeeper-server-start.sh config/zookeeper.properties &
sleep 10
bin/kafka-server-start.sh config/server.properties &
sleep 10
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test

