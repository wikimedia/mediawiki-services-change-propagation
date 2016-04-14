#!/bin/bash

dropTopics ( ) {
  if [ "$#" -eq 1 ]
  then
    PATTERN=$1
    echo "looking for topics named '*${PATTERN}*'..."
    for TOPIC in `${KAFKA_HOME}/bin/kafka-topics.sh --zookeeper localhost:2181 --list | grep ${PATTERN}`
    do
      echo "dropping topic ${TOPIC}"
      ${KAFKA_HOME}/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic ${TOPIC} > /dev/null
    done
  fi
}

dropTopics "test_topic"
