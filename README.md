# Change Propagation 
[![Version](https://img.shields.io/npm/v/change-propagation.svg?maxAge=2592000&style=flat-square)](https://www.npmjs.com/package/change-propagation)
[![Travis](https://img.shields.io/travis/wikimedia/change-propagation.svg?maxAge=2592000&style=flat-square)](https://travis-ci.org/wikimedia/change-propagation)
[![Coveralls](https://img.shields.io/coveralls/wikimedia/change-propagation.svg?maxAge=2592000&style=flat-square)](https://coveralls.io/github/wikimedia/change-propagation)
[![Dependencies](https://img.shields.io/david/wikimedia/change-propagation.svg?maxAge=2592000&style=flat-square)](https://david-dm.org/wikimedia/change-propagation)
[![License](https://img.shields.io/github/license/wikimedia/change-propagation.svg?maxAge=2592000&style=flat-square)](https://github.com/wikimedia/change-propagation/blob/master/LICENSE)

A [RESTBase](https://github.com/wikimedia/restbase) queuing module for
[Apache Kafka](http://kafka.apache.org/)


## Testing

For testing locally you need to setup and start Apache Kafka and set the 
`KAFKA_HOME` environment variable to point to the Kafka home directory.
Here's a sample script you need to run:

```bash
export KAFKA_HOME=<your desired kafka install path>
wget http://www.us.apache.org/dist/kafka/0.8.2.2/kafka_2.10-0.8.2.2.tgz -O kafka.tgz
mkdir -p $KAFKA_HOME && tar xzf kafka.tgz -C $KAFKA_HOME --strip-components 1
echo "KAFKA_HOME=$KAFKA_HOME" >> ~/.bash_profile
echo "PATH=\$PATH:\$KAFKA_HOME/bin" >> ~/.bash_profile
```

Also, you need to enable topic deletion so that the test scripts could clean up
kafka state before each test run:

```bash
echo 'delete.topic.enable=true' >> KAFKA_HOME/config/server.properties
```

Before starting the development version of change propagation or running
test you need to start Zookeeper and Kafka with `start-kafka` npm script.
To stop Kafka and Zookeeper tun `stop-kafka` npm script.

