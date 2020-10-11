#!/usr/bin/env python
# =============================================================================
#
# Consume messages from Kafka
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Consumer
from product import Product
import json
import kafka_lib



if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = kafka_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = kafka_lib.read_config(config_file)

    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer = Consumer({
        'bootstrap.servers': conf['bootstrap.servers'],
        #'sasl.mechanisms': conf['sasl.mechanisms'],
        #'security.protocol': conf['security.protocol'],
        #'sasl.username': conf['sasl.username'],
        #'sasl.password': conf['sasl.password'],
        'group.id': 'python_example_group_1',
        'auto.offset.reset': 'earliest',
    })

    # Subscribe to topic
    consumer.subscribe([topic])

    # Process messages
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_key = msg.key()
                record_value = msg.value()
                
                key = json.loads(record_key)
                data = json.loads(record_value)
                print("Consumed record with key {} and value {}"
                      .format(key, data))
                #============================
                #
                # Do Something with data here
                #
                #============================
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
