#!/usr/bin/env python
# =============================================================================
#
# Produce messages to Kafka
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Producer, KafkaError

import json
import kafka_lib

class ProducerEntity(object):
    """
    Creates Kafka Producer Instance

    Args:
        conf (dict): Producer configurations
    
    Returns:

    """
    def __init__(self, conf):
        self.producer = Producer({
            'bootstrap.servers': conf['bootstrap.servers'],
        #    'sasl.mechanisms': conf['sasl.mechanisms'],
        #    'security.protocol': conf['security.protocol'],
        #    'sasl.username': conf['sasl.username'],
        #    'sasl.password': conf['sasl.password'],
        })

    # Optional per-message on_delivery handler (triggered by poll() or flush())
    # when a message has been successfully delivered or
    # permanently failed delivery (after retries).
    def acked(self, err, msg):
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            print("Produced record to topic {} partition [{}] @ offset {}"
                  .format(msg.topic(), msg.partition(), msg.offset()))

    def produce(self, topic, key, value, encoder):
        """ 
        Produce records
        
        Args:
            topic (str): topic name/id

            key (str): record key to produce

            value (product): record value to produce

        Returns:
            
        """
        record_key = json.dumps(key)
        record_value = json.dumps(value,indent=4, cls=encoder)
        print("Producing record: {}\t{}".format(record_key, record_value))
        self.producer.produce(topic, key=record_key, value=record_value, on_delivery=self.acked)
        self.producer.poll(0)

    def flush(self):
        self.producer.flush()

