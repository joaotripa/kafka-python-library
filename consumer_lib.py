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

class ConsumerEntity(object):
    """ 
    Create Consumer instance
    'auto.offset.reset=earliest' to start reading from the beginning of the
    topic if no committed offsets exist

    Args: 
        conf (dict): Consumer configurations

    Returns:
           
    """
    def __init__(self, conf):
        self.consumer = Consumer({
            'bootstrap.servers': conf['bootstrap.servers'],
            #'sasl.mechanisms': conf['sasl.mechanisms'],
            #'security.protocol': conf['security.protocol'],
            #'sasl.username': conf['sasl.username'],
            #'sasl.password': conf['sasl.password'],
            'group.id': 'python_example_group_1',
            'auto.offset.reset': 'earliest',
        })

    def subscribe(self, topic):
        """ 
        Subscribe to topic
        
        Args:
            topic (str): topic name/id

        Returns:

        """
        self.consumer.subscribe([topic])

    def consume(self):
        """ 
        Consume records
        
        Args:

        Returns: records
            
        """
        return self.consumer.poll(1.0)

    def close(self):
        self.consumer.close()            
