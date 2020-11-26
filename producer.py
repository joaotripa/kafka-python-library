#!/usr/bin/env python
# =============================================================================
#
# Produce messages to Kafka
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from producer_lib import ProducerEntity as Producer
from productencoder import ProductEncoder
from product import Product
from uuid import uuid4

import json
import kafka_lib
import random

if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = kafka_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = kafka_lib.read_config(config_file)

    # Create Producer instance
    producer = Producer(conf)

    # Create topic if needed
    kafka_lib.create_topic(conf, topic)

    #Producing Sample Messages
    for n in range(10):
        product = Product(prodID="product"+str(n),prodDesc="Simple Product",prodSpecs=random.randint(0,10))
        producer.produce(topic, str(uuid4()), product, ProductEncoder)

    producer.flush()