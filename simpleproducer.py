#!/usr/bin/env python
# =============================================================================
#
# Produce messages to Kafka
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Producer, KafkaError
from product import Product
from uuid import uuid4
from productencoder import ProductEncoder
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
    producer = Producer({
        'bootstrap.servers': conf['bootstrap.servers'],
    #    'sasl.mechanisms': conf['sasl.mechanisms'],
    #    'security.protocol': conf['security.protocol'],
    #    'sasl.username': conf['sasl.username'],
    #    'sasl.password': conf['sasl.password'],
    })

    # Create topic if needed
    kafka_lib.create_topic(conf, topic)

    delivered_records = 0

    # Optional per-message on_delivery handler (triggered by poll() or flush())
    # when a message has been successfully delivered or
    # permanently failed delivery (after retries).
    def acked(err, msg):
        global delivered_records
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1
            print("Produced record to topic {} partition [{}] @ offset {}"
                  .format(msg.topic(), msg.partition(), msg.offset()))

    #Producing Sample Messages
    for n in range(10):
        record_key = json.dumps(str(uuid4()))
        product = Product(prodID="product"+str(n),prodDesc="Simple Product",prodSpecs=random.randint(0,10))
        record_value = json.dumps(product,indent=4, cls=ProductEncoder)
        print("Producing record: {}\t{}".format(record_key, record_value))
        producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
        producer.poll(0)

    producer.flush()

    print("{} messages were produced to topic {}!".format(delivered_records, topic))
