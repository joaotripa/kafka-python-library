# Kafka Python 

A Kafka Library for Python clients

For more information see (https://docs.confluent.io/current/tutorials/examples/clients/docs/python.html#client-examples-python)

## Prerequisites

Kafka Cluster running: 
    - on a local machine 
    - in a cloud environment

A python environment with Confluent Python Client for Apache Kafka.
To create a virtual environment:    
    virtualenv kafka-python-venv
    source ./kafka-python-venv/bin/activate
    pip install -r requirements.txt

## Setup

Create a local file (for example, at $HOME/.confluent/lbrkafka.config) with configuration parameters to connect to your Kafka cluster.

    - Template configuration file for Confluent Cloud

        # Kafka
        bootstrap.servers={{ BROKER_ENDPOINT }}
        security.protocol=SASL_SSL
        sasl.mechanisms=PLAIN
        sasl.username={{ CLUSTER_API_KEY }}
        sasl.password={{ CLUSTER_API_SECRET }}


    - Template configuration file for local host

        # Kafka
        bootstrap.servers=localhost:9092


Inilialize Kafka Cluster 
	
	docker-compose up -d

Initialize the virtual environment (if not initialized):

    source ./kafka-python-venv/bin/activate


## Running the Producer

./producer.py -f $HOME/.confluent/lbrkafka.config -t test1

## Running the Consumer

./consumer.py -f $HOME/.confluent/lbrkafka.config -t test1
