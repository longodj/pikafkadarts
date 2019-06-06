#!/usr/bin/env python
#
# Copyright (c) Microsoft Corporation. All rights reserved.
# Copyright 2016 Confluent Inc.
# Licensed under the MIT License.
# Licensed under the Apache License, Version 2.0
#
# Original Confluent sample modified for use with Azure Event Hubs for Apache Kafka Ecosystems

from confluent_kafka import Producer
import sys
import time
import random
import os

#from dotenv import load_dotenv
#from pathlib import Path

'''
Environment variables file passed through --env-file should contain:

PIKAFKADARTS_BOOTSTRAP_SERVER="{FQDN}"
PIKAFKADARTS_CONNECTION_STRING="{CONNECTION_STRING}"
'''
#env_path = Path('.') / '.env'
#load_dotenv(dotenv_path=env_path)

random.seed()

if __name__ == '__main__':
    if len(sys.argv) != 2:
        sys.stderr.write('Usage: %s <topic>\n' % sys.argv[0])
        sys.exit(1)
    topic = sys.argv[1]

    # Producer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    # See https://github.com/edenhill/librdkafka/wiki/Using-SSL-with-librdkafka#prerequisites for SSL issues
    conf = {
        'bootstrap.servers': os.getenv("PIKAFKADARTS_BOOTSTRAP_SERVER") + \
                ':9093',
        'security.protocol': 'SASL_SSL',
        'ssl.ca.location': '/usr/lib/ssl/certs/ca-certificates.crt',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': '$ConnectionString',
        'sasl.password': os.getenv("PIKAFKADARTS_CONNECTION_STRING"),
        'client.id': os.uname()[1]
    }

    # Create Producer instance
    p = Producer(**conf)
    print("Producer created")


    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %o\n' % (msg.topic(), msg.partition(), msg.offset()))


    # Write 1-100 to topic
    while True:
        try:
            p.produce(topic, str(random.uniform(0,4)-2), callback=delivery_callback)
        except BufferError as e:
            sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' % len(p))
        p.poll(0)
        p.flush()
        time.sleep(1)

    # Wait until all messages have been delivered
    sys.stderr.write('%% Waiting for %d deliveries\n' % len(p))
