import json,os
from confluent_kafka import KafkaError
import server.infrastructure.kafka.EventBackboneConfig as EventBackboneConfig
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

POLL_TIMEOUT=5.0

class KafkaAvroConsumer:

    def __init__(self, value_schema, topic_name = "kafka-avro-producer", groupID = 'KafkaAvroConsumer', autocommit = True):

        # Schema Registry configuration
        self.schema_registry_conf = EventBackboneConfig.getSchemaRegistryConf()
        # Schema Registry Client
        self.schema_registry_client = SchemaRegistryClient(self.schema_registry_conf)

        # Key Deserializer
        self.key_deserializer = StringDeserializer('utf_8')
        # Value Deserializer
        # Presenting the schema to the Avro Deserializer is needed at the moment. In the future it might change
        # https://github.com/confluentinc/confluent-kafka-python/issues/834
        self.value_deserializer = AvroDeserializer(value_schema,self.schema_registry_client)

        # Get the consumer configuration
        self.consumer_conf = EventBackboneConfig.getConsumerConfiguration(groupID, autocommit, 
                    self.key_deserializer,
                    self.value_deserializer)
        # Create the consumer
        self.consumer = DeserializingConsumer(self.consumer_conf)
        EventBackboneConfig.printConsumerConfiguration(self.consumer_conf,self.schema_registry_conf['url'])
        # Subscribe to the topic
        self.consumer.subscribe([topic_name])
    
    def traceResponse(self, msg):
        print('[KafkaConsumer] - Topic {} partition [{}] at offset {}:\n\tkey: {}\n\tvalue: {}'
                    .format(msg.topic(), msg.partition(), msg.offset(), msg.key(), msg.value() ))

    # Polls for next event
    def pollNextEvent(self):
        # Poll for messages
        msg = self.consumer.poll(timeout=POLL_TIMEOUT)
        anEvent = {}
        # Validate the returned message
        if msg is None:
            print("[KafkaAvroConsumer] - [INFO] - No new messages on the topic")
            return None
        elif msg.error():
            if ("PARTITION_EOF" in msg.error()):
                print("[KafkaAvroConsumer] - [INFO] - End of partition")
            else:
                print("[KafkaAvroConsumer] - [ERROR] - Consumer error: {}".format(msg.error()))
            return None
        else:
            # Print the message
            self.traceResponse(msg)
        return msg.value()

   
    
    # Polls for the next event but returns the raw event
    def pollNextRawEvent(self):
        records = self.consumer.poll(timeout=POLL_TIMEOUT)
        if records is None:
            return None
        if records.error():
            # Stop reading if we find end of partition in the error message
            if ("PARTITION_EOF" in records.error()):
                return None
            else:
                print("[KafkaAvroConsumer] - [ERROR] - Consumer error: {}".format(records.error()))
                return None
        else:
            self.traceResponse(records)
        return records


    def commitEvent(self,event):
        self.consumer.commit(event)

    def close(self):
        self.consumer.close()