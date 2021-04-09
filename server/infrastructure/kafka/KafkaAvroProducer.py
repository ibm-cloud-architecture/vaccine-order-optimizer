import json,os
from confluent_kafka import KafkaError

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import server.infrastructure.kafka.EventBackboneConfig as EventBackboneConfig

class KafkaAvroProducer:

    def __init__(self, producer_name, value_schema, groupID = 'KafkaAvroProducer'):
        
        # Consumer name for logging purposes
        self.logging_prefix = '['+ producer_name + '][KafkaAvroProducer]'

        # Schema Registry configuration
        self.schema_registry_conf = EventBackboneConfig.getSchemaRegistryConf()
        # Schema Registry Client
        self.schema_registry_client = SchemaRegistryClient(self.schema_registry_conf)

        # String Serializer for the key
        self.key_serializer = StringSerializer('utf_8')
        # Avro Serializer for the value
        self.value_serializer = AvroSerializer(value_schema, self.schema_registry_client)
        
        # Get the producer configuration
        self.producer_conf = EventBackboneConfig.getProducerConfiguration(groupID,
                        self.key_serializer,
                        self.value_serializer)
        EventBackboneConfig.printProducerConfiguration(self.logging_prefix, self.producer_conf, self.schema_registry_conf['url'])
        # Create the producer
        self.producer = SerializingProducer(self.producer_conf)



    def delivery_report(self,err, msg):
        """ Called once for each message produced to indicate delivery result. Triggered by poll() or flush(). """
        if err is not None:
            print('[KafkaAvroProducer] - [ERROR] - Message delivery failed: {}'.format(err))
        else:
            print('[KafkaAvroProducer] - Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

    def publishEvent(self, key, value, topicName = 'kafka-avro-producer'):
        # Produce the Avro message
        self.producer.produce(topic=topicName,value=value,key=key, on_delivery=self.delivery_report)
        # Flush
        self.producer.flush()