import json,os
from confluent_kafka import KafkaError
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer


class KafkaAvroConsumer:

    def __init__(self, value_schema, topic_name = "kafka-avro-producer", groupID = 'KafkaAvroConsumer', autocommit = True):

        # Schema Registry configuration
        self.schema_registry_conf = self.getSchemaRegistryConf()
        # Schema Registry Client
        self.schema_registry_client = SchemaRegistryClient(self.schema_registry_conf)

        # Key Deserializer
        self.key_deserializer = StringDeserializer('utf_8')
        # Value Deserializer
        # Presenting the schema to the Avro Deserializer is needed at the moment. In the future it might change
        # https://github.com/confluentinc/confluent-kafka-python/issues/834
        self.value_deserializer = AvroDeserializer(value_schema,self.schema_registry_client)

        # Get the consumer configuration
        self.consumer_conf = self.getConsumerConfiguration(groupID, autocommit)
        # Create the consumer
        self.consumer = DeserializingConsumer(self.consumer_conf)
        # Subscribe to the topic
        self.consumer.subscribe([topic_name])


    def getSchemaRegistryConf(self):
        try:
            # For IBM Event Streams on IBM Cloud and on OpenShift, the Schema Registry URL is some sort of
            # https://KAFKA_USER:KAFKA_PASSWORD@SCHEMA_REGISTRY_URL
            # Make sure the SCHEMA_REGISTRY_URL your provide is in the form described above.
            url = os.environ['SCHEMA_REGISTRY_URL']
            # If we are talking to ES on prem, it uses an SSL self-signed certificate.
            # Therefore, we need the CA public certificate for the SSL connection to happen.
            if (os.path.isfile(os.getenv('KAFKA_CERT','/certs/es-cert.pem'))):
                ssl = os.getenv('KAFKA_CERT','/certs/es-cert.pem')
                return {'url': url, 'ssl.ca.location': ssl}
            return {'url': url}
        except KeyError:
            print('[KafkaAvroConsumer] - [ERROR] - There is no SCHEMA_REGISTRY_URL environment variable')
            exit(1)

    def getConsumerConfiguration(self, groupID, autocommit):
        try:
            options ={
                    'bootstrap.servers': os.environ['KAFKA_BROKERS'],
                    'group.id': groupID,
                    'key.deserializer': self.key_deserializer,
                    'value.deserializer': self.value_deserializer,
                    'auto.offset.reset': "earliest",
                    'enable.auto.commit': autocommit,
            }
            if (os.getenv('KAFKA_PASSWORD','') != ''):
                # Set security protocol common to ES on prem and on IBM Cloud
                options['security.protocol'] = 'SASL_SSL'
                # Depending on the Kafka User, we will know whether we are talking to ES on prem or on IBM Cloud
                # If we are connecting to ES on IBM Cloud, the SASL mechanism is plain
                if (os.getenv('KAFKA_USER','') == 'token'):
                    options['sasl.mechanisms'] = 'PLAIN'
                # If we are connecting to ES on OCP, the SASL mechanism is scram-sha-512
                else:
                    options['sasl.mechanisms'] = 'SCRAM-SHA-512'
                # Set the SASL username and password
                options['sasl.username'] = os.getenv('KAFKA_USER','')
                options['sasl.password'] = os.getenv('KAFKA_PASSWORD','')
            # If we are talking to ES on prem, it uses an SSL self-signed certificate.
            # Therefore, we need the CA public certificate for the SSL connection to happen.
            if (os.path.isfile(os.getenv('KAFKA_CERT','/certs/es-cert.pem'))):
                options['ssl.ca.location'] = os.getenv('KAFKA_CERT','/certs/es-cert.pem')
            
            # Print out the producer configuration
            self.printConsumerConfiguration(options)

            return options

        except KeyError as error:
            print('[KafkaAvroConsumer] - [ERROR] - A required environment variable does not exist: ' + error)
            exit(1)

    def printConsumerConfiguration(self,options):
        # Printing out consumer config for debugging purposes        
        print("[KafkaAvroConsumer] - This is the configuration for the consumer:")
        print("[KafkaAvroConsumer] - -------------------------------------------")
        print('[KafkaAvroConsumer] - Bootstrap Server:      {}'.format(options['bootstrap.servers']))
        print('[KafkaAvroConsumer] - Schema Registry url:   {}'.format(self.schema_registry_conf['url'].split('@')[-1]))
        if (os.getenv('KAFKA_PASSWORD','') != ''):
            # Obfuscate password
            if (len(options['sasl.password']) > 3):
                obfuscated_password = options['sasl.password'][0] + "*****" + options['sasl.password'][len(options['sasl.password'])-1]
            else:
                obfuscated_password = "*******"
            print('[KafkaAvroConsumer] - Security Protocol:     {}'.format(options['security.protocol']))
            print('[KafkaAvroConsumer] - SASL Mechanism:        {}'.format(options['sasl.mechanisms']))
            print('[KafkaAvroConsumer] - SASL Username:         {}'.format(options['sasl.username']))
            print('[KafkaAvroConsumer] - SASL Password:         {}'.format(obfuscated_password))
            if (os.path.isfile(os.getenv('KAFKA_CERT','/certs/es-cert.pem'))): 
                print('[KafkaAvroConsumer] - SSL CA Location:       {}'.format(options['ssl.ca.location']))
        print('[KafkaAvroConsumer] - Offset Reset:          {}'.format(options['auto.offset.reset']))
        print('[KafkaAvroConsumer] - Autocommit:            {}'.format(options['enable.auto.commit']))
        print("[KafkaAvroConsumer] - -------------------------------------------")

    
    def traceResponse(self, msg):
        print('[KafkaConsumer] - Topic {} partition [{}] at offset {}:\n\tkey: {}\n\tvalue: {}'
                    .format(msg.topic(), msg.partition(), msg.offset(), msg.key(), msg.value() ))

    # Polls for next event
    def pollNextEvent(self):
        # Poll for messages
        msg = self.consumer.poll(timeout=10.0)
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

    # Polls for events until it finds an event where keyId=keyname
    def pollNextEventKeyIdKeyName(self, keyID, keyname):
        gotIt = False
        while not gotIt:
            msg = self.consumer.poll(timeout=10.0)
            # Continue if we have not received a message yet
            if msg is None:
                continue
            if msg.error():
                print("[KafkaAvroConsumer] - [ERROR] - Consumer error: {}".format(msg.error()))
                # Stop reading if we find end of partition in the error message
                if ("PARTITION_EOF" in msg.error()):
                    gotIt= True
                continue
            self.traceResponse(msg)
            # If we've found our event based on keyname and keyID, stop reading messages
            if (msg.value()[keyname] == keyID):
                gotIt = True
        return msg.value()

    # Polls for events until it finds an event with same key
    def pollNextEventByKey(self, keyID):
        if (str(keyID) == ""):
            print("[KafkaAvroConsumer] - [ERROR] - Consumer error: Key is an empty string")
            return None
        gotIt = False
        while not gotIt:
            msg = self.consumer.poll(timeout=10.0)
            # Continue if we have not received a message yet
            if msg is None:
                continue
            if msg.error():
                print("[KafkaAvroConsumer] - [ERROR] - Consumer error: {}".format(msg.error()))
                # Stop reading if we find end of partition in the error message
                if ("PARTITION_EOF" in msg.error()):
                    gotIt= True
                continue
            self.traceResponse(msg)
            # If we've found our event based on keyname and keyID, stop reading messages
            if (msg.key() == keyID):
                gotIt = True
        return msg.value()
    
    # Polls for the next event but returns the raw event
    def pollNextRawEvent(self):
        msg = self.consumer.poll(timeout=5.0)
        if msg is None:
            return None
        if msg.error():
            # Stop reading if we find end of partition in the error message
            if ("PARTITION_EOF" in msg.error()):
                return None
            else:
                print("[KafkaAvroConsumer] - [ERROR] - Consumer error: {}".format(msg.error()))
                return None
        return msg

    # Polls for events endlessly
    def pollEvents(self):
        gotIt = False
        while not gotIt:
            msg = self.consumer.poll(timeout=10.0)
            if msg is None:
                continue
            if msg.error():
                print("[KafkaAvroConsumer] - [ERROR] - Consumer error: {}".format(msg.error()))
                if ("PARTITION_EOF" in msg.error()):
                    gotIt= True
                continue
            self.traceResponse(msg)


    def close(self):
        self.consumer.close()