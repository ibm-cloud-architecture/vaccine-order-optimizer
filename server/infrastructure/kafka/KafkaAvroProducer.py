import json,os
from confluent_kafka import KafkaError

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

class KafkaAvroProducer:

    def __init__(self, value_schema, groupID = 'KafkaAvroProducer'):
        
        # Schema Registry configuration
        self.schema_registry_conf = self.getSchemaRegistryConf()
        # Schema Registry Client
        self.schema_registry_client = SchemaRegistryClient(self.schema_registry_conf)

        # String Serializer for the key
        self.key_serializer = StringSerializer('utf_8')
        # Avro Serializer for the value
        self.value_serializer = AvroSerializer(value_schema, self.schema_registry_client)
        
        # Get the producer configuration
        self.producer_conf = self.getProducerConfiguration(groupID)
        # Create the producer
        self.producer = SerializingProducer(self.producer_conf)


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
            print('[KafkaAvroProducer] - [ERROR] - There is no SCHEMA_REGISTRY_URL environment variable')
            exit(1)
    
    def getProducerConfiguration(self, groupID):
        try:
            options ={
                    'bootstrap.servers': os.environ['KAFKA_BROKERS'],
                    'group.id': groupID,
                    'key.serializer': self.key_serializer,
                    'value.serializer': self.value_serializer
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
            self.printProducerConfiguration(options)

            return options

        except KeyError as error:
            print('[KafkaAvroProducer] - [ERROR] - A required environment variable does not exist: ' + error)
            exit(1)
    
    def printProducerConfiguration(self,options):
        # Printing out producer config for debugging purposes        
        print("[KafkaAvroProducer] - This is the configuration for the producer:")
        print("[KafkaAvroProducer] - -------------------------------------------")
        print('[KafkaAvroProducer] - Bootstrap Server:      {}'.format(options['bootstrap.servers']))
        print('[KafkaAvroProducer] - Schema Registry url:   {}'.format(self.schema_registry_conf['url'].split('@')[-1]))
        if (os.getenv('KAFKA_PASSWORD','') != ''):
            # Obfuscate password
            if (len(options['sasl.password']) > 3):
                obfuscated_password = options['sasl.password'][0] + "*****" + options['sasl.password'][len(options['sasl.password'])-1]
            else:
                obfuscated_password = "*******"
            print('[KafkaAvroProducer] - Security Protocol:     {}'.format(options['security.protocol']))
            print('[KafkaAvroProducer] - SASL Mechanism:        {}'.format(options['sasl.mechanisms']))
            print('[KafkaAvroProducer] - SASL Username:         {}'.format(options['sasl.username']))
            print('[KafkaAvroProducer] - SASL Password:         {}'.format(obfuscated_password))
            if (os.path.isfile(os.getenv('KAFKA_CERT','/certs/es-cert.pem'))): 
                print('[KafkaAvroProducer] - SSL CA Location:       {}'.format(options['ssl.ca.location']))
        print("[KafkaAvroProducer] - -------------------------------------------")


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