import json,os
from confluent_kafka import Consumer, KafkaError

POLL_TIMEOUT=5.0

class KafkaConsumer:

    def __init__(self, topic_name = "test", groupID = 'KafkaConsumer', autocommit = True):
        # Get the consumer configuration
        self.consumer_conf = self.getConsumerConfiguration(groupID, autocommit)
        # Create the Avro consumer
        self.consumer = Consumer(self.consumer_conf)
        # Subscribe to the topic
        self.consumer.subscribe([topic_name])

    def getConsumerConfiguration(self, groupID, autocommit):
        try:
            options ={
                    'bootstrap.servers': os.environ['KAFKA_BROKERS'],
                    'group.id': groupID,
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
            print('[KafkaConsumer] - [ERROR] - A required environment variable does not exist: ' + error)
            exit(1)
    
    def printConsumerConfiguration(self,options):
        # Printing out consumer config for debugging purposes        
        print("[KafkaConsumer] - This is the configuration for the consumer:")
        print("[KafkaConsumer] - -------------------------------------------")
        print('[KafkaConsumer] - Bootstrap Server:      {}'.format(options['bootstrap.servers']))
        if (os.getenv('KAFKA_PASSWORD','') != ''):
            # Obfuscate password
            if (len(options['sasl.password']) > 3):
                obfuscated_password = options['sasl.password'][0] + "*****" + options['sasl.password'][len(options['sasl.password'])-1]
            else:
                obfuscated_password = "*******"
            print('[KafkaConsumer] - Security Protocol:     {}'.format(options['security.protocol']))
            print('[KafkaConsumer] - SASL Mechanism:        {}'.format(options['sasl.mechanisms']))
            print('[KafkaConsumer] - SASL Username:         {}'.format(options['sasl.username']))
            print('[KafkaConsumer] - SASL Password:         {}'.format(obfuscated_password))
            if (os.path.isfile(os.getenv('KAFKA_CERT','/certs/es-cert.pem'))): 
                print('[KafkaConsumer] - SSL CA Location:       {}'.format(options['ssl.ca.location']))
        print('[KafkaConsumer] - Offset Reset:          {}'.format(options['auto.offset.reset']))
        print('[KafkaConsumer] - Autocommit:            {}'.format(options['enable.auto.commit']))
        print("[KafkaConsumer] - -------------------------------------------")
    
    # Prints out and returns the decoded events received by the consumer
    def traceResponse(self, msg):
        print('[KafkaConsumer] - Next Message consumed from {} partition: [{}] at offset: {}\n\tkey: {}\n\tvalue: {}'
                    .format(msg.topic(), msg.partition(), msg.offset(), msg.key().decode('utf-8'), msg.value().decode('utf-8')))

    # Polls for next event
    def pollNextEvent(self):
        # Poll for messages
        msg = self.consumer.poll(timeout=POLL_TIMEOUT)
        # Validate the returned message
        if msg is None:
            print("[KafkaConsumer] - [INFO] - No new messages on the topic")
        elif msg.error():
            if ("PARTITION_EOF" in msg.error()):
                print("[KafkaConsumer] - [INFO] - End of partition")
            else:
                print("[KafkaConsumer] - [ERROR] - Consumer error: {}".format(msg.error()))
        else:
            # Print the message
            self.traceResponse(msg)
    
    def close(self):
        self.consumer.close()