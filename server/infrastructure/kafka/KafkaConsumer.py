import json, os
from confluent_kafka import Consumer, KafkaError
import userapp.server.infrastructure.kafka.EventBackboneConfig as EventBackboneConfig


class KafkaConsumer:

    def __init__(self, topic_name = ""):
        self.topic_name = topic_name

    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    # Prepares de Consumer with specific options based on the case
    def prepareConsumer(self, groupID = "VOOKafkaConsumer"):
        options ={
                'bootstrap.servers': EventBackboneConfig.getKafkaBrokers(),
                'group.id': groupID,
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False,
        }
        if (EventBackboneConfig.needsAuthentication()):
            # Set security protocol common to ES on prem and on IBM Cloud
            options['security.protocol'] = 'SASL_SSL'
            # Depending on the Kafka User, we will know whether we are talking to ES on prem or on IBM Cloud
            # If we are connecting to ES on IBM Cloud, the SASL mechanism is plain
            if (EventBackboneConfig.getKafkaUser() == 'token'):
                options['sasl.mechanisms'] = 'PLAIN'
            # If we are connecting to ES on OCP, the SASL mechanism is scram-sha-512
            else:
                options['sasl.mechanisms'] = 'SCRAM-SHA-512'
            # Set the SASL username and password
            options['sasl.username'] = EventBackboneConfig.getKafkaUser()
            options['sasl.password'] = EventBackboneConfig.getKafkaPassword()
        # If we are talking to ES on prem, it uses an SSL certificate to establish communication
        if (EventBackboneConfig.isCertificateSecured()):
            options['ssl.ca.location'] = EventBackboneConfig.getKafkaCertificate()

        # Printing out consumer config for debugging purposes        
        print("[KafkaConsumer] - This is the configuration for the consumer:")
        print("[KafkaConsumer] - -------------------------------------------")
        print('[KafkaConsumer] - Bootstrap Server:  {}'.format(options['bootstrap.servers']))
        if (EventBackboneConfig.needsAuthentication()):
            # Obfuscate password
            if (len(EventBackboneConfig.getKafkaPassword()) > 3):
                obfuscated_password = EventBackboneConfig.getKafkaPassword()[0] + "*****" + EventBackboneConfig.getKafkaPassword()[len(EventBackboneConfig.getKafkaPassword())-1]
            else:
                obfuscated_password = "*******"
            print('[KafkaConsumer] - Security Protocol: {}'.format(options['security.protocol']))
            print('[KafkaConsumer] - SASL Mechanism:    {}'.format(options['sasl.mechanisms']))
            print('[KafkaConsumer] - SASL Username:     {}'.format(options['sasl.username']))
            print('[KafkaConsumer] - SASL Password:     {}'.format(obfuscated_password))
            if (EventBackboneConfig.isCertificateSecured()): 
                print('[KafkaConsumer] - SSL CA Location:   {}'.format(options['ssl.ca.location']))
        print("[KafkaConsumer] - -------------------------------------------")

        # Create the consumer
        self.consumer = Consumer(options)
        self.consumer.subscribe([self.topic_name])
    
    # Prints out and returns the decoded events received by the consumer
    def traceResponse(self, msg):
        msgStr = msg.value().decode('utf-8')
        print('[KafkaConsumer] - Topic {} partition [{}] at offset {}:\n\tkey {}:\n\tvalue: {}'
                    .format(msg.topic(), msg.partition(), msg.offset(), msg.key().decode('utf-8'), msgStr ))
        return msgStr

    # Polls for events until it finds an event where keyId=keyname
    def pollNextEvent(self, keyID, keyname):
        gotIt = False
        anEvent = {}
        while not gotIt:
            msg = self.consumer.poll(timeout=10.0)
            # Continue if we have not received a message yet
            if msg is None:
                continue
            if msg.error():
                print("[KafkaConsumer] - Consumer error: {}".format(msg.error()))
                # Stop reading if we find end of partition in the error message
                if ("PARTITION_EOF" in msg.error()):
                    gotIt= True
                continue
            msgStr = self.traceResponse(msg)
            # Create the json event based on message string formed by traceResponse
            anEvent = json.loads(msgStr)
            # If we've found our event based on keyname and keyID, stop reading messages
            if (anEvent["payload"][keyname] == keyID):
                gotIt = True
        return anEvent

    # Polls for events until it finds an event with same key
    def pollNextEventByKey(self, keyID):
        if (str(keyID) == ""):
            print("[KafkaConsumer] - Consumer error: Key is an empty string")
            return None
        gotIt = False
        anEvent = {}
        while not gotIt:
            msg = self.consumer.poll(timeout=10.0)
            # Continue if we have not received a message yet
            if msg is None:
                continue
            if msg.error():
                print("[KafkaConsumer] - Consumer error: {}".format(msg.error()))
                # Stop reading if we find end of partition in the error message
                if ("PARTITION_EOF" in msg.error()):
                    gotIt= True
                continue
            msgStr = self.traceResponse(msg)
            # Create the json event based on message string formed by traceResponse
            anEvent = json.loads(msgStr)
            # If we've found our event based on keyname and keyID, stop reading messages
            if (str(msg.key().decode('utf-8')) == keyID):
                gotIt = True
        return anEvent

    # Polls for the next event
    def pollNextEvent(self):
        anEvent = {}
        msg = self.consumer.poll(timeout=10.0)
        if msg is None:
            return None
        if msg.error():
            # Stop reading if we find end of partition in the error message
            if ("PARTITION_EOF" in msg.error()):
                return None
            else:
                print("[KafkaConsumer] - Consumer error: {}".format(msg.error()))
                return None
        msgStr = self.traceResponse(msg)
        # Create the json event based on message string formed by traceResponse
        anEvent = json.loads(msgStr)
        return anEvent
    
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
                print("[KafkaConsumer] - Consumer error: {}".format(msg.error()))
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
                print("[ERROR] - [KafkaConsumer] - Consumer error: {}".format(msg.error()))
                if ("PARTITION_EOF" in msg.error()):
                    gotIt= True
                continue
            self.traceResponse(msg)
    
    def close(self):
        self.consumer.close()