import time, json, datetime, logging, os
from confluent_kafka import KafkaError, Producer
import userapp.server.infrastructure.kafka.EventBackboneConfig as EventBackboneConfig

class KafkaProducer:

    def prepareProducer(self,groupID = "VaccineDataProducer"):
        options ={
                'bootstrap.servers': EventBackboneConfig.getKafkaBrokers(),
                'group.id': groupID,
                'delivery.timeout.ms': 15000,
                'request.timeout.ms' : 15000
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

        # Printing out producer config for debugging purposes        
        print("[KafkaProducer] - This is the configuration for the producer:")
        print("[KafkaProducer] - -------------------------------------------")
        print('[KafkaProducer] - Bootstrap Server:  {}'.format(options['bootstrap.servers']))
        if (EventBackboneConfig.needsAuthentication()):
            # Obfuscate password
            if (len(EventBackboneConfig.getKafkaPassword()) > 3):
                obfuscated_password = EventBackboneConfig.getKafkaPassword()[0] + "*****" + EventBackboneConfig.getKafkaPassword()[len(EventBackboneConfig.getKafkaPassword())-1]
            else:
                obfuscated_password = "*******"
            print('[KafkaProducer] - Security Protocol: {}'.format(options['security.protocol']))
            print('[KafkaProducer] - SASL Mechanism:    {}'.format(options['sasl.mechanisms']))
            print('[KafkaProducer] - SASL Username:     {}'.format(options['sasl.username']))
            print('[KafkaProducer] - SASL Password:     {}'.format(obfuscated_password))
            if (EventBackboneConfig.isCertificateSecured()): 
                print('[KafkaProducer] - SSL CA Location:   {}'.format(options['ssl.ca.location']))
        print("[KafkaProducer] - -------------------------------------------")

        self.producer = Producer(options)


    def delivery_report(self,err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            print( str(datetime.datetime.today()) + ' - Message delivery failed: {}'.format(err))
        else:
            print(str(datetime.datetime.today()) + ' - Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

    def publishEvent(self, topic, eventToSend, keyName):
        dataStr = json.dumps(eventToSend)
        logging.info("Send " + dataStr + " with key " + keyName + " to " + topic)
        
        self.producer.produce(topic,
                           key=str(eventToSend[keyName]).encode('utf-8'),
                           value=dataStr.encode('utf-8'),
                           callback=self.delivery_report)
        self.producer.flush()
  

   
  