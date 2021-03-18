import os

def getKafkaBrokers():
    return os.getenv('KAFKA_BROKERS','localhost:9092')

def getSchemaRegistryUrl():
    return os.getenv('SCHEMA_REGISTRY_URL','localhost:9092')

def getKafkaPassword():
    return os.getenv('KAFKA_PASSWORD','')

def getKafkaUser():
    return os.getenv('KAFKA_USER','')

def needsAuthentication():
    KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD','')
    return KAFKA_PASSWORD != ''

def isCertificateSecured():
    KAFKA_CERT = os.getenv('KAFKA_CERT','/app/certs/es-cert.pem')
    return os.path.isfile(KAFKA_CERT)

def getKafkaCertificate():
    return os.getenv('KAFKA_CERT','/app/certs/es-cert.pem')

def getReeferTopicName():
    return os.getenv("REEFER_TOPIC","vaccine-reefer")

def getInventoryTopicName():
    return os.getenv("INVENTORY_TOPIC","vaccine-inventory")

def getTransportationTopicName():
    return os.getenv("TRANSPORTATION_TOPIC","vaccine-transportation")

def getOrderTopicName():
    return os.getenv("ORDER_TOPIC","vaccine.public.orderevents")

def getConsumerGroup():
    return os.getenv("CONSUMER_GROUP","vaccine-optimizer")

def getKeySubject():
    return getOrderTopicName() + '-key'

def getValueSubject():
    return getOrderTopicName() + '-value'


def getSchemaRegistryConf():
        try:
            # For IBM Event Streams on IBM Cloud and on OpenShift, the Schema Registry URL is some sort of
            # https://KAFKA_USER:KAFKA_PASSWORD@SCHEMA_REGISTRY_URL
            # Make sure the SCHEMA_REGISTRY_URL your provide is in the form described above.
            url = os.environ['SCHEMA_REGISTRY_URL']
            # if (url.find("@") < 0 ):
            #     url="https://" + os.getenv('KAFKA_USER') + ":" + os.getenv('KAFKA_PASSWORD') + "@" + url
            # elif (url.find("https") < 0 ):
            #     url = "https://" + url
            # If we are talking to ES on prem, it uses an SSL self-signed certificate.
            # Therefore, we need the CA public certificate for the SSL connection to happen.
            if (os.path.isfile(os.getenv('KAFKA_CERT','/app/certs/es-cert.pem'))):
                ssl = os.getenv('KAFKA_CERT','/app/certs/es-cert.pem')
                return {'url': url, 'ssl.ca.location': ssl}
            return {'url': url}
        except KeyError:
            print('[KafkaAvroProducer] - [ERROR] - There is no SCHEMA_REGISTRY_URL environment variable')
            exit(1)

def getProducerConfiguration(groupID,key_serializer,value_serializer):
        try:
            options ={
                    'bootstrap.servers': os.environ['KAFKA_BROKERS'],
                    'group.id': groupID,
                    'key.serializer': key_serializer,
                    'value.serializer': value_serializer
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
            return options

        except KeyError as error:
            print('[KafkaAvroProducer] - [ERROR] - A required environment variable does not exist: ' + error)
            return {}
    
def printProducerConfiguration(options,url):
    # Printing out producer config for debugging purposes        
    print("[KafkaAvroProducer] - This is the configuration for the producer:")
    print("[KafkaAvroProducer] - -------------------------------------------")
    print('[KafkaAvroProducer] - Bootstrap Server:      {}'.format(options['bootstrap.servers']))
    print('[KafkaAvroProducer] - Schema Registry url:   {}'.format(url.split('@')[-1]))
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


def getConsumerConfiguration(groupID, autocommit, key_deserializer, value_deserializer):
        try:
            options ={
                    'bootstrap.servers': os.environ['KAFKA_BROKERS'],
                    'group.id': groupID,
                    'key.deserializer': key_deserializer,
                    'value.deserializer': value_deserializer,
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
            
            return options

        except KeyError as error:
            print('[KafkaAvroConsumer] - [ERROR] - A required environment variable does not exist: ' + error)
            return {} 

def printConsumerConfiguration(logging_prefix,options,url):
        # Printing out consumer config for debugging purposes        
        print(logging_prefix + ' - This is the configuration for the consumer:')
        print(logging_prefix + ' - -------------------------------------------')
        print(logging_prefix + ' - Bootstrap Server:      {}'.format(options['bootstrap.servers']))
        print(logging_prefix + ' - Schema Registry url:   {}'.format(url.split('@')[-1]))
        if (os.getenv('KAFKA_PASSWORD','') != ''):
            # Obfuscate password
            if (len(options['sasl.password']) > 3):
                obfuscated_password = options['sasl.password'][0] + "*****" + options['sasl.password'][len(options['sasl.password'])-1]
            else:
                obfuscated_password = "*******"
            print(logging_prefix + ' - Security Protocol:     {}'.format(options['security.protocol']))
            print(logging_prefix + ' - SASL Mechanism:        {}'.format(options['sasl.mechanisms']))
            print(logging_prefix + ' - SASL Username:         {}'.format(options['sasl.username']))
            print(logging_prefix + ' - SASL Password:         {}'.format(obfuscated_password))
            if (os.path.isfile(os.getenv('KAFKA_CERT','/certs/es-cert.pem'))): 
                print(logging_prefix + ' - SSL CA Location:       {}'.format(options['ssl.ca.location']))
        print(logging_prefix + ' - Offset Reset:          {}'.format(options['auto.offset.reset']))
        print(logging_prefix + ' - Autocommit:            {}'.format(options['enable.auto.commit']))
        print(logging_prefix + ' - -------------------------------------------')