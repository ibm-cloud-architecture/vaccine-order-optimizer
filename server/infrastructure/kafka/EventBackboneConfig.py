import os

KAFKA_ENV = 'OCP'
KAFKA_BROKERS = 'minimal-prod-kafka-bootstrap-eventstreams.gse-eda-demo-2020-08-fa9ee67c9ab6a7791435450358e564cc-0000.us-south.containers.appdomain.cloud:443'
KAFKA_CERT = '/project/userapp/certs/es-cert.pem'
KAFKA_USER =  'jesus'
KAFKA_PASSWORD =  'stFTCEIli4dP'
# KAFKA_BROKERS = os.getenv('KAFKA_BROKERS','localhost:9092')
# KAFKA_CERT = os.getenv('KAFKA_CERT','/certs/es-cert.pem')
# KAFKA_USER =  os.getenv('KAFKA_USER','token')
# KAFKA_PWD =  os.getenv('KAFKA_PWD','')
# KAFKA_SASL_MECHANISM=  os.getenv('KAFKA_SASL_MECHANISM','SCRAM-SHA-512')


def getKafkaEnv():
    return KAFKA_ENV

def getKafkaBroker():
    return KAFKA_BROKERS

def getKafkaPassword():
    return KAFKA_PASSWORD

def getKafkaUser():
    return KAFKA_USER

# For now we are going to use Event Streams on Prem
###################################################
# def isSecured():
#     return KAFKA_PASSWORD != ''

# def isEncrypted():
#     #return KAFKA_CERT != ''
#     return os.path.isfile(KAFKA_CERT)

def getKafkaCertificate():
    return KAFKA_CERT

def getContainerTopicName():
    return os.getenv("CONTAINER_TOPIC","jesus-containers")
