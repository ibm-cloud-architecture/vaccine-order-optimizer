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

def getConsumerGroup():
    return os.getenv("CONSUMER_GROUP","vaccine-optimizer")
