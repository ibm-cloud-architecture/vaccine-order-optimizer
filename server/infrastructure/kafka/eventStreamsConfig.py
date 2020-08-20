import os

KAFKA_BROKERS = os.getenv('KAFKA_BROKERS','localhost:9092')
KAFKA_CERT = os.getenv('KAFKA_CERT','/certs/es-cert.pem')
KAFKA_USER =  os.getenv('KAFKA_USER','token')
KAFKA_PWD =  os.getenv('KAFKA_PWD','')
KAFKA_SASL_MECHANISM=  os.getenv('KAFKA_SASL_MECHANISM','SCRAM-SHA-512')
