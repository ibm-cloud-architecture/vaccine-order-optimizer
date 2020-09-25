import logging
from server.infrastructure.kafka.KafkaProducer import KafkaProducer
import server.infrastructure.kafka.eventBackboneConfig as config

if __name__ == '__main__':
    print("Start Reefer Event Producer")
    logging.basicConfig(level=logging.INFO)
    producer = KafkaProducer(kafka_brokers = config.KAFKA_BROKERS, 
                kafka_user = config.KAFKA_USER, 
                kafka_pwd = config.KAFKA_PWD, 
                kafka_cacert = config.KAFKA_CERT, 
                kafka_sasl_mechanism=config.KAFKA_SASL_MECHANISM,
                topic_name = "vaccine-reefers")
    producer.prepare("ReeferProducer-1")
    reefer = {'REEFER_ID': "R001",'STATUS':'Ready', 'LOCATION': 'Beerse, Belgium'}
    producer.publishEvent(reefer,"REEFER_ID")
    