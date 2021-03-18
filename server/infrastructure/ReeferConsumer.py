import json, threading, time
from server.infrastructure.kafka.KafkaAvroConsumer import KafkaAvroConsumer
import server.infrastructure.kafka.EventBackboneConfig as EventBackboneConfig
import server.infrastructure.kafka.avroUtils as avroUtils
from server.infrastructure.ReeferDataStore import ReeferDataStore
import logging

AUTO_COMMIT = False

class ReeferConsumer:

    instance = None

    @classmethod
    def getInstance(cls):
        if cls.instance == None:
            cls.instance = ReeferConsumer()
        return cls.instance 

    """ 
    This class is meant to be instantiated once when the application starts up in order
    to consume events from the Kafka Topics about Refrigerator containers
    """
    
    def __init__(self):
        # logging.info("[ReeferConsumer] - Initializing the consumer")
        print("[ReeferConsumer] - Initializing the consumer")
        self.store = ReeferDataStore()
        self.cloudEvent_schema = avroUtils.getCloudEventSchema()
        self.kafkaconsumer=KafkaAvroConsumer('ReeferConsumer',
                                            json.dumps(self.cloudEvent_schema.to_json()),
                                            EventBackboneConfig.getReeferTopicName(),
                                            EventBackboneConfig.getConsumerGroup(),
                                            AUTO_COMMIT)
        
    def startProcessing(self):
        x = threading.Thread(target=self.processEvents, daemon=True)
        # logging.info("[ReeferConsumer] - Starting to consume Events")
        x.start()
    
    def processEvents(self):
        print("[ReeferConsumer] - Starting to consume events")
        try:
            while True:
                event = self.kafkaconsumer.pollNextRawEvent()
                if event is not None:
                    logging.info('[ReeferConsumer] - New event consumed: ' + json.dumps(event.value()))
                    event_json = event.value()['data']
                    self.store.addReefer(event.key(),event_json)
                    if not AUTO_COMMIT:
                        self.kafkaconsumer.commitEvent(event)
        finally:
                self.kafaconsumer.close()

    def getStore(self):
        return self.store    