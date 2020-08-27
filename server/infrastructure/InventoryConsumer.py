import logging, json, threading, time
from userapp.server.infrastructure.kafka.KafkaConsumer import KafkaConsumer
import userapp.server.infrastructure.kafka.EventBackboneConfig as EventBackboneConfig

class InventoryConsumer:
    """ 
    This class is meant to be instantiated once when the application starts up in order
    to consume events from the Kafka Topics
    """
    def __init__(self):
        print("[InventoryConsumer] - Initializing the consumer")
        self.events={}
        self.kafkaconsumer=KafkaConsumer(EventBackboneConfig.getInventoryTopicName())
        self.kafkaconsumer.prepareConsumer('InventoryConsumer')

    def startProcessing(self):
        x = threading.Thread(target=self.processEvents, daemon=True)
        print("[InventoryConsumer] - Starting to consume Events")
        x.start()
    
    def processEvents(self):
        while True:
            event = self.kafkaconsumer.pollNextRawEvent()
            if event is not None:
                print('[InventoryConsumer] - New event consumed: ' + event.value().decode('utf-8'))
                self.events[event.key().decode('utf-8')] = json.loads(event.value().decode('utf-8'))
            time.sleep(10)
    
    def getEvents(self):
        return self.events