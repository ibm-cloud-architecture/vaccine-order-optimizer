import json, threading, time
from server.infrastructure.kafka.KafkaAvroConsumer import KafkaAvroConsumer
import server.infrastructure.kafka.EventBackboneConfig as EventBackboneConfig
import server.infrastructure.kafka.avroUtils as avroUtils
from server.infrastructure.InventoryDataStore import InventoryDataStore
import logging
import pandas as pd 

AUTO_COMMIT = False

class InventoryConsumer:
    
    instance = None

    @classmethod
    def getInstance(cls):
        if cls.instance == None:
            cls.instance = InventoryConsumer()
        return cls.instance 

    """ 
    This class is meant to be instantiated once when the application starts up in order
    to consume events about vaccine lots manufactured 
    """
    def __init__(self):
        logging.info("[InventoryConsumer] - Initializing the consumer")
        self.cloudEvent_schema = avroUtils.getCloudEventSchema()
        self.store = InventoryDataStore()
        self.kafkaconsumer=KafkaAvroConsumer(json.dumps(self.cloudEvent_schema.to_json()),
                                        EventBackboneConfig.getInventoryTopicName(),
                                        EventBackboneConfig.getConsumerGroup(), AUTO_COMMIT)
        
    def startProcessing(self):
        x = threading.Thread(target=self.processEvents, daemon=True)
        logging.info("[InventoryConsumer] - Starting to consume Events")
        x.start()
    
    def processEvents(self):
        try:   
            while True:
                event = self.kafkaconsumer.pollNextRawEvent()
                if event is not None:
                    logging.info('[InventoryConsumer] - New event consumed: ' + json.dumps(event.value()))
                    event_json = event.value()['data']
                    self.store.addLotToInventory(event.key(),event_json)
                    if not AUTO_COMMIT:
                        self.kafkaconsumer.commitEvent(event)
        finally:
            self.kafkaconsumer.close()
    

    def getStore(self):
        return self.store            