import json, threading, time
from server.infrastructure.kafka.KafkaAvroConsumer import KafkaAvroConsumer
import server.infrastructure.kafka.EventBackboneConfig as EventBackboneConfig
import server.infrastructure.kafka.avroUtils as avroUtils
from server.infrastructure.InventoryDataStore import InventoryDataStore
import logging

class InventoryConsumer:
    """ 
    This class is meant to be instantiated once when the application starts up in order
    to consume events about vaccine lots manufactured 
    """
    
    def __init__(self):
        logging.debug("[InventoryConsumer] - Initializing the consumer")
        self.cloudEvent_schema = avroUtils.getCloudEventSchema()
        self.store = InventoryDataStore.getInstance()
        self.kafkaconsumer=KafkaAvroConsumer(json.dumps(self.cloudEvent_schema.to_json()),
                                        EventBackboneConfig.getInventoryTopicName(),
                                        EventBackboneConfig.getConsumerGroup(), False)
        
    def startProcessing(self):
        x = threading.Thread(target=self.processEvents, daemon=True)
        logging.debug("[InventoryConsumer] - Starting to consume Events")
        x.start()
    
    def processEvents(self):   
        while True:
            event = self.kafkaconsumer.pollNextRawEvent()
            if event is not None:
                logging.debug('[InventoryConsumer] - New event consumed: ' + json.dumps(event.value()))
                event_json = event.value()['data']
                self.store.addLotToInventory(event.key(),event_json)
                