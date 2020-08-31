import json, threading, time
from userapp.server.infrastructure.kafka.KafkaConsumer import KafkaConsumer
import userapp.server.infrastructure.kafka.EventBackboneConfig as EventBackboneConfig
import pandas as pd

class InventoryConsumer:
    """ 
    This class is meant to be instantiated once when the application starts up in order
    to consume events from the Kafka Topics
    """
    def __init__(self):
        print("[InventoryConsumer] - Initializing the consumer")
        self.events_panda={}
        self.events={}
        self.index=0
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
                event_json = json.loads(event.value().decode('utf-8'))
                if ( len(self.events_panda) == 0 ):
                    for key, value in event_json.items():
                        self.events_panda[key] = {0:value}
                    self.index+=1
                else:
                    for key, value in event_json.items():
                        intermediate = self.events_panda[key]
                        intermediate[self.index] = value
                        self.events_panda[key] = intermediate
                    self.index+=1
                self.events[event.key().decode('utf-8')] = json.loads(event.value().decode('utf-8'))
            # time.sleep(3)
    
    def getEvents(self):
        return self.events
    
    def getEventsPanda(self):
        return pd.DataFrame.from_dict(self.events_panda)