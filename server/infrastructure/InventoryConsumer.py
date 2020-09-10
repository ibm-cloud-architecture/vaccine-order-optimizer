import json, threading, time
from userapp.server.infrastructure.kafka.KafkaAvroConsumer import KafkaAvroConsumer
import userapp.server.infrastructure.kafka.EventBackboneConfig as EventBackboneConfig
import userapp.server.infrastructure.kafka.avroUtils as avroUtils
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
        self.cloudEvent_schema = avroUtils.getCloudEventSchema()
        self.kafkaconsumer=KafkaAvroConsumer(json.dumps(self.cloudEvent_schema.to_json()),
                                            EventBackboneConfig.getInventoryTopicName(),
                                            "InventoryConsumer", False)

    def startProcessing(self):
        x = threading.Thread(target=self.processEvents, daemon=True)
        print("[InventoryConsumer] - Starting to consume Events")
        x.start()
    
    def processEvents(self):
        while True:
            event = self.kafkaconsumer.pollNextRawEvent()
            if event is not None:
                print('[InventoryConsumer] - New event consumed: ' + json.dumps(event.value()))
                event_json = event.value()['data']
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
                self.events[event.key()] = event_json
            # time.sleep(3)
    
    def getEvents(self):
        return self.events
    
    def getEventsPanda(self):
        return pd.DataFrame.from_dict(self.events_panda)