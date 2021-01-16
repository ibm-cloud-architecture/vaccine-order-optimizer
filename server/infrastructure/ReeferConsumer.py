import json, threading, time
from server.infrastructure.kafka.KafkaAvroConsumer import KafkaAvroConsumer
import server.infrastructure.kafka.EventBackboneConfig as EventBackboneConfig
import server.infrastructure.kafka.avroUtils as avroUtils

class ReeferConsumer:
    """ 
    This class is meant to be instantiated once when the application starts up in order
    to consume events from the Kafka Topics about Refrigerator containers
    """
    
    def __init__(self,dataStore):
        print("[ReeferConsumer] - Initializing the consumer")
        self.dataStore = dataStore
        self.cloudEvent_schema = avroUtils.getCloudEventSchema()
        self.kafkaconsumer=KafkaAvroConsumer(json.dumps(self.cloudEvent_schema.to_json()),
                                    EventBackboneConfig.getReeferTopicName(),
                                    "ReeferConsumer")
        
    def startProcessing(self):
        x = threading.Thread(target=self.processEvents, daemon=True)
        print("[ReeferConsumer] - Starting to consume Events")
        x.start()
    
    def processEvents(self):
        while True:
            event = self.kafkaconsumer.pollNextRawEvent()
            if event is not None:
                print('[ReeferConsumer] - New event consumed: ' + json.dumps(event.value()))
                event_json = event.value()['data']
                self.dataStore.addReefer(event.key(),event_json)
 