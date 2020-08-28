import json, time, os
from userapp.server.infrastructure.kafka.KafkaProducer import KafkaProducer
import userapp.server.infrastructure.kafka.EventBackboneConfig as EventBackboneConfig

class DataProducer:
    """ 
    This class is meant to be instantiated once when the application starts up in order
    to producer test data to the Kafka Topics
    """
    def __init__(self):
        print("[DataProducer] - Initializing the data producer")
        self.kafkaproducer = KafkaProducer()
        self.kafkaproducer.prepareProducer()

    def produceData(self,path="/project/userapp/data/txt"):
        self.processFile(path + '/inventory.txt',EventBackboneConfig.getInventoryTopicName(),'lot_id')
        self.processFile(path + '/reefer.txt',EventBackboneConfig.getReeferTopicName(),'reefer_id')
        self.processFile(path + '/transportation.txt',EventBackboneConfig.getTransportationTopicName(),'lane_id')
    
    def processFile(self,file, topic, key):
        print("[DataProducer] - Producing events in " + file)
        if os.path.isfile(file):
            with open(file) as f:
                lines = [line.rstrip() for line in f]
            for event in lines:
                event_json = json.loads(event)
                self.kafkaproducer.publishEvent(topic,event_json,key)
        else:
            print('[DataProducer] - ERROR - The file ' + file + ' does not exist')
        
        
        
        #     return "File inventory.txt does not exist"