import logging, json, threading, time
from userapp.server.infrastructure.kafka.KafkaConsumer import KafkaConsumer
import userapp.server.infrastructure.kafka.EventBackboneConfig as EventBackboneConfig

class ContainerConsumer:
    """ 
    This class is meant to be instantiated once when the application starts up in order
    to consume events from the Kafka Topics
    """
    def __init__(self):
        print("[ContainerConsumer] - Initializing the container consumer")
        self.containers={}
        self.kafkaconsumer=KafkaConsumer(EventBackboneConfig.getKafkaEnv(),
                                        EventBackboneConfig.getKafkaBroker(),
                                        EventBackboneConfig.getKafkaUser(),
                                        EventBackboneConfig.getKafkaPassword(),
                                        EventBackboneConfig.getKafkaCertificate(),
                                        EventBackboneConfig.getContainerTopicName(),
                                        'earliest')
        self.kafkaconsumer.prepareConsumer('ContainerConsumer')

    def startProcessing(self):
        x = threading.Thread(target=self.processEvents, daemon=True)
        print("[ContainerConsumer] - Starting to consume Contanier Events")
        x.start()
    
    def processEvents(self):
        while True:
            event = self.kafkaconsumer.pollNextRawEvent()
            if event is not None:
                # print("[ContainerConsumer] - [Thread] - New Event: " + json.dumps(event))
                self.containers[event.key().decode('utf-8')] = json.loads(event.value().decode('utf-8'))
            print("[ContainerConsumer] - Sleeping 10")
            time.sleep(10)
    
    def getContainers(self):
        return self.containers