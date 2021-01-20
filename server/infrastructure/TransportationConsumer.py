import json, threading, time
from server.infrastructure.kafka.KafkaAvroConsumer import KafkaAvroConsumer
import server.infrastructure.kafka.EventBackboneConfig as EventBackboneConfig
import server.infrastructure.kafka.avroUtils as avroUtils
from server.infrastructure.DataStore import DataStore

class TransportationConsumer(object):
    """ 
    This class is meant to be instantiated once when the application starts up in order
    to consume events from the Kafka Topics about transportation itinerary available
    """

    def __init__(self):
        print("[TransportationConsumer] - Initializing the consumer")
        self.index=0
        self.cloudEvent_schema = avroUtils.getCloudEventSchema()
        self.kafkaconsumer=KafkaAvroConsumer(json.dumps(self.cloudEvent_schema.to_json()),
                                            EventBackboneConfig.getTransportationTopicName(),
                                            EventBackboneConfig.getConsumerGroup(),False)

    def startProcessing(self):
        x = threading.Thread(target=self.processEvents, daemon=True)
        print("[TransportationConsumer] - Starting to consume Events")
        x.start()
    
    def processEvents(self):
        while True:
            event = self.kafkaconsumer.pollNextRawEvent()
            if event is not None:
                print('[TransportationConsumer] - New event consumed: ' + json.dumps(event.value()))
                event_json = event.value()['data']
                DataStore.getInstance().addTransportation(event.key(),event_json)
            # time.sleep(1)
    