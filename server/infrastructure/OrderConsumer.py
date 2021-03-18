import json, threading, time
from server.infrastructure.kafka.KafkaAvroCDCConsumer import KafkaAvroCDCConsumer
import server.infrastructure.kafka.EventBackboneConfig as EventBackboneConfig
from server.infrastructure.OrderDataStore import OrderDataStore
import logging
import pandas as pd 

AUTO_COMMIT = False

class OrderConsumer:
    
    instance = None

    @classmethod
    def getInstance(cls):
        if cls.instance == None:
            cls.instance = OrderConsumer()
        return cls.instance 

    """ 
    This class is meant to be instantiated once when the application starts up in order
    to consume events about vaccine order coming for the order manager service
    """
    def __init__(self):
        # logging.info("[OrderConsumer] - Initializing the consumer")
        print("[OrderConsumer] - Initializing the consumer")
        self.store = OrderDataStore.getInstance()
        self.kafkaconsumer=KafkaAvroCDCConsumer('OrderConsumer',
                                                EventBackboneConfig.getOrderTopicName(),
                                                EventBackboneConfig.getConsumerGroup(),
                                                AUTO_COMMIT)
        
    def startProcessing(self):
        x = threading.Thread(target=self.processEvents, daemon=True)
        # logging.info("[OrderConsumer] - Starting to consume Events from " + EventBackboneConfig.getOrderTopicName())
        x.start()
    
    def processEvents(self):
        print("[OrderConsumer] - Starting to consume events")
        try:   
            while True:
                # logging.info("[OrderConsumer] - consume Events")
                event = self.kafkaconsumer.pollNextRawEvent()
                if event is not None:
                    event_value = event.value()
                    #logging.info('[OrderConsumer] - New event consumed: ' + json.dumps(event.value()))
                    order_json = json.loads(event_value['after']['payload'])
                    print('[OrderConsumer] - New Order: ' + json.dumps(order_json))
                    self.store.processOrder(order_json['orderID'],order_json)
                    if not AUTO_COMMIT:
                        self.kafkaconsumer.commitEvent(event)
        except Exception as e: 
            print("Error " + str(e))
        finally:
            self.kafkaconsumer.close()
    

    def getStore(self):
        return self.store            