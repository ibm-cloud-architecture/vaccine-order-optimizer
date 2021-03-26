import json, threading, time, os
from server.infrastructure.kafka.KafkaAvroCDCConsumer import KafkaAvroCDCConsumer
import server.infrastructure.kafka.EventBackboneConfig as EventBackboneConfig
from server.infrastructure.OrderDataStore import OrderDataStore
# from server.infrastructure.ReeferDataStore import ReeferDataStore
# from server.infrastructure.InventoryDataStore import InventoryDataStore
# from server.infrastructure.TransportationDataStore import TransportationDataStore
from server.domain.doaf_vaccine_order_optimizer import VaccineOrderOptimizer
import logging
import pandas as pd
from datetime import date

AUTO_COMMIT = False

class OrderConsumer:
    
    instance = None

    @classmethod
    def getInstance(cls,inventoryStore,reeferStore,transportationStore):
        if cls.instance == None:
            cls.instance = OrderConsumer(inventoryStore,reeferStore,transportationStore)
        return cls.instance 

    """ 
    This class is meant to be instantiated once when the application starts up in order
    to consume events about vaccine order coming for the order manager service
    """
    def __init__(self,inventoryStore,reeferStore,transportationStore):
        print("[OrderConsumer] - Initializing the consumer")
        self.debugOptimization = self.debugOptimization()
        self.orderStore = OrderDataStore.getInstance()
        self.reeferStore = reeferStore
        self.inventoryStore = inventoryStore
        self.transporationStore = transportationStore
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
                    self.orderStore.processOrder(order_json['orderID'],order_json)
                    if not AUTO_COMMIT:
                        self.kafkaconsumer.commitEvent(event)
                    # Optimize Order
                    self.optimizeOrder()     
        except Exception as e: 
            print("Error " + str(e))
        finally:
            self.kafkaconsumer.close()
    

    def getStore(self):
        return self.orderStore

    def optimizeOrder(self):
        # Call optimize
        print('[OrderConsumer] - calling optimizeOrder')
        # Create the optimizer
        optimizer = VaccineOrderOptimizer(
            start_date=date(2020, 9, 1), debug=self.debugOptimization)
        # print('00000000000000')
        # print(self.orderStore.getOrdersAsPanda())
        # print('11111111111111')
        # print(self.reeferStore.getAllReefersAsPanda())
        # print('22222222222222')
        # print(self.inventoryStore.getAllLotInventoryAsPanda())
        # print('33333333333333')
        # print(self.transporationStore.getAllTransportationsAsPanda())
        optimizer.prepare_data(self.orderStore.getOrdersAsPanda(),
                            self.reeferStore.getAllReefersAsPanda(),
                            self.inventoryStore.getAllLotInventoryAsPanda(),
                            self.transporationStore.getAllTransportationsAsPanda())
        optimizer.optimize()
        print('------LOGS------')
        print(optimizer.getLogs())

        # Get the optimization solution
        plan_orders, plan_orders_details, plan_shipments = optimizer.get_sol_panda()
        result = "Orders\n"
        result += "------------------\n"
        result += plan_orders.to_string() + "\n\n"
        result += "Order Details\n"
        result += "------------------\n"
        result += plan_orders_details.to_string() + "\n\n"
        result += "Shipments\n"
        result += "------------------\n"
        result += plan_shipments.to_string()
        print('XXXXXXXXXXXXXXXXXXXXXXXX')
        print(result)

    def debugOptimization(self):
        return (os.getenv('DEBUG_OPTIMIZATION','False') != 'False')