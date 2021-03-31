import json, threading, time, os, uuid, datetime
from server.infrastructure.kafka.KafkaAvroCDCConsumer import KafkaAvroCDCConsumer
from server.infrastructure.kafka.KafkaAvroProducer import KafkaAvroProducer
import server.infrastructure.kafka.EventBackboneConfig as EventBackboneConfig
from server.infrastructure.OrderDataStore import OrderDataStore
from server.domain.doaf_vaccine_order_optimizer import VaccineOrderOptimizer
import server.infrastructure.kafka.avroUtils as avroUtils
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
        # Avro data schemas location
        self.schemas_location = "/app/data/avro/schemas/"
        # CloudEvent Schema
        self.cloudEvent_schema = avroUtils.getCloudEventSchema()
        # Build the Kafka Avro Producers
        self.kafkaproducer = KafkaAvroProducer("OrderConsumer",json.dumps(self.cloudEvent_schema.to_json()),"VOO-ShipmentPlan")
        
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
            start_date=date.today(), debug=self.debugOptimization)
            # start_date=date(2020, 9, 1), debug=self.debugOptimization)

        optimizer.prepare_data(self.orderStore.getOrdersAsPanda(),
                            self.reeferStore.getAllReefersAsPanda(),
                            self.inventoryStore.getAllLotInventoryAsPanda(),
                            self.transporationStore.getAllTransportationsAsPanda())
        optimizer.optimize()
        if self.debugOptimization:
            print('[OrderConsumer] - optimizeOrder summary:')
            print(optimizer.getLogs())

            print('\n[OrderConsumer] - optimizeOrder retults:')
            print(optimizer.get_sol_json())
        self.produceShipmentPlan(optimizer.get_sol_json())

    def debugOptimization(self):
        return (os.getenv('DEBUG_OPTIMIZATION','False') != 'False')

    def produceShipmentPlan(self, sol_json):
        print('\n[OrderConsumer] - producing shipment plan')
        
        shipments = json.loads(sol_json['Shipments'])
        shipments_clean = []
        if len(shipments) > 0:
            for shipment in shipments:
                shipments_clean.append({x.replace(' ', ''): v for x, v in shipment.items()})
            event_json = {}
            event_json['type'] = "ibm.gse.eda.vaccine.orderoptimizer.VaccineOrderCloudEvent"
            event_json['specversion'] = "1.0"
            event_json['source'] = "Vaccine Order Optimizer engine"
            event_json['id'] = str(uuid.uuid4())
            event_json['time'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            event_json['dataschema'] = "https://raw.githubusercontent.com/ibm-cloud-architecture/vaccine-order-optimizer/master/data/avro/schemas/shipment_plan.avsc"
            event_json['datacontenttype'] =	"application/json"
            event_json['data'] = { "Shipments": shipments_clean}
            if self.debugOptimization:
                print('[OrderConsumer] - Shipment Plan event to be produced:')
                print(event_json)
            self.kafkaproducer.publishEvent(event_json['id'],event_json,EventBackboneConfig.getShipmentPlanTopicName())
        else:
            print('[ERROR] - There is no shipment plan.')