import json, time, os, datetime, uuid
from datetime import date
from server.infrastructure.kafka.KafkaAvroProducer import KafkaAvroProducer
import server.infrastructure.kafka.EventBackboneConfig as EventBackboneConfig
import server.infrastructure.kafka.avroUtils as avroUtils

class DataProducer:
    """ 
    This class is meant to be instantiated once when the application starts up in order
    to producer test data to the Kafka Topics
    """
    def __init__(self):
        print("[DataProducer] - Initializing the data producers")
        # Get the data events file locations
        self.inventory_file = "/app/data/txt/inventory.txt"
        self.reefer_file = "/app/data/txt/reefer.txt"
        self.transportation_file = "/app/data/txt/transportation.txt"
        # Get the events Avro data schemas location
        self.schemas_location = "/app/data/avro/schemas/"
        self.cloudEvent_schema = avroUtils.getCloudEventSchema()
        # print(self.cloudEvent_schema.to_json())
        # Build the Kafka Avro Producers
        self.kafkaproducer_inventory = KafkaAvroProducer("DataProducer_Inventory",json.dumps(self.cloudEvent_schema.to_json()),"VOO-Inventory")
        self.kafkaproducer_reefer = KafkaAvroProducer("DataProducer_Reefer",json.dumps(self.cloudEvent_schema.to_json()),"VOO-Reefer")
        self.kafkaproducer_transportation = KafkaAvroProducer("DataProducer_Transportation",json.dumps(self.cloudEvent_schema.to_json()),"VOO-Transportation")

    def produceData(self):
        # self.produceInventoryDataFromFile(EventBackboneConfig.getInventoryTopicName())
        # self.produceReeferDataFromFile(EventBackboneConfig.getReeferTopicName())
        # self.produceTransportationDataFromFile(EventBackboneConfig.getTransportationTopicName())
        self.produceInventoryData(EventBackboneConfig.getInventoryTopicName())
        self.produceReeferData(EventBackboneConfig.getReeferTopicName())
        self.produceTransportationData(EventBackboneConfig.getTransportationTopicName())

    def produceInventoryDataFromFile(self,topic):
        print("[DataProducer] - Producing events in " + self.inventory_file)
        if os.path.isfile(self.inventory_file):
            with open(self.inventory_file) as f:
                lines = [line.rstrip() for line in f]
            for event in lines:
                event_json = {}
                event_json['type'] = "ibm.gse.eda.vaccine.orderoptimizer.VaccineOrderCloudEvent"
                event_json['specversion'] = "1.0"
                event_json['source'] = "Vaccine Order Optimizer producer endpoint"
                event_json['id'] = str(uuid.uuid4())
                event_json['time'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
                event_json['dataschema'] = "https://raw.githubusercontent.com/ibm-cloud-architecture/vaccine-order-optimizer/master/data/avro/schemas/inventory.avsc"
                event_json['datacontenttype'] =	"application/json"
                event_json['data'] = json.loads(event)
                # We might want to publish events with the inventory 'lot_id' field as the key for better partitioning
                # self.kafkaproducer_inventory.publishEvent(event_json['id'],event_json,topic)
                self.kafkaproducer_inventory.publishEvent(event_json['data']['lot_id'],event_json,topic)
        else:
            print('[DataProducer] - ERROR - The file ' + self.inventory_file + ' does not exist')
    
    def produceReeferDataFromFile(self,topic):
        print("[DataProducer] - Producing events in " + self.reefer_file)
        if os.path.isfile(self.reefer_file):
            with open(self.reefer_file) as f:
                lines = [line.rstrip() for line in f]
            for event in lines:
                event_json = {}
                event_json['type'] = "ibm.gse.eda.vaccine.orderoptimizer.VaccineOrderCloudEvent"
                event_json['specversion'] = "1.0"
                event_json['source'] = "Vaccine Order Optimizer producer endpoint"
                event_json['id'] = str(uuid.uuid4())
                event_json['time'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
                event_json['dataschema'] = "https://raw.githubusercontent.com/ibm-cloud-architecture/vaccine-order-optimizer/master/data/avro/schemas/reefer.avsc"
                event_json['datacontenttype'] =	"application/json"
                event_json['data'] = json.loads(event)
                # We might want to publish events with the reefer 'reefer_id' field as the key for better partitioning
                # self.kafkaproducer_reefer.publishEvent(event_json['id'],event_json,topic)
                self.kafkaproducer_reefer.publishEvent(event_json['data']['reefer_id'],event_json,topic)
        else:
            print('[DataProducer] - ERROR - The file ' + self.reefer_file + ' does not exist')
    
    def produceTransportationDataFromFile(self,topic):
        print("[DataProducer] - Producing events in " + self.transportation_file)
        if os.path.isfile(self.transportation_file):
            with open(self.transportation_file) as f:
                lines = [line.rstrip() for line in f]
            for event in lines:
                event_json = {}
                event_json['type'] = "ibm.gse.eda.vaccine.orderoptimizer.VaccineOrderCloudEvent"
                event_json['specversion'] = "1.0"
                event_json['source'] = "Vaccine Order Optimizer producer endpoint"
                event_json['id'] = str(uuid.uuid4())
                event_json['time'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
                event_json['dataschema'] = "https://raw.githubusercontent.com/ibm-cloud-architecture/vaccine-order-optimizer/master/data/avro/schemas/transportation.avsc"
                event_json['datacontenttype'] =	"application/json"
                event_json['data'] = json.loads(event)
                # We might want to publish events with the transportation 'lane_id' field as the key for better partitioning
                # self.kafkaproducer_transportation.publishEvent(event_json['id'],event_json,topic)
                self.kafkaproducer_transportation.publishEvent(event_json['data']['lane_id'],event_json,topic)
        else:
            print('[DataProducer] - ERROR - The file ' + self.transportation_file + ' does not exist')

    def produceInventoryData(self,topic):
        print("[DataProducer] - Producing inventory events")
        inventory = [
            {"lot_id":"L0001","qty":200,"location":"Beerse, Belgium","date_available":""},
            {"lot_id":"L0002","qty":200,"location":"Beerse, Belgium","date_available":""},
            {"lot_id":"L0003","qty":200,"location":"Beerse, Belgium","date_available": date.today().strftime('%m/%d/%Y')},
            {"lot_id":"L0004","qty":200,"location":"Beerse, Belgium","date_available": (date.today() + datetime.timedelta(days=1)).strftime('%m/%d/%Y')},
            {"lot_id":"L0005","qty":200,"location":"Beerse, Belgium","date_available": (date.today() + datetime.timedelta(days=2)).strftime('%m/%d/%Y')},
            {"lot_id":"L0006","qty":200,"location":"Beerse, Belgium","date_available": (date.today() + datetime.timedelta(days=4)).strftime('%m/%d/%Y')},
            {"lot_id":"L0007","qty":200,"location":"Beerse, Belgium","date_available": (date.today() + datetime.timedelta(days=6)).strftime('%m/%d/%Y')},
            {"lot_id":"L0008","qty":200,"location":"Beerse, Belgium","date_available": (date.today() + datetime.timedelta(days=7)).strftime('%m/%d/%Y')},
            {"lot_id":"L0016","qty":200,"location":"Titusville, N.J.","date_available":""},
            {"lot_id":"L0017","qty":200,"location":"Titusville, N.J.","date_available": date.today().strftime('%m/%d/%Y')},
            {"lot_id":"L0018","qty":200,"location":"Titusville, N.J.","date_available": (date.today() + datetime.timedelta(days=1)).strftime('%m/%d/%Y')},
            {"lot_id":"L0019","qty":200,"location":"Titusville, N.J.","date_available": (date.today() + datetime.timedelta(days=2)).strftime('%m/%d/%Y')}
        ]
        for item in inventory:
            event_json = {}
            event_json['type'] = "ibm.gse.eda.vaccine.orderoptimizer.VaccineOrderCloudEvent"
            event_json['specversion'] = "1.0"
            event_json['source'] = "Vaccine Order Optimizer producer endpoint"
            event_json['id'] = str(uuid.uuid4())
            event_json['time'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            event_json['dataschema'] = "https://raw.githubusercontent.com/ibm-cloud-architecture/vaccine-order-optimizer/master/data/avro/schemas/inventory.avsc"
            event_json['datacontenttype'] =	"application/json"
            event_json['data'] = item
            # We might want to publish events with the inventory 'lot_id' field as the key for better partitioning
            self.kafkaproducer_inventory.publishEvent(event_json['data']['lot_id'],event_json,topic)

    def produceReeferData(self,topic):
        print("[DataProducer] - Producing reefer events")
        reefer = [
            {"reefer_id":"RC00001","status":"Ready","location":"Beerse, Belgium","date_available":""},
            {"reefer_id":"RC00002","status":"Ready","location":"Beerse, Belgium","date_available":""},
            {"reefer_id":"RC00003","status":"Ready","location":"Titusville, N.J.","date_available":""},
            {"reefer_id":"RC00004","status":"Ready","location":"Titusville, N.J.","date_available":""},
            {"reefer_id":"RC00005","status":"In-Transit","location":"Paris, France","date_available": date.today().strftime('%m/%d/%Y')},
            {"reefer_id":"RC00006","status":"In-Transit","location":"Paris, France","date_available": (date.today() + datetime.timedelta(days=2)).strftime('%m/%d/%Y')},
            {"reefer_id":"RC00007","status":"In-Transit","location":"Paris, France","date_available": (date.today() + datetime.timedelta(days=2)).strftime('%m/%d/%Y')},
            {"reefer_id":"RC00008","status":"In-Transit","location":"London, England","date_available": date.today().strftime('%m/%d/%Y')},
            {"reefer_id":"RC00009","status":"In-Transit","location":"London, England","date_available": (date.today() + datetime.timedelta(days=2)).strftime('%m/%d/%Y')},
            {"reefer_id":"RC00010","status":"In-Transit","location":"London, England","date_available": (date.today() + datetime.timedelta(days=2)).strftime('%m/%d/%Y')}
        ]
        for container in reefer:
            event_json = {}
            event_json['type'] = "ibm.gse.eda.vaccine.orderoptimizer.VaccineOrderCloudEvent"
            event_json['specversion'] = "1.0"
            event_json['source'] = "Vaccine Order Optimizer producer endpoint"
            event_json['id'] = str(uuid.uuid4())
            event_json['time'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            event_json['dataschema'] = "https://raw.githubusercontent.com/ibm-cloud-architecture/vaccine-order-optimizer/master/data/avro/schemas/reefer.avsc"
            event_json['datacontenttype'] =	"application/json"
            event_json['data'] = container
            # We might want to publish events with the reefer 'reefer_id' field as the key for better partitioning
            self.kafkaproducer_reefer.publishEvent(event_json['data']['reefer_id'],event_json,topic)
    
    def produceTransportationData(self,topic):
        print("[DataProducer] - Producing transportation events")
        transportation = [
            {"lane_id":"1","from_loc":"Beerse, Belgium","to_loc":"Paris, France","transit_time":2,"reefer_cost":10,"fixed_cost":20},
            {"lane_id":"2","from_loc":"Titusville, N.J.","to_loc":"London, England","transit_time":2,"reefer_cost":12,"fixed_cost":20},
            {"lane_id":"3","from_loc":"Beerse, Belgium","to_loc":"London, England","transit_time":2,"reefer_cost":11,"fixed_cost":20},
            {"lane_id":"4","from_loc":"Titusville, N.J.","to_loc":"Paris, France","transit_time":2,"reefer_cost":13,"fixed_cost":20},
            {"lane_id":"5","from_loc":"Paris, France","to_loc":"Beerse, Belgium","transit_time":1,"reefer_cost":6,"fixed_cost":10},
            {"lane_id":"6","from_loc":"London, England","to_loc":"Titusville, N.J.","transit_time":2,"reefer_cost":6,"fixed_cost":10},
            {"lane_id":"7","from_loc":"Paris, France","to_loc":"Beerse, Belgium","transit_time":1,"reefer_cost":5,"fixed_cost":10},
            {"lane_id":"8","from_loc":"London, England","to_loc":"Titusville, N.J.","transit_time":2,"reefer_cost":6,"fixed_cost":10},
            {"lane_id":"9","from_loc":"Beerse, Belgium","to_loc":"Titusville, N.J.","transit_time":1,"reefer_cost":5,"fixed_cost":10},
            {"lane_id":"10","from_loc":"Titusville, N.J.","to_loc":"Beerse, Belgium","transit_time":1,"reefer_cost":5,"fixed_cost":10},
            {"lane_id":"11","from_loc":"Paris, France","to_loc":"Paris, France","transit_time":1,"reefer_cost":2,"fixed_cost":0},
            {"lane_id":"12","from_loc":"London, England","to_loc":"London, England","transit_time":1,"reefer_cost":2,"fixed_cost":0},
            {"lane_id":"13","from_loc":"Beerse, Belgium","to_loc":"Beerse, Belgium","transit_time":1,"reefer_cost":1,"fixed_cost":0},
            {"lane_id":"14","from_loc":"Titusville, N.J.","to_loc":"Titusville, N.J.","transit_time":1,"reefer_cost":1,"fixed_cost":0}
        ]
        for lane in transportation:
            event_json = {}
            event_json['type'] = "ibm.gse.eda.vaccine.orderoptimizer.VaccineOrderCloudEvent"
            event_json['specversion'] = "1.0"
            event_json['source'] = "Vaccine Order Optimizer producer endpoint"
            event_json['id'] = str(uuid.uuid4())
            event_json['time'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            event_json['dataschema'] = "https://raw.githubusercontent.com/ibm-cloud-architecture/vaccine-order-optimizer/master/data/avro/schemas/transportation.avsc"
            event_json['datacontenttype'] =	"application/json"
            event_json['data'] = lane
            # We might want to publish events with the transportation 'lane_id' field as the key for better partitioning
            self.kafkaproducer_transportation.publishEvent(event_json['data']['lane_id'],event_json,topic)