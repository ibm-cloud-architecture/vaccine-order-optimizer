import avro.schema
import json

def getCloudEventSchema(schema_files_location = "/app/data/avro/schemas/",
                        cloudEvent = "cloudEvent.avsc",
                        inventory = "inventory.avsc",
                        reefer = "reefer.avsc",
                        transportation = "transportation.avsc"):
  # Read all the schemas needed in order to produce the final Container Event Schema
  known_schemas = avro.schema.Names()
  inventory_schema = LoadAvsc(schema_files_location + inventory, known_schemas)
  reefer_schema = LoadAvsc(schema_files_location + reefer, known_schemas)
  transportation_schema = LoadAvsc(schema_files_location + transportation, known_schemas)
  cloudEvent_schema = LoadAvsc(schema_files_location + cloudEvent, known_schemas)
  return cloudEvent_schema

def getSchema(schemas_location, schema_name):
  # Get the Inventory events Avro data schema
  known_schemas = avro.schema.Names()
  schema = LoadAvsc(schemas_location + schema_name, known_schemas)
  return schema
  
def LoadAvsc(file_path, names=None):
  # Load avsc file
  # file_path: path to schema file
  # names(optional): avro.schema.Names object
  file_text = open(file_path).read()
  json_data = json.loads(file_text)
  schema = avro.schema.SchemaFromJSONData(json_data, names)
  return schema