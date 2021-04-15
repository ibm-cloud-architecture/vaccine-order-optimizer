from server.infrastructure.OrderConsumer import OrderConsumer
from flask import Flask, redirect, abort, Response, Blueprint
from flasgger import Swagger
import os, sys
from datetime import datetime
import logging

for idx in range(1, len(sys.argv)):
    arg=sys.argv[idx]
    if arg == "--log":
      loglevel = sys.argv[idx+1]
      numeric_level = getattr(logging, loglevel.upper(), None)
      print(numeric_level)
      logging.basicConfig(level=numeric_level)

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

# Import the Blueprints
from server.api.orderResource import orders_blueprint
from server.api.optimize import optimize_blueprint, OrderOptimizer, optimizerAPI
from server.api.inventoryResource import data_inventory_blueprint, inventoryApi, DataInventory, DataInventoryPandas
from server.api.reeferResource import data_reefer_blueprint, reeferApi, ReeferResource, ReeferPandasResource
from server.api.transportationResource import data_transportation_blueprint, transportApi, TransportationResource, TransportationPandasResource
from server.api.health import health_bp
from server.api.prometheus import metrics_bp

from server.infrastructure.ReeferConsumer import ReeferConsumer
from server.infrastructure.InventoryConsumer import InventoryConsumer
from server.infrastructure.TransportationConsumer import TransportationConsumer


# Flask configuration
app = Flask(__name__)
# order_consumer = OrderConsumer.getInstance()

# Inventory
inventory_consumer = InventoryConsumer.getInstance()
inventoryApi.add_resource(DataInventory, 
                          "/api/v1/data/inventory",
                          resource_class_kwargs={'inventoryStore': inventory_consumer.getStore()})
inventoryApi.add_resource(DataInventoryPandas, 
                          "/api/v1/data/inventory/pandas",
                          resource_class_kwargs={'inventoryStore': inventory_consumer.getStore()})

# Reefer
reefer_consumer = ReeferConsumer.getInstance()
reeferApi.add_resource(ReeferResource, 
                      "/api/v1/data/reefers",
                      resource_class_kwargs={'reeferStore': reefer_consumer.getStore()})
reeferApi.add_resource(ReeferPandasResource, 
                          "/api/v1/data/reefers/pandas",
                          resource_class_kwargs={'reeferStore': reefer_consumer.getStore()})

# Transportation
transportation_consumer = TransportationConsumer.getInstance()
transportApi.add_resource(TransportationResource,
                          "/api/v1/data/transportations",
                          resource_class_kwargs={'transportationStore': transportation_consumer.getStore()})
transportApi.add_resource(TransportationPandasResource, 
                          "/api/v1/data/transportations/pandas",
                          resource_class_kwargs={'transportationStore': transportation_consumer.getStore()})

# Order
order_consumer = OrderConsumer.getInstance(inventory_consumer.getStore(),reefer_consumer.getStore(),transportation_consumer.getStore())

app.register_blueprint(orders_blueprint)
app.register_blueprint(optimize_blueprint)
app.register_blueprint(data_inventory_blueprint)
app.register_blueprint(data_reefer_blueprint)
app.register_blueprint(data_transportation_blueprint)
app.register_blueprint(health_bp)
app.register_blueprint(metrics_bp)


# Print JSON responses with indentation for our flask application
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = os.getenv('JSONIFY_PRETTYPRINT_REGULAR', False)

# The python-flask stack includes the flask extension flasgger, which will build
# and publish your swagger ui and specification at the /apidocs url. Here we set up
# the basic swagger attributes, which you should modify to match you application.
# See: https://github.com/rochacbruno-archive/flasgger
swagger_template = {
  "swagger": "2.0",
  "info": {
    "title": "Vaccine Order to Reefer allocation optimizer",
    "description": "API for getting optimized vaccine order shipment plan",
    "contact": {
      "responsibleOrganization": "IBM",
      "email": "boyerje@us.ibm.com",
      "url": "https://ibm-cloud-architecture.github.io",
    },
    "version": os.getenv("APP_VERSION","0.0.3")
  },
  "schemes": [
    "http"
  ],
}

swagger = Swagger(app, template=swagger_template)

# Start the consumers
reefer_consumer.startProcessing()
transportation_consumer.startProcessing()
inventory_consumer.startProcessing()
order_consumer.startProcessing()

# It is considered bad form to return an error for '/', so let's redirect to the apidocs
@app.route('/')
def index():
    return redirect('/apidocs')

if __name__ == "__main__":
  # Start the consumers
  reefer_consumer.startProcessing()
  transportation_consumer.startProcessing()
  inventory_consumer.startProcessing()
  order_consumer.startProcessing()
  app.run(debug=True,host='0.0.0.0')