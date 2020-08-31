from flask import Flask, redirect, abort, Response, Blueprint
from flasgger import Swagger

import os, time, json
from datetime import datetime
import pandas as pd

# Application specifics
from userapp.server.infrastructure.ReeferConsumer import ReeferConsumer
from userapp.server.infrastructure.InventoryConsumer import InventoryConsumer
from userapp.server.infrastructure.TransportationConsumer import TransportationConsumer
from userapp.server.infrastructure.Orders import Orders
from userapp.server.infrastructure.DataProducer import DataProducer

# Create the consumer instances
reefer_consumer = ReeferConsumer()
transportation_consumer = TransportationConsumer()
inventory_consumer = InventoryConsumer()

# Create the orders object
orders = Orders()
# Create the data producer
data_producer = DataProducer()

# Flask configuration
app = Flask(__name__)

# Import the Blueprints
from userapp.server.api.new_order import new_order_blueprint
from userapp.server.api.data import data_blueprint
from userapp.server.api.data_inventory import data_inventory_blueprint
from userapp.server.api.data_orders import data_orders_blueprint
from userapp.server.api.data_reefer import data_reefer_blueprint
from userapp.server.api.data_transportation import data_transportation_blueprint
from userapp.server.api.produce import produce_blueprint

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
    "version": "0.1"
  },
  "schemes": [
    "http"
  ],
}

app.register_blueprint(new_order_blueprint)
app.register_blueprint(data_blueprint)
app.register_blueprint(data_inventory_blueprint)
app.register_blueprint(data_orders_blueprint)
app.register_blueprint(data_reefer_blueprint)
app.register_blueprint(data_transportation_blueprint)
app.register_blueprint(produce_blueprint)
swagger = Swagger(app, template=swagger_template)

# Start the consumers
reefer_consumer.startProcessing()
transportation_consumer.startProcessing()
inventory_consumer.startProcessing()

# It is considered bad form to return an error for '/', so let's redirect to the apidocs
@app.route('/')
def index():
    return redirect('/apidocs')