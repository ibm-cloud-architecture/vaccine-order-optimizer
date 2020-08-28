from flask import Flask, redirect, abort, Response
from flasgger import Swagger

import os, time, json
from datetime import datetime
import pandas as pd

# Application specifics
from server import app
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

from userapp.server.api.new_order import new_order_blueprint

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
swagger = Swagger(app, template=swagger_template)

# Start the consumers
reefer_consumer.startProcessing()
transportation_consumer.startProcessing()
inventory_consumer.startProcessing()

# It is considered bad form to return an error for '/', so let's redirect to the apidocs
@app.route('/')
def index():
    return redirect('/apidocs')

# Debugging endpoint to control the in-memory data structures
@app.route('/data')
def getData():
  print('[Init] - Calling /data endpoint')
  # Aggregate all data in a single dict
  aggregate_dict = {}
  aggregate_dict['REEFER'] = reefer_consumer.getEvents()
  aggregate_dict['INVENTORY'] = inventory_consumer.getEvents()
  aggregate_dict['TRANSPORTATION'] = transportation_consumer.getEvents()
  return aggregate_dict,202

# Debugging endpoint to control the in-memory data structures
@app.route('/data/reefer')
def getReefer():
  print('[Init] - Called /data/reefer endpoint')
  return reefer_consumer.getEvents(),202

# Debugging endpoint to control the in-memory data structures
@app.route('/data/inventory')
def getInventory():
  print('[Init] - Called /data/inventory endpoint')
  return inventory_consumer.getEvents(),202

# Debugging endpoint to control the in-memory data structures
@app.route('/data/transportation')
def getTransportation():
  print('[Init] - Called /data/transportation endpoint')
  return transportation_consumer.getEvents(),202

# Debugging endpoint to control the in-memory data structures
@app.route('/data/orders')
def getOrders():
  print('[Init] - Called /data/orders endpoint')
  return orders.getOrders(),202

# Debugging endpoint to control the in-memory data structures
@app.route('/data/reefer/pandas')
def getReeferPanda():
  print('[Init] - Called /data/reefer/pandas endpoint')
  return Response(reefer_consumer.getEventsPanda().to_string(), 202, {'Content-Type': 'text/plaintext'})

# Debugging endpoint to control the in-memory data structures
@app.route('/data/inventory/pandas')
def getInventoryPanda():
  print('[Init] - Called /data/inventory/pandas endpoint')
  return Response(inventory_consumer.getEventsPanda().to_string(), 202, {'Content-Type': 'text/plaintext'})

# Debugging endpoint to control the in-memory data structures
@app.route('/data/transportation/pandas')
def getTransportationPanda():
  print('[Init] - Called /data/transportation/pandas endpoint')
  return Response(transportation_consumer.getEventsPanda().to_string(), 202, {'Content-Type': 'text/plaintext'})

# Debugging endpoint to control the in-memory data structures
@app.route('/data/orders/pandas')
def getOrdersPanda():
  print('[Init] - Called /data/orders/pandas endpoint')
  return Response(orders.getOrdersPanda().to_string(), 202, {'Content-Type': 'text/plaintext'})

# Endpoint to produce testing data
@app.route('/produce')
def produceData():
  print('[Init] - Called /produce endpoint')
  data_producer.produceData()
  return "Done", 202