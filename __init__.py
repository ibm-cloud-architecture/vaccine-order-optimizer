from flask import Flask, redirect, abort
from flasgger import Swagger
import pandas as pd

import os, time, json
from datetime import datetime

# Application specifics
from server import app
from userapp.server.infrastructure.ReeferConsumer import ReeferConsumer
from userapp.server.infrastructure.InventoryConsumer import InventoryConsumer
from userapp.server.infrastructure.TransportationConsumer import TransportationConsumer

# Create the consumer instances
reefer_consumer = ReeferConsumer()
transportation_consumer = TransportationConsumer()
inventory_consumer = InventoryConsumer()

from userapp.server.api.controller import control_blueprint

# Print JSON responses with indentation for our flask application
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

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

app.register_blueprint(control_blueprint)
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