from flask import Flask, redirect, abort, Response, Blueprint
from flasgger import Swagger
import os, time, json
from datetime import datetime
import logging


log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

# Import the Blueprints
from server.api.orderResource import orders_blueprint
from server.api.optimize import optimize_blueprint
from server.api.inventoryResource import data_inventory_blueprint
from server.api.reeferResource import data_reefer_blueprint
from server.api.transportationResource import data_transportation_blueprint
from server.api.health import health_bp
from server.api.prometheus import metrics_bp

from server.infrastructure.ReeferConsumer import ReeferConsumer
from server.infrastructure.InventoryConsumer import InventoryConsumer
from server.infrastructure.TransportationConsumer import TransportationConsumer


# Flask configuration
app = Flask(__name__)


app.register_blueprint(orders_blueprint)
app.register_blueprint(optimize_blueprint)
app.register_blueprint(data_inventory_blueprint)
app.register_blueprint(data_reefer_blueprint)
app.register_blueprint(data_transportation_blueprint)
app.register_blueprint(health_bp)
app.register_blueprint(metrics_bp)


# Create the consumer instances
reefer_consumer = ReeferConsumer()
transportation_consumer = TransportationConsumer()
inventory_consumer = InventoryConsumer()



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

# It is considered bad form to return an error for '/', so let's redirect to the apidocs
@app.route('/')
def index():
    return redirect('/apidocs')

if __name__ == "__main__":
    app.run(debug=True,host='0.0.0.0')