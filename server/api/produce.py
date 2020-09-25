
from flask import Blueprint
from flasgger import swag_from
from flask_restful import Resource, Api
from server.routes.prometheus import track_requests
from userapp import data_producer
"""
 created a new instance of the Blueprint class and bound the DataProducer resource to it.
"""

produce_blueprint = Blueprint("produce", __name__)
api = Api(produce_blueprint)

# The python-flask stack includes the prometheus metrics engine. You can ensure your endpoints
# are included in these metrics by enclosing them in the @track_requests wrapper.

class DataProducer(Resource):  

    # Returns the aggregations of all data
    @track_requests
    @swag_from('produce.yml')
    def post(self):
        print('[DataProducerResource] - calling /api/v1/produce endpoint')
        data_producer.produceData()
        return "Done", 202

api.add_resource(DataProducer, "/api/v1/produce")