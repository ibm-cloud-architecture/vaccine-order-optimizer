
from flask import Blueprint, Response
from flasgger import swag_from
from flask_restful import Resource, Api
from server.routes.prometheus import track_requests
from userapp import transportation_consumer
"""
 created a new instance of the Blueprint class and bound the DataTransportation and DataTransportationPandas resources to it.
"""

data_transportation_blueprint = Blueprint("data_transportation", __name__)
api = Api(data_transportation_blueprint)

# The python-flask stack includes the prometheus metrics engine. You can ensure your endpoints
# are included in these metrics by enclosing them in the @track_requests wrapper.

class DataTransportation(Resource):  

    # Returns the Transportation data in JSON format
    @track_requests
    @swag_from('data_transportation.yml')
    def get(self):
        print('[DataTransportationResource] - calling /api/v1/data/transportation endpoint')
        return transportation_consumer.getEvents(),202

class DataTransportationPandas(Resource):  

    # Returns the Transportation data in pandas format
    @track_requests
    @swag_from('data_transportation_pandas.yml')
    def get(self):
        print('[DataTransportationPandasResource] - calling /api/v1/data/transportation/pandas endpoint')
        return Response(transportation_consumer.getEventsPanda().to_string(), 202, {'Content-Type': 'text/plaintext'})

api.add_resource(DataTransportation, "/api/v1/data/transportation")
api.add_resource(DataTransportationPandas, "/api/v1/data/transportation/pandas")