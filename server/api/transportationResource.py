
from flask import Blueprint, Response
from flasgger import swag_from
from flask_restful import Resource, Api
from server.api.prometheus import track_requests
from server.infrastructure.DataStore import DataStore

"""
 created a new instance of the Blueprint class and bound the DataTransportation and DataTransportationPandas resources to it.
"""

data_transportation_blueprint = Blueprint("data_transportation", __name__)
api = Api(data_transportation_blueprint)

# The python-flask stack includes the prometheus metrics engine. You can ensure your endpoints
# are included in these metrics by enclosing them in the @track_requests wrapper.

class TransportationResource(Resource):  

    # Returns the Transportation data in JSON format
    @track_requests
    @swag_from('transportationAPI.yml')
    def get(self):
        print('[TransportationResource] - calling /api/v1/data/transportation endpoint')
        ds = DataStore.getInstance()
        return ds.getAllTransportations(),202

api.add_resource(TransportationResource, "/api/v1/data/transportation")
