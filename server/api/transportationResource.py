
from flask import Blueprint, Response
from flasgger import swag_from
from flask_restful import Resource, Api
from server.api.prometheus import track_requests
from server.infrastructure.TransportationDataStore import TransportationDataStore
import logging

"""
 created a new instance of the Blueprint class and bound the DataTransportation and DataTransportationPandas resources to it.
"""

data_transportation_blueprint = Blueprint("data_transportation", __name__)
api = Api(data_transportation_blueprint)

# The python-flask stack includes the prometheus metrics engine. You can ensure your endpoints
# are included in these metrics by enclosing them in the @track_requests wrapper.

class TransportationResource(Resource):  

    def __init__(self):
        self.store = TransportationDataStore.getInstance()

    # Returns the Transportation data in JSON format
    @track_requests
    @swag_from('transportationAPI.yml')
    def get(self):
        logging.debug('[TransportationResource] - calling /api/v1/data/transportations endpoint')
        list = self.store.getAllTransportations()
        print(list)
        return list,202

api.add_resource(TransportationResource, "/api/v1/data/transportations")
